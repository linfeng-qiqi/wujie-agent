#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SQLite 存储层，零依赖。WAL 模式：读写互不阻塞（支撑运行中的实时查询）。
每线程一个连接，避免多线程共享连接的锁竞争。"""
import os
import sqlite3
import threading
import time
from datetime import datetime

import paths

BASE_DIR = paths.ROOT
DB_PATH = os.path.join(BASE_DIR, "agent.db")
_local = threading.local()
_write_lock = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state TEXT NOT NULL DEFAULT 'queued',
    raw_text TEXT,
    notes TEXT DEFAULT '',
    progress TEXT DEFAULT '',
    cancel_flag INTEGER DEFAULT 0,
    trigger TEXT DEFAULT '',
    created_at TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER,
    direction TEXT,
    content TEXT,
    seg_types TEXT DEFAULT '',
    sender_qq TEXT,
    ts TEXT
);

CREATE TABLE IF NOT EXISTS attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER,
    path TEXT,
    kind TEXT,
    orig_name TEXT,
    hash TEXT,
    size INTEGER,
    ready INTEGER DEFAULT 0,
    keep TEXT DEFAULT '',
    expire_at TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER,
    event_type TEXT,
    payload TEXT,
    ts TEXT
);

CREATE TABLE IF NOT EXISTS agents_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER,
    seq INTEGER,
    name TEXT,
    role TEXT,
    state TEXT DEFAULT 'queued',
    current_action TEXT DEFAULT '',
    state_ts TEXT,
    started_at TEXT,
    ended_at TEXT,
    elapsed REAL DEFAULT 0,
    tokens_in INTEGER DEFAULT 0,
    tokens_out INTEGER DEFAULT 0,
    result TEXT,
    error TEXT,
    heal_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS agent_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id INTEGER,
    seq INTEGER,
    ts TEXT,
    action_type TEXT,
    detail TEXT,
    result_summary TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS agent_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id INTEGER,
    seq INTEGER,
    ts TEXT,
    kind TEXT,
    content TEXT
);

CREATE TABLE IF NOT EXISTS memory_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity TEXT,
    relation TEXT,
    value TEXT,
    confidence REAL DEFAULT 1.0,
    version INTEGER DEFAULT 1,
    effective_at TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS memory_episodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER,
    content TEXT,
    summary TEXT,
    importance REAL DEFAULT 0.5,
    heat INTEGER DEFAULT 1,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS memory_skills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    content TEXT,
    success_count INTEGER DEFAULT 0,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attachment_id INTEGER,
    file_name TEXT,
    path TEXT,
    keep TEXT DEFAULT '7d',
    expire_at TEXT,
    summary TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS memory_hits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT,
    hit_count INTEGER,
    hits TEXT,
    ts TEXT
);

CREATE TABLE IF NOT EXISTS api_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT,
    task_id INTEGER,
    agent_id INTEGER,
    scene TEXT,
    model TEXT,
    thinking TEXT,
    prompt_tokens INTEGER DEFAULT 0,
    completion_tokens INTEGER DEFAULT 0,
    reasoning_tokens INTEGER DEFAULT 0,
    cached_tokens INTEGER DEFAULT 0,
    server_tools INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS search_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cache_key TEXT UNIQUE,
    question TEXT,
    answer TEXT,
    intent TEXT,
    source TEXT,
    created_at TEXT,
    expire_at TEXT,
    hits INTEGER DEFAULT 0,
    last_hit_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_actions_agent ON agent_actions(agent_id, seq);
CREATE INDEX IF NOT EXISTS idx_raw_agent ON agent_raw(agent_id, seq);
CREATE INDEX IF NOT EXISTS idx_runs_task ON agents_runs(task_id, seq);
CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state);
CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id, id);
CREATE INDEX IF NOT EXISTS idx_usage_ts ON api_usage(ts);
CREATE INDEX IF NOT EXISTS idx_usage_task ON api_usage(task_id);
"""


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _conn():
    c = getattr(_local, "conn", None)
    if c is None:
        os.makedirs(BASE_DIR, exist_ok=True)
        c = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA busy_timeout=8000")
        _local.conn = c
    return c


def init():
    with _write_lock:
        c = _conn()
        c.executescript(SCHEMA)
        c.commit()
        # FTS5 可选，失败不影响主流程
        try:
            c.execute("CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(payload)")
            c.commit()
        except Exception:
            pass
    return True


def execute(sql, params=()):
    with _write_lock:
        c = _conn()
        cur = c.execute(sql, params)
        c.commit()
        return cur.lastrowid


def query(sql, params=()):
    c = _conn()
    cur = c.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def query_one(sql, params=()):
    c = _conn()
    cur = c.execute(sql, params)
    r = cur.fetchone()
    return dict(r) if r else None


# ---------- 便捷方法 ----------

def add_task(raw_text, trigger=""):
    t = now()
    return execute(
        "INSERT INTO tasks(state,raw_text,notes,progress,cancel_flag,trigger,created_at,updated_at) "
        "VALUES('queued',?, '', '', 0, ?, ?, ?)",
        (raw_text, trigger, t, t))


def set_task_state(task_id, state, progress=None):
    if progress is None:
        execute("UPDATE tasks SET state=?, updated_at=? WHERE id=?", (state, now(), task_id))
    else:
        execute("UPDATE tasks SET state=?, progress=?, updated_at=? WHERE id=?",
                (state, progress, now(), task_id))


def append_note(task_id, note):
    row = query_one("SELECT notes FROM tasks WHERE id=?", (task_id,))
    cur = (row or {}).get("notes") or ""
    execute("UPDATE tasks SET notes=?, updated_at=? WHERE id=?",
            (cur + "\n" + note if cur else note, now(), task_id))


def cancel_task(task_id):
    execute("UPDATE tasks SET cancel_flag=1, updated_at=? WHERE id=?", (now(), task_id))


def is_cancelled(task_id):
    r = query_one("SELECT cancel_flag FROM tasks WHERE id=?", (task_id,))
    return bool(r and r.get("cancel_flag"))


def add_event(task_id, event_type, payload=""):
    eid = execute(
        "INSERT INTO events(task_id,event_type,payload,ts) VALUES(?,?,?,?)",
        (task_id, event_type, payload, now()))
    try:
        with _write_lock:
            c = _conn()
            c.execute("INSERT INTO events_fts(rowid,payload) VALUES(?,?)", (eid, payload))
            c.commit()
    except Exception:
        pass
    return eid


def add_message(task_id, direction, content, seg_types="", sender_qq=""):
    return execute(
        "INSERT INTO messages(task_id,direction,content,seg_types,sender_qq,ts) VALUES(?,?,?,?,?,?)",
        (task_id, direction, content, seg_types, sender_qq, now()))


def add_attachment(task_id, path, kind, orig_name, hash_val, size, ready):
    return execute(
        "INSERT INTO attachments(task_id,path,kind,orig_name,hash,size,ready,created_at) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (task_id, path, kind, orig_name, hash_val, size, 1 if ready else 0, now()))


def mark_attachment_ready(att_id, hash_val, size):
    execute("UPDATE attachments SET ready=1, hash=?, size=? WHERE id=?",
            (hash_val, size, att_id))


# ---------- Agent 实时状态 ----------

def add_agent_run(task_id, seq, name, role):
    t = now()
    return execute(
        "INSERT INTO agents_runs(task_id,seq,name,role,state,current_action,state_ts,started_at) "
        "VALUES(?,?,?,?, 'queued','',?,?)",
        (task_id, seq, name, role, t, t))


def set_agent_state(agent_id, state, current_action=""):
    execute("UPDATE agents_runs SET state=?, current_action=?, state_ts=? WHERE id=?",
            (state, current_action, now(), agent_id))


def add_agent_action(agent_id, seq, action_type, detail, result_summary=""):
    return execute(
        "INSERT INTO agent_actions(agent_id,seq,ts,action_type,detail,result_summary) "
        "VALUES(?,?,?,?,?,?)",
        (agent_id, seq, now(), action_type, detail, result_summary))


def finish_agent(agent_id, state, result, error="", tokens_in=0, tokens_out=0, heal_count=0):
    ended = now()
    row = query_one("SELECT started_at FROM agents_runs WHERE id=?", (agent_id,))
    elapsed = 0.0
    if row and row.get("started_at"):
        try:
            fmt = "%Y-%m-%d %H:%M:%S"
            elapsed = (datetime.strptime(ended, fmt) - datetime.strptime(row["started_at"], fmt)).total_seconds()
        except Exception:
            pass
    execute(
        "UPDATE agents_runs SET state=?, current_action='', state_ts=?, ended_at=?, elapsed=?, "
        "result=?, error=?, tokens_in=?, tokens_out=?, heal_count=? WHERE id=?",
        (state, ended, ended, elapsed, result, error, tokens_in, tokens_out, heal_count, agent_id))


def agents_for_task(task_id):
    return query("SELECT * FROM agents_runs WHERE task_id=? ORDER BY seq", (task_id,))


def actions_for_agent(agent_id):
    return query("SELECT * FROM agent_actions WHERE agent_id=? ORDER BY seq", (agent_id,))


def add_agent_raw(agent_id, seq, kind, content):
    return execute(
        "INSERT INTO agent_raw(agent_id,seq,ts,kind,content) VALUES(?,?,?,?,?)",
        (agent_id, seq, now(), str(kind or "")[:40], str(content or "")))


def raw_for_agent(agent_id):
    return query("SELECT * FROM agent_raw WHERE agent_id=? ORDER BY seq", (agent_id,))


def task_by_id(task_id):
    return query_one("SELECT * FROM tasks WHERE id=?", (task_id,))


def events_for_task(task_id):
    return query("SELECT * FROM events WHERE task_id=? ORDER BY id", (task_id,))


def attachments_for_task(task_id):
    return query("SELECT * FROM attachments WHERE task_id=? ORDER BY id", (task_id,))


def latest_task():
    return query_one("SELECT * FROM tasks ORDER BY id DESC LIMIT 1")


def tasks_by_state(states):
    if not states:
        return []
    ph = ",".join("?" for _ in states)
    return query(f"SELECT * FROM tasks WHERE state IN ({ph}) ORDER BY id", tuple(states))


# ---------- 记忆 ----------

def add_fact(entity, relation, value, confidence=1.0):
    t = now()
    return execute(
        "INSERT INTO memory_facts(entity,relation,value,confidence,version,effective_at,created_at) "
        "VALUES(?,?,?,?,1,?,?)",
        (entity, relation, value, confidence, t, t))


def search_facts(keyword):
    kw = f"%{keyword}%"
    return query(
        "SELECT * FROM memory_facts WHERE entity LIKE ? OR relation LIKE ? OR value LIKE ? "
        "ORDER BY version DESC, effective_at DESC LIMIT 20", (kw, kw, kw))


def add_episode(task_id, content, summary, importance=0.5):
    return execute(
        "INSERT INTO memory_episodes(task_id,content,summary,importance,heat,created_at) "
        "VALUES(?,?,?,?,1,?)", (task_id, content, summary, importance, now()))


def search_episodes(keyword):
    return query(
        "SELECT * FROM memory_episodes WHERE content LIKE ? OR summary LIKE ? "
        "ORDER BY heat DESC, id DESC LIMIT 20", (f"%{keyword}%", f"%{keyword}%"))


def add_skill(name, content):
    return execute(
        "INSERT INTO memory_skills(name,content,success_count,created_at) VALUES(?,?,1,?)",
        (name, content, now()))


def search_skills(keyword):
    return query(
        "SELECT * FROM memory_skills WHERE name LIKE ? OR content LIKE ? ORDER BY success_count DESC LIMIT 10",
        (f"%{keyword}%", f"%{keyword}%"))


def add_memory_hit(query_text, count, hits):
    import json
    return execute(
        "INSERT INTO memory_hits(query,hit_count,hits,ts) VALUES(?,?,?,?)",
        (query_text, count, json.dumps(hits, ensure_ascii=False), now()))


def memory_hit_stats(limit=20):
    rows = query("SELECT * FROM memory_hits ORDER BY id DESC LIMIT ?", (limit,))
    if not rows:
        return {"total": 0, "hit": 0, "avg": 0, "recent": None}
    hit = sum(1 for r in rows if (r.get("hit_count") or 0) > 0)
    return {
        "total": len(rows),
        "hit": hit,
        "avg": round(sum((r.get("hit_count") or 0) for r in rows) / len(rows), 1),
        "recent": rows[0],
    }


# ---------- 文档 ----------

def add_document(att_id, file_name, path, keep, expire_at, summary=""):
    return execute(
        "INSERT INTO documents(attachment_id,file_name,path,keep,expire_at,summary,created_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (att_id, file_name, path, keep, expire_at, summary, now()))


def set_document_keep(doc_id, keep, expire_at):
    execute("UPDATE documents SET keep=?, expire_at=? WHERE id=?", (keep, expire_at, doc_id))


def expired_documents():
    return query("SELECT * FROM documents WHERE keep='7d' AND expire_at < ?", (now(),))


def delete_document(doc_id):
    execute("DELETE FROM documents WHERE id=?", (doc_id,))


# ---------- API 用量全量落库 ----------

def add_usage(scene, model, prompt_tokens=0, completion_tokens=0,
              reasoning_tokens=0, cached_tokens=0, server_tools=0,
              task_id=None, agent_id=None, ts=None):
    """每次模型调用都记一条，含 thinking token。所有调用统一入口。"""
    return execute(
        "INSERT INTO api_usage(ts,task_id,agent_id,scene,model,thinking,"
        "prompt_tokens,completion_tokens,reasoning_tokens,cached_tokens,server_tools) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (ts or now(), task_id, agent_id, str(scene or "")[:40], str(model or "")[:60],
         "", int(prompt_tokens or 0), int(completion_tokens or 0),
         int(reasoning_tokens or 0), int(cached_tokens or 0), int(server_tools or 0)))


def cache_get(cache_key):
    """取未过期的缓存；过期返回 None（不删除，保留追溯）。"""
    return query_one(
        "SELECT * FROM search_cache WHERE cache_key=? AND expire_at>?",
        (cache_key, now()))


def cache_put(cache_key, question, answer, intent, source, ttl_seconds):
    """写入/覆盖缓存，带过期时间。"""
    from datetime import datetime, timedelta
    expire = (datetime.now() + timedelta(seconds=ttl_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    existing = query_one("SELECT id FROM search_cache WHERE cache_key=?", (cache_key,))
    if existing:
        execute("UPDATE search_cache SET question=?,answer=?,intent=?,source=?,created_at=?,expire_at=?,hits=0,last_hit_at=NULL WHERE cache_key=?",
                (question, answer, intent, source, now(), expire, cache_key))
    else:
        execute("INSERT INTO search_cache(cache_key,question,answer,intent,source,created_at,expire_at,hits) VALUES(?,?,?,?,?,?,?,0)",
                (cache_key, question, answer, intent, source, now(), expire))


def cache_hit(cache_key):
    """缓存命中，命中数+1。"""
    execute("UPDATE search_cache SET hits=hits+1,last_hit_at=? WHERE cache_key=?", (now(), cache_key))


def cache_stats():
    total = query_one("SELECT COUNT(*) c,SUM(hits) h FROM search_cache")
    expired = query_one("SELECT COUNT(*) c FROM search_cache WHERE expire_at<=?", (now(),))
    return {"total": total["c"] or 0, "hits": total["h"] or 0, "expired": expired["c"] or 0}


def usage_summary():
    """全量统计：按场景/模型分组 + 今日合计。用于「查看用量」。"""
    total = query_one(
        "SELECT COUNT(*) n, SUM(prompt_tokens) pin, SUM(completion_tokens) pout, "
        "SUM(reasoning_tokens) reason, SUM(cached_tokens) cached, SUM(server_tools) tools "
        "FROM api_usage") or {}
    today = query_one(
        "SELECT COUNT(*) n, SUM(prompt_tokens) pin, SUM(completion_tokens) pout, "
        "SUM(reasoning_tokens) reason, SUM(cached_tokens) cached FROM api_usage "
        "WHERE ts LIKE ?", (datetime.now().strftime("%Y-%m-%d") + "%",)) or {}
    by_scene = query(
        "SELECT scene, COUNT(*) n, SUM(prompt_tokens) pin, SUM(completion_tokens) pout, "
        "SUM(reasoning_tokens) reason FROM api_usage GROUP BY scene ORDER BY n DESC")
    by_model = query(
        "SELECT model, COUNT(*) n, SUM(prompt_tokens) pin, SUM(completion_tokens) pout, "
        "SUM(reasoning_tokens) reason FROM api_usage GROUP BY model ORDER BY n DESC")
    return {"total": total, "today": today, "by_scene": by_scene, "by_model": by_model}


def task_stats():
    """清空前预览：任务及其关联数据的条数。"""
    tables = ["tasks", "agents_runs", "agent_actions", "agent_raw", "events",
              "messages", "attachments", "documents", "api_usage",
              "memory_episodes", "memory_hits"]
    out = {}
    for tb in tables:
        try:
            out[tb] = query_one(f"SELECT COUNT(*) c FROM {tb}")["c"] or 0
        except Exception:
            out[tb] = 0
    return out


def clear_all_tasks():
    """清空所有任务及关联数据；保留 config / memory_facts / memory_skills / search_cache。
    返回各表删除条数。自增 id 一并归零，追溯编号从头开始。"""
    tables = ["agent_raw", "agent_actions", "agents_runs", "events", "messages",
              "attachments", "documents", "api_usage", "memory_episodes",
              "memory_hits", "tasks"]
    counts = {}
    with _write_lock:
        c = _conn()
        for tb in tables:
            try:
                counts[tb] = c.execute(f"SELECT COUNT(*) FROM {tb}").fetchone()[0]
            except Exception:
                counts[tb] = 0
            c.execute(f"DELETE FROM {tb}")
        # 自增序列归零
        try:
            ph = ",".join("?" for _ in tables)
            c.execute(f"DELETE FROM sqlite_sequence WHERE name IN ({ph})", tuple(tables))
        except Exception:
            pass
        # 全文索引同步清空
        try:
            c.execute("DELETE FROM events_fts")
        except Exception:
            pass
        c.commit()
    return counts


def config_get(key, default=None):
    r = query_one("SELECT value FROM config WHERE key=?", (key,))
    return r["value"] if r else default


def config_set(key, value):
    execute("INSERT OR REPLACE INTO config(key,value) VALUES(?,?)", (str(key), str(value)))
