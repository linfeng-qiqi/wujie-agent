#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""五层记忆：工作记忆(交给调用方) + 情节/语义/程序记忆(SQLite) + 摘要。
检索走三路召回 + 实体锚定一跳扩展，结果落 memory_hits 供菜单统计。"""
import re
import time
import json
from datetime import datetime, timedelta

import db
import ai

STOP = set("的 了 和 是 在 我 你 他 这 那 吗 呢 啊 吧 把 被 给 从 到 有 就 都 也 不 很 什么 怎么 一下 请 帮我 看看 查询 一下".split())


def _keywords(text):
    """简单中文分词：2-4 字片段 + 英文数字词。"""
    words = set(re.findall(r"[a-zA-Z0-9_]{2,}", text or ""))
    cn = re.findall(r"[\u4e00-\u9fff]{2,4}", text or "")
    for w in cn:
        if w not in STOP:
            words.add(w)
    for i in range(len(cn or "")):
        pass
    return [w for w in words if w and w not in STOP][:12]


def recall(query, limit=8, record=True):
    """检索记忆：关键词召回 facts / episodes / skills，实体一跳扩展。
    返回 (上下文文本, 命中列表)。"""
    kws = _keywords(query)
    if not kws:
        if record:
            db.add_memory_hit(query, 0, [])
        return "", []

    hits = []
    seen = set()

    for kw in kws:
        for f in db.search_facts(kw):
            key = ("fact", f["id"])
            if key in seen:
                continue
            seen.add(key)
            hits.append({
                "kind": "fact",
                "text": f"{f['entity']} → {f['relation']} → {f['value']}",
                "score": (f.get("confidence") or 1.0),
                "ref": key,
            })
        for e in db.search_episodes(kw):
            key = ("episode", e["id"])
            if key in seen:
                continue
            seen.add(key)
            hits.append({
                "kind": "episode",
                "text": (e.get("summary") or e.get("content") or "")[:300],
                "score": 0.7 + (e.get("importance") or 0) * 0.3,
                "ref": key,
            })
        for s in db.search_skills(kw):
            key = ("skill", s["id"])
            if key in seen:
                continue
            seen.add(key)
            hits.append({
                "kind": "skill",
                "text": f"{s['name']}: {(s.get('content') or '')[:250]}",
                "score": 0.6 + min((s.get("success_count") or 0) * 0.05, 0.4),
                "ref": key,
            })

    # 连锁：从命中的 fact 实体出发，一跳扩展关联事实
    entities = set()
    for h in hits:
        if h["kind"] == "fact":
            txt = h["text"]
            if " → " in txt:
                entities.add(txt.split(" → ")[0])
    for ent in list(entities)[:4]:
        for f in db.search_facts(ent):
            key = ("fact", f["id"])
            if key in seen:
                continue
            seen.add(key)
            hits.append({
                "kind": "fact(连锁)",
                "text": f"{f['entity']} → {f['relation']} → {f['value']}",
                "score": 0.5,
                "ref": key,
            })

    hits.sort(key=lambda x: -x["score"])
    hits = hits[:limit]
    if record:
        db.add_memory_hit(query, len(hits), [h["text"][:100] for h in hits])

    if not hits:
        return "", []
    lines = [f"- [{h['kind']}] {h['text']}" for h in hits]
    return "相关记忆：\n" + "\n".join(lines), hits


EXTRACT_SYSTEM = """你是记忆抽取器。从对话中抽取值得长期记住的事实，输出严格 JSON。
只抽取：用户身份/偏好、地理位置、常用项目、明确的计划或约定、成功的经验方法。
不抽取：一次性的天气查询结果、临时价格、无意义的闲聊。
输出格式：{"facts":[{"entity":"实体","relation":"关系","value":"值"}], "skills":[{"name":"经验名","content":"方法"}]}
没有可抽取内容时输出：{"facts":[],"skills":[]}"""


# 入库规则抽取：只认稳定事实句式，纯本地零 token。
# 时效词（天气/票价/今天/新闻等）一律不抽，避免把旧数据写进长期事实。
_FACT_PATTERNS = [
    (r"我叫(.{1,12})", "用户", "名字"),
    (r"我的名字是(.{1,12})", "用户", "名字"),
    (r"我是(.{1,16})(?:，|。|$)", "用户", "身份"),
    (r"我喜欢(.{1,20})", "用户", "喜好"),
    (r"我(?:讨厌|不喜欢)(.{1,20})", "用户", "厌恶"),
    (r"我在(.{2,12})(?:工作|生活|上学|住)", "用户", "所在地"),
    (r"我家在(.{2,12})", "用户", "家乡"),
    (r"我的(.{1,8})是(.{1,24})", "用户", None),  # group1 作关系
    (r"记住[:：]?\s*(?:我的)?(.{1,8})是(.{1,24})", "用户", None),
    (r"记住[:：]?\s*(?:我的)?(.{1,8})叫(.{1,24})", "用户", None),
]
_TIME_SENSITIVE = ("天气", "气温", "温度", "票价", "价格", "多少钱", "今天", "明天",
                   "昨天", "现在", "最新", "新闻", "股价", "航班", "高铁", "火车", "天气如何")


def _rule_extract(user_text):
    """本地规则抽稳定事实，返回 [(entity, relation, value)]，零 token。"""
    t = str(user_text or "").strip()
    if not t or any(w in t for w in _TIME_SENSITIVE):
        return []
    out = []
    for pattern, ent, rel in _FACT_PATTERNS:
        for m in re.finditer(pattern, t):
            groups = [g.strip(" ，。！？、") for g in m.groups() if g]
            if rel and groups:
                val = groups[0]
                r = rel
            elif len(groups) >= 2:
                r, val = groups[0], groups[1]
            else:
                continue
            if not val or len(val) > 30 or val in ("什么", "哪", "谁"):
                continue
            out.append((ent, r[:12], val[:30]))
            if len(out) >= 6:
                return out
    # 去重
    seen, uniq = set(), []
    for item in out:
        key = tuple(item)
        if key not in seen:
            seen.add(key)
            uniq.append(item)
    return uniq[:6]


def extract_and_store(task_id, user_text, assistant_text):
    """任务后写回：规则抽稳定事实（零 token）+ 情节原文。返回写入事实条数。"""
    try:
        n = 0
        for ent, rel, val in _rule_extract(user_text):
            db.add_fact(ent, rel, val, confidence=0.9)
            n += 1
        # 情节记忆：保留原文 + 摘要（模型不可用也不影响）
        summary = user_text[:150] + ("｜" + assistant_text[:150] if assistant_text else "")
        db.add_episode(task_id, user_text[:2000], summary, importance=0.5)
        return n
    except Exception:
        try:
            db.add_episode(task_id, user_text[:2000], user_text[:150], importance=0.4)
        except Exception:
            pass
        return 0


def config_fast():
    import config
    return config.get("model_fast")


def stats_text():
    s = db.memory_hit_stats(20)
    lines = [f"▎记忆命中统计（最近 {s['total']} 次）"]
    if not s["total"]:
        lines.append("暂无检索记录")
        return "\n".join(lines)
    rate = round(s["hit"] / s["total"] * 100) if s["total"] else 0
    lines.append(f"平均命中：{s['avg']} 条    命中率：{rate}%")
    r = s.get("recent")
    if r:
        hits = []
        try:
            hits = json.loads(r.get("hits") or "[]")
        except Exception:
            pass
        lines.append(f"最近一次「{r.get('query','')[:30]}」命中 {r.get('hit_count',0)} 条：")
        for h in hits[:6]:
            lines.append(f"· {h}")
    n_fact = len(db.query("SELECT id FROM memory_facts"))
    n_ep = len(db.query("SELECT id FROM memory_episodes"))
    n_sk = len(db.query("SELECT id FROM memory_skills"))
    lines.append(f"库存：语义 {n_fact} 条 / 情节 {n_ep} 条 / 经验 {n_sk} 条")
    return "\n".join(lines)


def forget_maintenance():
    """遗忘降级：长期未访问的情节降低热度，不删除。"""
    try:
        cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        db.execute("UPDATE memory_episodes SET heat=MAX(0,heat-1) WHERE created_at < ?", (cutoff,))
    except Exception:
        pass
