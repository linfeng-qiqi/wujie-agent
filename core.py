#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""核心编排：聚合 → 分流 → 队列 → 执行流水线 → 发送 → 反馈；命令与菜单。"""
import os
import re
import json
import time
import threading
import queue
from datetime import datetime, timedelta

import config
import db
import bus
import ai
import agents
import memory
import trace
import cache

TARGET = None
_pending = {}          # task_id -> PendingBundle
_pending_lock = threading.RLock()
_work_queue = queue.Queue()
_current_task_id = None
_running_lock = threading.RLock()
_shutdown = threading.Event()
_menu_state = {"open": False, "task_id": None, "expires": 0}
_confirm_state = {"open": False, "task_id": None, "doc_id": None, "expires": 0,
                  "attachments": [], "result": ""}
_clear_state = {"open": False, "expires": 0}   # 清空所有任务的二次确认
_user_history = []      # 最近对话，供多轮上下文
_history_lock = threading.RLock()


# ================= 聚合层 =================

class PendingBundle:
    def __init__(self, task_id):
        self.task_id = task_id
        self.texts = []
        self.attachments = []
        self.last_text_ts = 0
        self.last_attach_ready_ts = 0
        self.pending_downloads = 0
        self.created = time.time()
        self.timer = None

    def add_text(self, text):
        if text:
            self.texts.append(text)
        self.last_text_ts = time.time()

    def add_attachment_info(self, info):
        self.attachments.append(info)
        self.pending_downloads += 1

    def on_attachment_ready(self, att):
        self.pending_downloads = max(0, self.pending_downloads - 1)
        self.last_attach_ready_ts = time.time()

    def all_text(self):
        return "\n".join(self.texts).strip()

    def has_attach(self):
        return len(self.attachments) > 0

    def remaining_wait(self):
        cfg_silent = config.get("silent_text", 2)
        cfg_attach = config.get("silent_attach", 8)
        cfg_cap = config.get("hard_cap", 30)
        if self.pending_downloads > 0:
            return None  # 附件未就绪，不启动静默计时
        if self.has_attach():
            base = self.last_attach_ready_ts or time.time()
            wait = cfg_attach
        else:
            base = self.last_text_ts
            wait = cfg_silent
        remaining = wait - (time.time() - base)
        cap_remaining = cfg_cap - (time.time() - self.created)
        return max(0, min(remaining, cap_remaining))


def _arm_timer(bundle, task_id):
    """按剩余时间重新安排封口检查。"""
    if bundle.timer:
        bundle.timer.cancel()
    remaining = bundle.remaining_wait()
    if remaining is None:
        return
    t = threading.Timer(max(0.2, remaining + 0.1), _on_seal_timer, args=(task_id,))
    t.daemon = True
    bundle.timer = t
    t.start()


def _on_seal_timer(task_id):
    bundle = None
    with _pending_lock:
        b = _pending.get(task_id)
        if not b:
            return
        remaining = b.remaining_wait()
        if remaining is None:
            return
        if remaining > 0.05:
            _arm_timer(b, task_id)
            return
        bundle = _seal_locked(task_id, "静默超时")
    # 封口后必须入队，否则任务永远停在 queued（之前漏了这一步）
    if bundle:
        _dispatch(bundle)


def _seal_locked(task_id, trigger):
    b = _pending.pop(task_id, None)
    if not b:
        return None
    if b.timer:
        try:
            b.timer.cancel()
        except Exception:
            pass
    text = b.all_text()
    if not text and not b.attachments:
        db.set_task_state(task_id, "failed")
        return None
    db.set_task_state(task_id, "queued", json.dumps(
        {"text": text, "attachments": len(b.attachments), "trigger": trigger}, ensure_ascii=False))
    db.add_event(task_id, "sealed", f"封口:{trigger} 文字={text[:80]} 附件={len(b.attachments)}")
    return {"task_id": task_id, "text": text, "attachments": b.attachments, "trigger": trigger}


def on_incoming(data):
    """收到一条 QQ 消息（已在 bus 中过滤目标 QQ）。"""
    global TARGET
    sender = data.get("sender", {}) or {}
    uid = str(sender.get("user_id", ""))
    TARGET = uid
    nickname = sender.get("nickname", "未知")
    msg_type = data.get("message_type")
    group_id = data.get("group_id")
    raw_segs = data.get("message", [])

    # 纯表情包 / 纯表情符号消息：完全忽略，不识别、不建任务、不入库
    if not bus.has_usable_content(raw_segs):
        bus.log(f"忽略纯表情消息: {nickname}({uid})")
        return

    text, attachments = bus.parse_segments(raw_segs)
    text = (text or "").strip()

    bus.log(f"收到 {nickname}({uid}) [{'私聊' if msg_type=='private' else '群'}]: {text[:150]}")
    db.add_message(None, "in", text,
                   ",".join(s.get("type", "") for s in raw_segs if isinstance(s, dict)), uid)

    # 1) 文档保留确认态优先
    if _confirm_state["open"] and text:
        if handle_doc_confirm(text):
            return

    # 2) 菜单选择态
    if _menu_state["open"] and time.time() < _menu_state["expires"] and text:
        if handle_menu_selection(text, uid, group_id):
            return
        _menu_state["open"] = False  # 非菜单回复 → 退出菜单态，继续分流

    # 3) 控制命令（随时可用）
    if text and handle_command(text, uid, group_id):
        return

    # 4) 结束词 → 立即封口
    if text and text in (config.get("end_words", []) or []):
        with _pending_lock:
            for tid in list(_pending.keys()):
                bundle = _seal_locked(tid, "结束词")
                if bundle:
                    _dispatch(bundle)
        return

    # 5) 聚合：已有 pending 就并入，否则新建
    with _pending_lock:
        tid = None
        for k, b in _pending.items():
            # 同一时刻只允许一个聚合窗口（单用户场景）
            tid = k
            break
        if tid is None:
            tid = db.add_task(text or "[附件]", trigger="message")
            bundle = PendingBundle(tid)
            _pending[tid] = bundle
            new_one = True
        else:
            bundle = _pending[tid]
            new_one = False

        # 把新文字并入任务原文（新建时 add_task 已存过，避免重复）
        if text and not new_one:
            row = db.query_one("SELECT raw_text FROM tasks WHERE id=?", (tid,))
            cur = (row or {}).get("raw_text") or ""
            db.execute("UPDATE tasks SET raw_text=?, updated_at=? WHERE id=?",
                       ((cur + "\n" + text).strip() if cur else text, db.now(), tid))
        bundle.add_text(text)

        # 处理附件
        for info in attachments:
            bundle.add_attachment_info(info)
            path, h, size = bus.download_attachment(info)
            if path:
                aid = db.add_attachment(tid, path, info.get("kind"), info.get("orig_name"),
                                        h, size, True)
                info["id"] = aid
                info["path"] = path
                db.add_message(tid, "in", f"[附件:{info.get('orig_name')}]", info.get("kind", ""), uid)
            else:
                bus.send_text(uid, f"附件《{info.get('orig_name')}》接收失败，请重发。", group_id)
            bundle.on_attachment_ready(info)

        if new_one and len(bundle.texts) >= 2:
            bus.send_text(uid, "还有吗？没有就发『好了』，我马上开始。", group_id)

        _arm_timer(bundle, tid)


# ================= 分流层 =================

SUPPLEMENT_WORDS = ["顺便", "你先看", "好像更便宜", "另外", "补充", "再加上", "而且", "对 ", "还有", "改一下", "换成"]

# 查看余额 / API 信息的所有叫法（统一小写比较，避免大小写踩坑）
API_INFO_ALIASES = {
    "查看余额", "查余额", "余额", "还有多少钱", "还剩多少钱", "剩多少钱", "多少钱余额",
    "查看api", "查看api信息", "api信息", "api", "我的api", "api详情", "查看api详情",
    "查看api密钥", "用的什么模型", "密钥", "apikey", "查看apikey",
    "api情况", "我的api信息", "看看余额", "余额多少",
}

TRACE_EXPORT_RE = re.compile(
    r"^(导出追溯|追溯文档|导出日志|导出报告|生成追溯|导出agent记录)\s*#?\s*(\d+)?$", re.I)

# 查看用量（token + 估算费用 + 余额），随时可用
USAGE_ALIASES = {
    "查看用量", "用量", "用了多少", "token用量", "消耗多少", "花了多少",
    "查看消耗", "用量统计", "api用量", "看看用量", "花了多少钱",
}

# 查看搜索缓存
CACHE_ALIASES = {
    "查看缓存", "缓存", "缓存库", "看看缓存", "缓存统计", "搜索缓存",
}

# 模型切换（本地 Ollama / DeepSeek 在线），随时可用
MODEL_ALIASES = {
    "切换模型", "模型切换", "模型状态", "当前模型", "切换本地", "本地模式",
    "切换在线", "在线模式", "切换deepseek", "切换云端", "本地模型", "用什么模型",
}

# 清空所有任务（危险操作，需二次确认）
CLEAR_TASKS_ALIASES = {
    "清空任务", "清空所有任务", "清除所有任务", "清除任务", "清空任务列表",
    "清空历史任务", "删除所有任务", "清空记录", "重置任务", "清任务",
}
CLEAR_CONFIRM_WORDS = {"确认清空", "确认清除", "确认清空任务", "确认删除", "确认", "确定", "yes"}
CLEAR_CANCEL_WORDS = {"取消清空", "不清空", "算了不清", "取消删除"}


def _apply_model_switch(t):
    """处理模型切换命令，返回给用户的文本。"""
    tl = t.lower()
    changed = []
    # 指定本地模型名，如「本地模型 qwen3:4b」
    m = re.search(r"(qwen[\w.:\-]*|llama[\w.:\-]*|gemma[\w.:\-]*|phi[\w.:\-]*|mistral[\w.:\-]*)", tl)
    if m:
        config.set_many({"model_local": m.group(1), "provider": "local"})
        changed.append(f"本地模型已设为 {m.group(1)}（自动切到本地模式）")
        db.add_event(None, "config", f"本地模型切换为 {m.group(1)}")
    elif "本地" in t:
        config.set_many({"provider": "local"})
        changed.append("已切换到本地 Ollama（零 API 费用）")
        db.add_event(None, "config", "切换到本地模型")
    elif "deepseek" in tl or "在线" in t or "云端" in t or "远程" in t:
        config.set_many({"provider": "deepseek"})
        changed.append("已切换到 DeepSeek 在线")
        db.add_event(None, "config", "切换到 DeepSeek")
    elif tl in ("切换模型", "模型切换", "当前模型", "模型状态", "用什么模型") or not changed:
        cur = config.get("provider") or "deepseek"
        provider = "deepseek" if cur == "local" else "local"
        config.set_many({"provider": provider})
        changed.append("已切换到 " + ("本地 Ollama" if provider == "local" else "DeepSeek 在线"))
        db.add_event(None, "config", f"模型切换为 {provider}")
    prov = config.get("provider") or "deepseek"
    ok, detail = ai.ollama_status()
    lines = ["▎模型状态",
             "当前使用：" + (f"本地 Ollama（{config.get('model_local') or '未设置'}）" if prov == "local" else "DeepSeek 在线")]
    lines.append(f"本地 Ollama：{'✅ 可用 · 模型: ' + detail if ok else '❌ 不可达（' + detail + '，会自动切 DeepSeek）'}")
    lines.append("计费：本地 0 元 / DeepSeek 按 token 计费")
    lines.extend(changed)
    lines.append("切换说法：切换本地 / 切换在线 / 本地模型 qwen3:4b")
    return "\n".join(lines)


def handle_command(text, uid, group_id=None):
    """控制命令。返回 True 表示已处理。"""
    global _current_task_id
    t = text.strip()

    # 清空任务的二次确认态（优先于其他命令）
    if _clear_state["open"]:
        if time.time() > _clear_state["expires"]:
            _clear_state["open"] = False
            bus.send_text(uid, "清空确认已超时，未做任何改动。", group_id)
            return True
        if t in CLEAR_CONFIRM_WORDS:
            _clear_state["open"] = False
            return _do_clear_all_tasks(uid, group_id)
        if t in CLEAR_CANCEL_WORDS or t in ("停止", "取消", "算了", "stop"):
            _clear_state["open"] = False
            bus.send_text(uid, "已取消清空，数据未改动。", group_id)
            return True
        # 其他内容 = 视为放弃清空，继续当普通消息处理
        _clear_state["open"] = False

    # 清空所有任务（先预览条数 + 二次确认）
    if t.lower() in CLEAR_TASKS_ALIASES:
        _clear_state["open"] = True
        _clear_state["expires"] = time.time() + 60
        stats = db.task_stats()
        bus.send_text(uid,
            "⚠️ 确认清空所有任务？\n"
            f"将永久删除：任务 {stats.get('tasks',0)} 条、Agent 记录 {stats.get('agents_runs',0)} 条、"
            f"追溯事件 {stats.get('events',0)} 条、消息 {stats.get('messages',0)} 条、"
            f"附件 {stats.get('attachments',0)} 条、用量 {stats.get('api_usage',0)} 条。\n"
            "保留：配置、记忆、搜索缓存、本地模型设置。\n"
            "此操作不可恢复。确认请发『确认清空』，取消请发『取消清空』（60 秒内有效）。", group_id)
        return True

    # 设置密钥 / 链接
    setting = config.parse_setting(t)
    if setting:
        kind, value = setting
        if kind == "api_key":
            config.set_many({"api_key": value})
            bus.send_text(uid, f"API 密钥已保存：{config.mask_key(value)}\n位置：{config.CONFIG_PATH}（权限 600）", group_id)
            db.add_event(None, "config", "设置 API 密钥")
        else:
            if not re.match(r"^https?://[^\s]+$", value):
                bus.send_text(uid, "地址格式不对，应为 http(s):// 开头的 URL", group_id)
            else:
                config.set_many({"base_url": value.rstrip("/")})
                bus.send_text(uid, f"API 地址已保存：{value}", group_id)
        return True

    # 停止
    if t in ("停止", "取消", "算了", "stop", "别干了", "停"):
        with _running_lock:
            tid = _current_task_id
        if tid:
            db.cancel_task(tid)
            rows = db.agents_for_task(tid)
            done = [r["name"] for r in rows if r["state"] == "done"]
            undone = [r["name"] for r in rows if r["state"] in ("queued", "starting", "thinking", "tool_calling", "waiting_api")]
            bus.send_text(uid, f"已停止任务 #{tid}\n已完成：{'、'.join(done) or '无'}\n未完成：{'、'.join(undone) or '无'}\n结果已保留。", group_id)
        else:
            bus.send_text(uid, "当前没有正在运行的任务。", group_id)
        return True

    # 查看 agent 列表（运行中）
    if t in ("查看agent列表", "查看agent", "agent列表", "查看agents", "现在在干嘛"):
        with _running_lock:
            tid = _current_task_id
        if tid:
            bus.send_text(uid, agents.agent_list_text(tid), group_id)
        else:
            bus.send_text(uid, "当前没有运行中的任务。任务开始后可用此命令查看。", group_id)
        return True

    # 查看 N
    m = re.match(r"^查看\s*(\d+)$", t)
    if m:
        with _running_lock:
            tid = _current_task_id
        if tid:
            bus.send_text(uid, agents.agent_detail_text(tid, int(m.group(1))), group_id)
        else:
            bus.send_text(uid, "当前没有运行中的任务。", group_id)
        return True

    # 查看余额 / API 信息（随时可用，不必先进菜单；大小写不敏感）
    if t.lower() in API_INFO_ALIASES:
        bus.send_text(uid, ai.api_info_text(), group_id)
        return True

    # 查看用量：全量 token + 估算费用 + 余额
    if t.lower() in USAGE_ALIASES:
        bus.send_text(uid, ai.usage_text(), group_id)
        return True

    # 查看搜索缓存
    if t.lower() in CACHE_ALIASES:
        bus.send_text(uid, cache.stats_text(), group_id)
        return True

    # 模型切换（本地 Ollama / DeepSeek 在线）
    if t.lower() in MODEL_ALIASES:
        bus.send_text(uid, _apply_model_switch(t), group_id)
        return True

    # 按需导出原始全量追溯文档（Markdown 文件）
    m = TRACE_EXPORT_RE.match(t)
    if m:
        return _handle_trace_export(uid, group_id, int(m.group(2)) if m.group(2) else None)

    # 历史追溯
    m = re.match(r"^追溯\s*(\d+)$", t)
    if m:
        return _handle_trace(int(m.group(1)), uid, group_id)

    # 空闲时直接回数字（菜单已过期也能用，避免数字被当成任务）
    if t.isdigit() and 1 <= int(t) <= 13:
        with _running_lock:
            running = _current_task_id is not None
        if not running and _work_queue.empty():
            return handle_menu_selection(t, uid, group_id)

    # 菜单（仅空闲）
    if t in ("菜单", "menu", "帮助", "?", "？", "你能干什么", "有哪些功能"):
        with _running_lock:
            running = _current_task_id is not None
        if running or not _work_queue.empty():
            bus.send_text(uid, "任务进行中，结束后可用菜单。", group_id)
        else:
            _show_menu(uid, group_id)
        return True

    return False


def _do_clear_all_tasks(uid, group_id=None):
    """真正执行清空：停当前任务 → 清内存队列/上下文 → 清库。"""
    global _current_task_id
    # 1) 停止正在运行的任务（避免清空后 sql 又写回）
    with _running_lock:
        rid = _current_task_id
    stopped = False
    if rid:
        db.cancel_task(rid)
        stopped = True
        # 等运行中的任务真正退出（最多 8 秒），避免清库后残留写回
        deadline = time.time() + 8
        while time.time() < deadline:
            with _running_lock:
                if _current_task_id is None:
                    break
            time.sleep(0.2)
    # 2) 清内存待聚合窗口（连同定时器）
    with _pending_lock:
        for tid, b in list(_pending.items()):
            if b.timer:
                try:
                    b.timer.cancel()
                except Exception:
                    pass
        _pending.clear()
    # 3) 清工作队列中的排队任务
    drained = 0
    while True:
        try:
            item = _work_queue.get_nowait()
        except queue.Empty:
            break
        drained += 1
        _work_queue.task_done()
    # 4) 清多轮对话上下文 / 菜单 / 文档确认态
    with _history_lock:
        _user_history.clear()
    _menu_state["open"] = False
    _confirm_state["open"] = False
    # 5) 落库清空（保留 config / 记忆 / 搜索缓存）
    try:
        counts = db.clear_all_tasks()
    except Exception as e:
        bus.send_text(uid, f"清空失败：{type(e).__name__}: {e}", group_id)
        return True
    with _running_lock:
        _current_task_id = None
    db.add_event(None, "clear_tasks",
                 f"任务{counts.get('tasks',0)} Agent{counts.get('agents_runs',0)} "
                 f"事件{counts.get('events',0)} 消息{counts.get('messages',0)} "
                 f"附件{counts.get('attachments',0)} 用量{counts.get('api_usage',0)} "
                 f"停止运行中={'是' if stopped else '否'} 清空排队={drained}")
    bus.send_text(uid,
        f"✅ 已清空所有任务\n"
        f"任务 {counts.get('tasks',0)} 条、Agent {counts.get('agents_runs',0)} 条、"
        f"追溯 {counts.get('events',0)} 条、消息 {counts.get('messages',0)} 条、"
        f"附件 {counts.get('attachments',0)} 条、用量 {counts.get('api_usage',0)} 条\n"
        f"另清空排队 {drained} 项、对话上下文已重置"
        + ("（已停止正在运行的任务）" if stopped else "")
        + "\n配置 / 记忆 / 搜索缓存 / 本地模型设置均已保留。", group_id)
    return True


def _api_brief():
    """菜单顶部的 API 摘要（密钥脱敏）。"""
    return (f"API：{config.get('base_url')} · {config.mask_key(config.get('api_key'))} · "
            f"{config.get('model_fast')}")


def _show_menu(uid, group_id=None):
    _menu_state["open"] = True
    _menu_state["expires"] = time.time() + 120
    bus.send_text(uid,
        "【助手菜单】\n"
        f"{_api_brief()}\n"
        "1、设置 API 密钥\n"
        "2、设置 API 地址\n"
        "3、查看上下文用量\n"
        "4、查看记忆命中\n"
        "5、查看任务队列\n"
        "6、停止当前任务\n"
        "7、文档保留策略\n"
        "8、历史追溯（发「导出追溯」可拿原始全量 .md 文档）\n"
        "9、查看余额（含 API 地址/密钥/模型）\n"
        "10、查看用量（token 明细 + 估算费用）\n"
        "11、查看搜索缓存（命中可零 token 复用）\n"
        "12、模型切换（本地 Ollama ↔ DeepSeek 在线）\n"
        "13、清空所有任务（危险·需二次确认，保留记忆/缓存/设置）\n"
        "回复数字或直接说话都可以（120 秒内有效）", group_id)


def handle_menu_selection(text, uid=None, group_id=None):
    t = text.strip()
    tl = t.lower()
    natural = ("用了多少", "还剩多少额度", "记住我什么了", "命中几条", "现在在干嘛",
               "有几件事", "别干了", "停")
    if (not t.isdigit() and tl not in natural
            and tl not in API_INFO_ALIASES and tl not in USAGE_ALIASES
            and tl not in CACHE_ALIASES and tl not in MODEL_ALIASES
            and tl not in CLEAR_TASKS_ALIASES and t not in CLEAR_CONFIRM_WORDS
            and t not in CLEAR_CANCEL_WORDS):
        return False
    _menu_state["open"] = False
    mapping = {"用了多少": "3", "还剩多少额度": "3", "记住我什么了": "4",
               "命中几条": "4", "现在在干嘛": "5", "有几件事": "5",
               "别干了": "6", "停": "6"}
    key = mapping.get(t, t)

    if key == "1":
        bus.send_text(uid, "请直接发送：设置密钥sk-xxxx（一行搞定，冒号可带可不带）", group_id)
    elif key == "2":
        bus.send_text(uid, "请直接发送：设置链接https://api.deepseek.com", group_id)
    elif key == "3":
        rows = db.query("SELECT SUM(tokens_in) ti, SUM(tokens_out) to_ FROM agents_runs")
        r = rows[0] if rows else {}
        ti = r.get("ti") or 0
        to = r.get("to_") or 0
        window = config.get("context_window", 64000)
        today = db.query_one("SELECT SUM(tokens_in+tokens_out) s FROM agents_runs WHERE started_at LIKE ?",
                             (datetime.now().strftime("%Y-%m-%d") + "%",))
        today_s = (today or {}).get("s") or 0
        remain = max(0, 100 - int(ti / window * 100)) if window else 100
        bus.send_text(uid, f"▎上下文用量\n累计输入：{ti:,} tokens\n累计输出：{to:,} tokens\n今日消耗：{today_s:,} tokens\n窗口：{window:,} · 剩余约 {remain}%", group_id)
    elif key == "4":
        bus.send_text(uid, memory.stats_text(), group_id)
    elif key == "5":
        bus.send_text(uid, queue_text(), group_id)
    elif key == "6":
        return handle_command("停止", uid, group_id)
    elif key == "7":
        keep = config.get("doc_keep_default", "7d")
        bus.send_text(uid, f"▎文档保留策略\n当前默认：{'长期' if keep=='forever' else '短期（7 天）'}\n回复：默认长期 / 默认短期 / 查看待清理文档", group_id)
    elif key == "8":
        latest = db.latest_task()
        if not latest:
            bus.send_text(uid, "暂无历史任务。", group_id)
        else:
            rows = db.agents_for_task(latest["id"])
            lines = [f"▎历史追溯 · 最近任务 #{latest['id']}",
                     f"{(latest.get('raw_text') or '')[:60]}", "Agent 列表："]
            for i, r in enumerate(rows, 1):
                lines.append(f"  {i}. {r['name']} — {'完成' if r['state']=='done' else r['state']}")
            lines.append("回复「追溯 N」看某个 Agent 的完整动作")
            bus.send_text(uid, "\n".join(lines), group_id)
    elif key == "9" or tl in API_INFO_ALIASES:
        bus.send_text(uid, ai.api_info_text(), group_id)
    elif key == "10" or tl in USAGE_ALIASES:
        bus.send_text(uid, ai.usage_text(), group_id)
    elif key == "11" or tl in CACHE_ALIASES:
        bus.send_text(uid, cache.stats_text(), group_id)
    elif key == "12" or tl in MODEL_ALIASES:
        bus.send_text(uid, _apply_model_switch(t), group_id)
    elif key == "13" or tl in CLEAR_TASKS_ALIASES:
        return handle_command("清空所有任务", uid, group_id)
    elif t in CLEAR_CONFIRM_WORDS:
        return handle_command(t, uid, group_id)
    elif t in CLEAR_CANCEL_WORDS:
        return handle_command(t, uid, group_id)
    return True


def _handle_trace_export(uid, group_id=None, task_id=None):
    """把指定任务（默认最近任务）的原始全量记录生成 .md 并发 QQ 文件。"""
    try:
        path, content, tid = trace.build_trace_markdown(task_id)
        if not path:
            bus.send_text(uid, "暂无历史任务。", group_id)
            return True
        res = bus.send_file(uid, path, group_id)
        ret = res.get("retcode") if isinstance(res, dict) else -1
        if ret == 0:
            bus.send_text(uid, f"任务 #{tid} 的原始全量追溯文档已生成（.md，{len(content)} 字），见上方文件。", group_id)
        else:
            err = (res.get("error") or res) if isinstance(res, dict) else res
            bus.send_text(uid, f"文档已生成，但发送失败：{err}。本地路径：{path}", group_id)
    except Exception as e:
        bus.send_text(uid, f"导出追溯失败：{type(e).__name__}: {e}", group_id)
    return True


def _handle_trace(index, uid, group_id=None):
    latest = db.latest_task()
    if not latest:
        bus.send_text(uid, "暂无历史任务。", group_id)
        return True
    rows = db.agents_for_task(latest["id"])
    if index < 1 or index > len(rows):
        bus.send_text(uid, f"没有编号 {index} 的 Agent（最近任务共 {len(rows)} 个）", group_id)
        return True
    bus.send_text(uid, agents.agent_detail_text(latest["id"], index), group_id)
    return True


def queue_text():
    running = []
    pending = []
    with _running_lock:
        rid = _current_task_id
    if rid:
        r = db.query_one("SELECT * FROM tasks WHERE id=?", (rid,))
        if r:
            running.append(r)
    for r in db.tasks_by_state(["queued"]):
        if rid and r["id"] == rid:
            continue
        pending.append(r)
    lines = ["任务列表："]
    for r in running:
        lines.append(f"{len(lines)}、[进行中] {(r.get('raw_text') or '')[:80]}")
    for r in pending:
        lines.append(f"{len(lines)}、[排队中] {(r.get('raw_text') or '')[:80]}")
    if len(lines) == 1:
        lines.append("（当前没有任务）")
    return "\n".join(lines)


# ================= 文档保留确认 =================

def handle_doc_confirm(text):
    global _confirm_state
    if not _confirm_state["open"]:
        return False
    if time.time() > _confirm_state["expires"]:
        _finalize_doc_keep("7d")
        return False
    t = text.strip()
    if t in ("1", "长期", "长期保留"):
        _finalize_doc_keep("forever")
        return True
    if t in ("2", "短期", "短期保留"):
        _finalize_doc_keep("7d")
        return True
    # 其他内容 = 新任务 → 默认短期，交回分流
    _finalize_doc_keep("7d")
    return False


def _finalize_doc_keep(keep):
    global _confirm_state
    st = _confirm_state
    if not st["open"]:
        return
    st["open"] = False
    doc_id = st.get("doc_id")
    if doc_id:
        if keep == "forever":
            exp = "2099-12-31 23:59:59"
        else:
            exp = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        db.set_document_keep(doc_id, keep, exp)
    bus.send_text(_current_uid(), f"已按{'长期' if keep=='forever' else '短期（7 天）'}保留。")


def _current_uid():
    return TARGET or config.get("target_qq")


# ================= 任务执行流水线 =================

def _dispatch(bundle):
    """新任务入队。"""
    _work_queue.put(bundle)
    bus.send_text(_current_uid(), queue_text())


def work_loop():
    """单 worker 串行执行任务队列。"""
    global _current_task_id
    while not _shutdown.is_set():
        try:
            bundle = _work_queue.get(timeout=1)
        except queue.Empty:
            _check_doc_timeout()
            continue
        if bundle is None:
            break
        tid = bundle["task_id"]
        with _running_lock:
            if _current_task_id is not None:
                # 理论上单 worker 不会发生，保险起见重新入队
                _work_queue.put(bundle)
                time.sleep(0.5)
                continue
            _current_task_id = tid

        try:
            db.set_task_state(tid, "running")
            db.add_event(tid, "task_start", (bundle.get("text") or "")[:200])
            _execute_task(bundle)
        except Exception as e:
            bus.log(f"任务 #{tid} 异常: {type(e).__name__}: {e}")
            db.set_task_state(tid, "failed")
            bus.send_text(_current_uid(), f"任务 #{tid} 执行出错：{type(e).__name__}: {str(e)[:150]}")
        finally:
            with _running_lock:
                _current_task_id = None
            _work_queue.task_done()
            _check_doc_timeout()


def _direct_chat(tid, user_text, uid, history, mem_ctx=""):
    """普通聊天/追问：带最近上下文直接调模型回答，不建 Agent、不播报列表。
    回答同样写入历史，保证下一次追问能接上（例：查完天气再问『这是几点的?』）。"""
    sys = ("你是 QQ 聊天助手，回答要简短自然。\n"
           "最近你和用户刚聊过下面的内容，用户现在是在接着聊或追问，请结合上下文作答：\n")
    if history:
        lines = []
        for h in history[-6:]:
            role = "用户" if h.get("role") == "user" else "助手"
            lines.append(f"{role}：{str(h.get('content') or '')[:300]}")
        sys += "\n".join(lines) + "\n"
    if mem_ctx:
        sys += "\n相关记忆（仅作参考）：\n" + mem_ctx[:800] + "\n"
    sys += ("\n要求：直接回答用户这句话，不要复述上面的历史，不要创建任务或搜索，"
            "不知道就说不知道。")
    messages = [{"role": "system", "content": sys},
                {"role": "user", "content": user_text}]
    db.add_event(tid, "direct_chat", f"普通聊天直答（不建 Agent）：{user_text[:80]}")
    try:
        msg, usage = ai.chat(messages, scene="direct_chat", thinking=ai.THINK_OFF, timeout=90)
        content = (msg.get("content") or "").strip()
    except Exception as e:
        bus.log(f"直接聊天失败: {type(e).__name__}: {e}")
        content = ""
    # 跑题/编故事拦截：本地小模型偶发把「查」理解成查案，输出无关叙事
    if content and agents.looks_off_topic(user_text, content):
        bus.log(f"直答输出跑题，已拦截：{content[:80]}")
        db.add_event(tid, "off_topic_blocked", content[:200])
        content = ""
    if not content:
        content = "（这个问题我暂时答不上来，你可以换个说法，或让我帮你查一下。）"
    bus.send_text(uid, content)
    _add_history(user_text, content)
    db.set_task_state(tid, "done")
    db.add_event(tid, "task_done", content[:200])
    return content


def _execute_task(bundle):
    tid = bundle["task_id"]
    user_text = bundle.get("text") or ""
    attachments = [a for a in bundle.get("attachments") if a.get("path")]
    uid = _current_uid()

    if db.is_cancelled(tid):
        db.set_task_state(tid, "cancelled")
        return

    # 记忆召回：只对稳定事实类注入（时效类不塞上下文，避免白花 token）
    mem_ctx = ""
    try:
        _cat, _ttl = cache.classify_ttl(user_text)
        if _cat == "fact":
            mem_ctx, hits = memory.recall(user_text, record=True)
            if hits:
                db.add_event(tid, "memory_hit", f"命中 {len(hits)} 条（稳定事实类，已注入）")
        else:
            db.add_memory_hit(user_text, 0, [])  # 时效类仍记统计，但不召回不注入
    except Exception:
        pass

    history = _recent_history()
    # ⓪ 上下文串联：省略句/「我要你去查」补成完整目标（否则机器当成没有主语的新任务乱搜）
    effective_text = agents.resolve_query(user_text, history)
    if effective_text != user_text:
        db.add_event(tid, "context_resolve", f"原话：{user_text[:80]} → 补全：{effective_text[:120]}")
        bus.log(f"#{tid} 上下文补全: {user_text[:30]} → {effective_text[:60]}")

    # 分流：普通聊天/追问/寒暄 → 带上下文直接答，不建 Agent、不播报列表
    if not agents.needs_agent(user_text, attachments, history):
        return _direct_chat(tid, user_text, uid, history, mem_ctx)

    # ① 协调器拆解（用补全后的目标，保证搜索词里有航线/日期等具体信息）
    coord = agents.CoordinatorAgent(tid, 1, effective_text, attachments, history,
                                    memory_ctx=mem_ctx)
    try:
        subtasks = coord.run()
    except agents.Cancelled:
        db.set_task_state(tid, "cancelled")
        return
    except Exception as e:
        bus.log(f"协调器异常: {e}")
        subtasks = [{"name": "执行任务", "type": "general", "goal": user_text}]

    if db.is_cancelled(tid):
        db.set_task_state(tid, "cancelled")
        return

    # ② 播报一次 Agent 列表（不再重复粘贴用户原话）
    bus.send_text(uid, "创建agent列表\n" +
                  "\n".join(f"{i}.{s['name']}" for i, s in enumerate(subtasks, 1)))

    # ③ 并行执行
    results = agents.run_subtasks(tid, subtasks, attachments, base_seq=2,
                                  cancel_check=lambda: db.is_cancelled(tid),
                                  memory_ctx=mem_ctx)

    if db.is_cancelled(tid):
        db.set_task_state(tid, "cancelled")
        bus.send_text(uid, f"任务 #{tid} 已取消，已完成的结果已保留。")
        return

    # ④ 整理
    syn = agents.SynthesizerAgent(tid, 1 + len(subtasks) + 1, user_text, results)
    try:
        final_text = syn.run()
    except Exception as e:
        final_text = "\n\n".join(f"【{r['name']}】\n{r.get('result') or r.get('error','')}" for r in results)

    # ⑤ 发送
    bus.send_text(uid, final_text)
    _add_history(user_text, final_text)

    # ⑥ 反馈：文档保留确认
    doc_rows = db.query("SELECT d.id, d.file_name FROM documents d JOIN attachments a ON d.attachment_id=a.id WHERE a.task_id=? AND d.keep='pending'", (tid,))
    if doc_rows:
        names = "、".join(f"《{d['file_name']}》" for d in doc_rows[:3])
        _confirm_state.update({"open": True, "task_id": tid,
                               "doc_id": doc_rows[0]["id"],
                               "expires": time.time() + 30})
        bus.send_text(uid, f"{names} 已处理完成。\n是否长期保留？\n1、长期保留   2、短期保留（默认 7 天）\n30 秒内不回复，或期间发来新任务，将按短期处理。")

    db.set_task_state(tid, "done")
    db.add_event(tid, "task_done", final_text[:200])

    # 异步抽取记忆
    t = threading.Thread(target=memory.extract_and_store, args=(tid, user_text, final_text), daemon=True)
    t.start()


def _check_doc_timeout():
    st = _confirm_state
    if st["open"] and time.time() > st["expires"]:
        _finalize_doc_keep("7d")


def _recent_history():
    with _history_lock:
        return list(_user_history[-6:])


def _add_history(user, assistant):
    with _history_lock:
        _user_history.append({"role": "user", "content": user[:2000]})
        _user_history.append({"role": "assistant", "content": assistant[:2000],
                              "reasoning_content": ""})
        if len(_user_history) > 20:
            del _user_history[:-20]


# ================= 启动 =================

def start_background():
    db.init()
    global TARGET
    TARGET = config.get("target_qq")

    # 将已处理但未确认的附件标记为 documents pending（简化：全部文档附件登记）
    for a in db.query("SELECT * FROM attachments WHERE ready=1 AND kind IN ('image','file','video','audio')"):
        exists = db.query_one("SELECT id FROM documents WHERE attachment_id=?", (a["id"],))
        if not exists:
            db.add_document(a["id"], a.get("orig_name") or "附件", a["path"], "pending", "", "")

    w = threading.Thread(target=work_loop, daemon=True, name="workloop")
    w.start()

    # 恢复上次运行时卡在 queued 的任务（重启/崩溃后不丢任务）
    try:
        stuck = db.query("SELECT id, raw_text, progress FROM tasks WHERE state='queued' ORDER BY id")
        for r in stuck:
            b = {"task_id": r["id"], "text": r.get("raw_text") or "",
                 "attachments": [], "trigger": "重启恢复"}
            _work_queue.put(b)
            bus.log(f"恢复排队任务 #{r['id']}: {(r.get('raw_text') or '')[:40]}")
    except Exception as e:
        bus.log(f"恢复排队任务失败: {e}")

    # 定时清理过期文档（每 10 分钟）
    def cleanup_loop():
        while not _shutdown.is_set():
            time.sleep(600)
            try:
                bus.cleanup_expired_docs()
                memory.forget_maintenance()
            except Exception as e:
                bus.log(f"清理任务异常: {e}")
    threading.Thread(target=cleanup_loop, daemon=True).start()

    bus.log("核心编排已启动")
