#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""原始全量追溯文档生成器：把某个任务的所有 Agent 记录整理成 Markdown。"""
import os
import json
from datetime import datetime, timedelta, timezone

import db
import paths

EXPORT_DIR = paths.EXPORT_DIR


def _bj(ts):
    """数据库时间戳按 UTC 存，展示时转北京时间。"""
    if not ts:
        return "—"
    try:
        t = datetime.strptime(str(ts)[:19], "%Y-%m-%d %H:%M:%S")
        return (t + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


def _now_bj():
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def _cell(value, limit=300):
    s = str(value if value is not None else "")
    s = s.replace("|", "\\|").replace("\r", " ").replace("\n", " ")
    return s[:limit]


def _fence(content, lang=""):
    text = str(content if content is not None else "")
    text = text.replace("````", "```` ")
    return "````" + lang + "\n" + text + "\n````"


def _pretty_json(raw):
    s = str(raw or "")
    if not s:
        return "（空）"
    try:
        return json.dumps(json.loads(s), ensure_ascii=False, indent=2)
    except Exception:
        return s


def build_trace_markdown(task_id=None):
    """返回 (文件路径, 文档内容, 任务号)；没有任务返回 (None, None, None)。"""
    task = db.task_by_id(task_id) if task_id else db.latest_task()
    if not task:
        return None, None, None

    tid = task["id"]
    agents = db.agents_for_task(tid)
    events = db.events_for_task(tid)
    attachments = db.attachments_for_task(tid)

    final_result = ""
    for a in reversed(agents):
        if a.get("role") == "synthesizer" and a.get("result"):
            final_result = a["result"]
            break
    if not final_result:
        for a in reversed(agents):
            if a.get("result"):
                final_result = a["result"]
                break

    lines = []
    lines.append(f"# Agent 追溯报告 · 任务 #{tid}")
    lines.append("")
    lines.append(f"> 导出时间（北京时间）：{_now_bj()}")
    lines.append("> 原始全量版：包含模型推理、工具原始入参/出参、服务端工具调用原文，内容未删改，可能较长。")
    lines.append("")

    lines.append("## 1. 任务摘要")
    lines.append("")
    lines.append("| 字段 | 值 |")
    lines.append("|---|---|")
    lines.append(f"| 任务号 | {tid} |")
    lines.append(f"| 状态 | {_cell(task.get('state'))} |")
    lines.append(f"| 触发方式 | {_cell(task.get('trigger'))} |")
    lines.append(f"| 创建时间（北京） | {_bj(task.get('created_at'))} |")
    lines.append(f"| 更新时间（北京） | {_bj(task.get('updated_at'))} |")
    lines.append(f"| Agent 数量 | {len(agents)} |")
    lines.append("")
    lines.append("**用户原始需求：**")
    lines.append("")
    lines.append(_fence(task.get("raw_text") or ""))
    lines.append("")
    lines.append("**最终回复：**")
    lines.append("")
    lines.append(_fence(final_result or "（无）"))
    lines.append("")

    lines.append("## 2. Agent 总览")
    lines.append("")
    if agents:
        lines.append("| # | 名称 | 角色 | 状态 | 开始（北京） | 结束（北京） | 耗时 | tokens 入/出 | 自愈 |")
        lines.append("|---|---|---|---|---|---|---|---|---|")
        for i, a in enumerate(agents, 1):
            lines.append(
                f"| {i} | {_cell(a.get('name'), 40)} | {_cell(a.get('role'), 20)} | "
                f"{_cell(a.get('state'), 30)} | {_bj(a.get('started_at'))} | {_bj(a.get('ended_at'))} | "
                f"{a.get('elapsed', 0) or 0:.2f}s | {a.get('tokens_in', 0) or 0}/{a.get('tokens_out', 0) or 0} | "
                f"{a.get('heal_count', 0) or 0} |")
    else:
        lines.append("（该任务没有 Agent 记录）")
    lines.append("")

    lines.append("## 3. Agent 逐个复盘")
    lines.append("")
    for i, a in enumerate(agents, 1):
        aid = a["id"]
        lines.append(f"### 3.{i} Agent #{aid} · {a.get('name')}（{a.get('role')}）")
        lines.append("")
        lines.append(f"- 状态：`{a.get('state')}`")
        lines.append(f"- 开始：{_bj(a.get('started_at'))}")
        lines.append(f"- 结束：{_bj(a.get('ended_at'))}")
        lines.append(f"- 耗时：{a.get('elapsed', 0) or 0:.2f} 秒")
        lines.append(f"- tokens：入 {a.get('tokens_in', 0) or 0} / 出 {a.get('tokens_out', 0) or 0}")
        lines.append(f"- 自愈次数：{a.get('heal_count', 0) or 0}")
        if a.get("error"):
            lines.append(f"- 错误：{_cell(a.get('error'), 500)}")
        lines.append("")

        lines.append("**输出 / 结果：**")
        lines.append("")
        lines.append(_fence(a.get("result") or "（无）"))
        lines.append("")

        lines.append("**动作时间线：**")
        lines.append("")
        actions = db.actions_for_agent(aid)
        if actions:
            lines.append("| 序号 | 时间（北京） | 类型 | 动作 | 摘要 |")
            lines.append("|---|---|---|---|---|")
            for act in actions:
                lines.append(
                    f"| {act.get('seq')} | {_bj(act.get('ts'))} | {_cell(act.get('action_type'), 30)} | "
                    f"{_cell(act.get('detail'), 400)} | {_cell(act.get('result_summary'), 400)} |")
        else:
            lines.append("（无动作记录）")
        lines.append("")

        lines.append("**原始全量记录：**")
        lines.append("")
        raws = db.raw_for_agent(aid)
        if raws:
            for raw in raws:
                lines.append(f"#### 原始记录 {raw.get('seq')} · {raw.get('kind')} · {_bj(raw.get('ts'))}")
                lines.append("")
                if raw.get("kind") in ("web_server_tools", "web_annotations", "classify_output"):
                    lines.append(_fence(_pretty_json(raw.get("content"))))
                else:
                    lines.append(_fence(raw.get("content") or ""))
                lines.append("")
        else:
            lines.append("（该 Agent 运行于旧版本，未保存原始推理；仅有上面的动作记录。）")
            lines.append("")
        lines.append("---")
        lines.append("")

    lines.append("## 4. 系统事件")
    lines.append("")
    if events:
        lines.append("| # | 时间（北京） | 类型 | 内容 |")
        lines.append("|---|---|---|---|")
        for e in events:
            lines.append(
                f"| {e.get('id')} | {_bj(e.get('ts'))} | {_cell(e.get('event_type'), 30)} | "
                f"{_cell(e.get('payload'), 500)} |")
    else:
        lines.append("（无事件记录）")
    lines.append("")

    lines.append("## 5. 本次任务附件")
    lines.append("")
    if attachments:
        lines.append("| # | 类型 | 文件名 | 路径 |")
        lines.append("|---|---|---|---|")
        for a in attachments:
            lines.append(
                f"| {a.get('id')} | {_cell(a.get('kind'), 20)} | {_cell(a.get('orig_name'), 120)} | "
                f"{_cell(a.get('path'), 200)} |")
    else:
        lines.append("（无附件）")
    lines.append("")

    lines.append("## 6. 记忆命中")
    lines.append("")
    stats = db.memory_hit_stats(20)
    lines.append(_fence(json.dumps(stats, ensure_ascii=False, indent=2)))
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(f"报告生成：QQ AI Assistant · 北京时间 {_now_bj()}")

    content = "\n".join(lines)
    os.makedirs(EXPORT_DIR, exist_ok=True)
    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(EXPORT_DIR, f"追溯_任务{tid}_{stamp}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path, content, tid
