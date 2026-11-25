#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DeepSeek 客户端：Chat Completions（含 function calling + reasoning_content 回传）
+ Search / Fetch / OCR API。纯标准库。"""
import os
import json
import re
import time
import base64
import html
import urllib.request
import urllib.error

import config
import bus
import db

# 思考档位：简单调用关闭思考；只有需要拆解/推理的地方用 low
# DeepSeek 官方文档：thinking enabled 时进入深度思考搜索，最多约 20 步反复开网页
# 关闭思考后服务端联网走普通搜索（一步），网页不回灌长正文，token 大幅下降
THINK_OFF = "off"
THINK_LOW = "low"
THINK_HIGH = "high"

# 官方空闲时段价（元/百万 token）。高峰时段约为 2 倍，估算按空闲价。
_PRICE = {
    "deepseek-flash": {"in": 1.0, "cached": 0.02, "out": 4.0},
    "deepseek-v4-pro": {"in": 4.5, "cached": 0.15, "out": 13.5},
}


def _usage_fields(usage):
    """兼容不同 usage 字段名，抽出统一的记账字段。"""
    u = usage or {}
    pin = int(u.get("prompt_tokens") or u.get("input_tokens") or 0)
    pout = int(u.get("completion_tokens") or u.get("output_tokens") or 0)
    pd = u.get("prompt_tokens_details") or {}
    cd = u.get("completion_tokens_details") or u.get("output_tokens_details") or {}
    cached = int(pd.get("cached_tokens") or u.get("cache_read_input_tokens") or 0)
    reason = int(cd.get("reasoning_tokens") or cd.get("thinking_tokens") or 0)
    st = u.get("server_tool_use") or {}
    tools = 0
    if isinstance(st, dict):
        for k in ("tool_search", "tool_fetch", "web_search_requests", "web_fetch_requests"):
            tools += int(st.get(k) or 0)
    return pin, pout, reason, cached, tools


def _record_usage(scene, model, usage, agent=None):
    """每次模型调用后立即落库，失败不影响主流程。"""
    try:
        pin, pout, reason, cached, tools = _usage_fields(usage)
        db.add_usage(scene, model or config.get("model_fast"),
                     pin, pout, reason, cached, tools,
                     task_id=(getattr(agent, "task_id", None) if agent else None),
                     agent_id=(getattr(agent, "id", None) if agent else None))
    except Exception:
        pass


def _estimate_cost(model, pin, pout, cached):
    m = str(model or "")
    if m in _PRICE:
        p = _PRICE[m]
    elif m.startswith(_LOCAL_ZERO):
        return 0.0  # 本地模型零费用
    else:
        p = _PRICE["deepseek-flash"]
    uncached = max(0, pin - cached)
    return (uncached * p["in"] + cached * p["cached"] + pout * p["out"]) / 1_000_000

TOOL_DEFS = [
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "联网搜索网页信息，返回标题、摘要、链接。适合查价格、天气、新闻等实时信息。",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "搜索词，支持 site: 语法"},
                    "ori_question": {"type": "string", "description": "用户原始问题"},
                    "count": {"type": "integer", "description": "结果条数，1-10", "default": 5},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "web_fetch",
            "description": "抓取指定网页的正文（Markdown）。已知 URL 时用。",
            "parameters": {
                "type": "object",
                "properties": {"url": {"type": "string", "description": "完整的 http(s) URL"}},
                "required": ["url"],
            },
        },
    },
]


class AIError(Exception):
    def __init__(self, message, kind="unknown", status=0):
        super().__init__(message)
        self.kind = kind
        self.status = status


def _key():
    k = (config.get("api_key", "") or "").strip()
    if not k:
        raise AIError("API Key 未配置。请发送：设置密钥sk-xxxx", kind="auth", status=401)
    return k


def _base():
    return (config.get("base_url", "") or "https://api.deepseek.com").rstrip("/")


def _post(path, body, timeout=90, key=None):
    url = _base() + path
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer " + (key or _key()),
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        msg = raw[:300]
        try:
            msg = (json.loads(raw).get("error") or {}).get("message", raw[:300])
        except Exception:
            pass
        kind = "auth" if e.code in (401, 402, 403) else ("network" if e.code in (408, 429) or e.code >= 500 else "params")
        raise AIError(f"HTTP {e.code}: {msg}", kind=kind, status=e.code)
    except urllib.error.URLError as e:
        raise AIError(f"网络错误: {e.reason}", kind="network")
    except Exception as e:
        raise AIError(f"{type(e).__name__}: {e}", kind="unknown")


# ---------------- 本地 Ollama ----------------

_LOCAL_ZERO = ("qwen", "llama", "gemma", "phi", "mistral", "deepseek-r1")


def _ollama_url():
    return (config.get("ollama_url") or "http://127.0.0.1:11434").rstrip("/")


def _ollama_post(path, body, timeout=120):
    url = _ollama_url() + path
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", "replace"))
    except urllib.error.URLError as e:
        raise AIError(f"Ollama 连接失败: {e.reason}", kind="network")
    except Exception as e:
        raise AIError(f"Ollama {type(e).__name__}: {e}", kind="unknown")


def ollama_chat(messages, timeout=120, agent=None, scene="chat", thinking=THINK_OFF):
    """本地 Ollama 调用，零 API 费用。返回 (msg dict, usage dict)。"""
    mdl = config.get("model_local") or "qwen3:1.7b"
    if agent:
        agent.set_state("thinking", f"等待本地模型 {mdl} 返回")
    body = {"model": mdl, "messages": messages, "stream": False,
            "think": thinking in (THINK_LOW, THINK_HIGH),
            "keep_alive": "30m",
            "options": {"num_predict": 512}}
    resp = _ollama_post("/api/chat", body, timeout=timeout)
    content = (resp.get("message") or {}).get("content") or ""
    usage = {"prompt_tokens": int(resp.get("prompt_eval_count") or 0),
             "completion_tokens": int(resp.get("eval_count") or 0)}
    _record_usage(scene, mdl, usage, agent)
    if agent and content:
        agent.log_raw("output", content)
    return {"role": "assistant", "content": content}, usage


def _provider_local():
    return (config.get("provider") or "deepseek") == "local"


def _ollama_get(path, timeout=8):
    url = _ollama_url() + path
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", "replace"))
    except urllib.error.URLError as e:
        raise AIError(f"Ollama 连接失败: {e.reason}", kind="network")
    except Exception as e:
        raise AIError(f"Ollama {type(e).__name__}: {e}", kind="unknown")


def ollama_status():
    """检测本地 Ollama 是否可达，返回 (ok, 模型列表文本)。"""
    try:
        resp = _ollama_get("/api/tags")
        names = [m.get("name", "?") for m in resp.get("models") or []]
        return True, "、".join(names) if names else "（未安装任何模型）"
    except Exception as e:
        return False, str(e)[:120]


# ---------------- Chat Completions ----------------

def chat(messages, model=None, tools=None, timeout=90, agent=None, scene="chat",
         thinking=THINK_OFF, allow_local=True):
    """调用 Chat Completions。返回 (assistant_message dict, usage dict)。
    messages 中的 assistant 消息必须原样带上 reasoning_content（坑：否则 400）。
    thinking: off=关闭思考（默认，省 token）；low/high=开启思考。
    allow_local: provider=local 时走本地 Ollama，失败直接抛出由上层用素材兜底，
    不再偷切失效的 DeepSeek 密钥（那只会 401）。
    带 tools 的调用（联网工具循环）和图像理解必须走 DeepSeek，不允许本地。"""
    if allow_local and tools is None and _provider_local():
        # 本地模式：本地失败就抛出，由上层用素材/原文兜底。
        # 绝不偷偷切 DeepSeek——密钥已失效，切过去只会 401，还把 TimeoutError 原文甩给用户。
        return ollama_chat(messages, timeout=timeout, agent=agent,
                           scene=scene, thinking=thinking)
    mdl = model or config.get("model_fast")
    if agent:
        label = {"off": "无思考", "low": "低思考", "high": "高思考"}.get(thinking, thinking)
        agent.set_state("thinking", f"等待模型 {mdl} 返回（{label}）")
    body = {
        "model": mdl,
        "messages": messages,
        "thinking": {"type": "enabled" if thinking in (THINK_LOW, THINK_HIGH) else "disabled"},
        "stream": False,
    }
    if thinking in (THINK_LOW, THINK_HIGH):
        body["reasoning_effort"] = THINK_HIGH if thinking == THINK_HIGH else THINK_LOW
    if tools:
        body["tools"] = tools
        body["tool_choice"] = "auto"
    resp = _post("/v1/chat/completions", body, timeout=timeout)
    try:
        choice = resp["choices"][0]
        msg = choice["message"]
        usage = resp.get("usage", {})
        _record_usage(scene, mdl, usage, agent)
        if agent and msg.get("reasoning_content"):
            agent.log_raw("thinking", msg["reasoning_content"])
        return msg, usage
    except Exception:
        raise AIError("响应格式异常: " + json.dumps(resp, ensure_ascii=False)[:300], kind="logic")


def chat_tool_loop(system, user_text, tool_executor, model=None, max_rounds=6,
                   timeout=90, agent=None, history=None):
    """工具循环：模型决定调用工具 → 执行 → 结果回灌 → 继续，直到模型给出最终文本。
    tool_executor(name, arguments) -> str
    history: 可选的历史 messages（不含本次 system/user）。"""
    messages = [{"role": "system", "content": system}]
    if history:
        messages.extend(history)
    messages.append({"role": "user", "content": user_text})

    total_in = total_out = 0
    for rnd in range(max_rounds):
        msg, usage = chat(messages, model=model, tools=TOOL_DEFS, timeout=timeout,
                          agent=agent, scene="general", thinking=THINK_LOW, allow_local=False)
        total_in += usage.get("prompt_tokens", 0) or 0
        total_out += usage.get("completion_tokens", 0) or 0

        calls = msg.get("tool_calls") or []
        if not calls:
            return {
                "content": msg.get("content") or "",
                "reasoning": msg.get("reasoning_content") or "",
                "tokens_in": total_in,
                "tokens_out": total_out,
                "rounds": rnd + 1,
            }

        # 关键：assistant 消息连同 reasoning_content / tool_calls 原样回传
        messages.append({
            "role": "assistant",
            "content": msg.get("content") or "",
            "reasoning_content": msg.get("reasoning_content") or "",
            "tool_calls": calls,
        })
        for call in calls:
            fn = call.get("function", {})
            name = fn.get("name", "")
            try:
                args = json.loads(fn.get("arguments") or "{}")
            except Exception:
                args = {}
            if agent:
                agent.set_state("tool_calling", f"调用工具 {name}")
                agent.log_action("tool_call", f"{name} {json.dumps(args, ensure_ascii=False)[:120]}")
                agent.log_raw("tool_call_args", f"{name}\n{fn.get('arguments') or '{}'}")
            try:
                result = tool_executor(name, args)
            except Exception as e:
                result = f"工具执行失败: {type(e).__name__}: {e}"
            if agent:
                agent.log_action("tool_result", f"{name} 返回", str(result)[:200])
                agent.log_raw("tool_result", f"{name}\n{result}")
            messages.append({
                "role": "tool",
                "tool_call_id": call.get("id", ""),
                "content": str(result)[:16000],
            })
        if agent:
            agent.set_state("waiting_api", "工具结果已回灌，等待模型继续")

    return {
        "content": "（达到最大工具轮次，以下是已有信息的总结）\n" + (messages[-1].get("content") or ""),
        "reasoning": "",
        "tokens_in": total_in,
        "tokens_out": total_out,
        "rounds": max_rounds,
    }


# ---------------- 服务端联网工具（搜索+读网页+整理一次完成） ----------------

WEB_ANSWER_SYSTEM = """你是中文信息助手。现在是北京时间 {today}。

【联网要求】
必须调用联网工具搜索后再回答，不要凭记忆作答。搜索一次拿到关键信息即可作答，不要反复打开多个网页。

【最高优先级：输出契约】
你的回答会被直接发到 QQ。只输出最终答案，不输出思考过程、工具过程、来源说明、查询时间、解释说明。

【天气类：按用户问的时间输出对应天数，问哪天答哪天】
第 1 行：{{地点}}<时间词>(YYYY年M月D日)天气如下
之后每行一天：<时间词>(M月D日)<天气><emoji>，气温X°～X°，体感X°
最后一行：温馨提醒：<一句简短提醒>～

天气类硬规则：
- 用户没指明时间 → 默认输出今天+明天两天；用户问哪天（明天/后天/大后天/具体日期/星期几）→ 只输出用户问的那些天。
- 今天是 {today}（北京时间）；明天=今天+1，后天=今天+2，大后天=今天+3；星期几按最近的将来那天算。
- 素材里没有对应日期的数据时，该行写「<时间词>(M月D日)未找到可靠数据☁️，气温—°～—°，体感—°」，不要拿别的日期冒充。
- 地点只写用户问的地点（可含区，如「银川市西夏区」）；用户没说地点时写「宁夏」。
- 第 4 行温馨提醒只写一句，按温差/降温/降雨给出，如「昼夜温差大，早晚记得添衣」「明天降温明显，注意保暖」。
- 不要写「详情」「不同预报源」「数据矛盾」「根据搜索结果」等多余文字。
- 不要用 Markdown 标题、加粗、列表符号、分隔线、引用、表格。
- 天气词只用：晴、晴间多云、多云、阴、小雨、中雨、大雨、雪、沙尘、雾、霾。
- emoji 只允许每行的天气词后跟 1 个：晴用🌞，阴用☁️，多云用⛅，雨用🌧️，雪用❄️，沙尘用🌪️，雾用🌫️。
- 气温和体感用整数加「°」符号（如 0°、16°），范围用「～」连接（如 0°～16°），不要用「度」「℃」「到」「左右」「约」。
- 如果查不到体感，就按气温估算一个整数填入，不要留空、不要解释。
- 即使网页数据互相矛盾，也只取一个主流值，禁止解释差异。
- 严禁输出多余行；数据行数=用户问的天数（默认 2 天），加上标题和温馨提醒即为全部。

【非天气类】
- 只输出 3-5 行：第一行标题，后面每行一个关键信息。
- 不输出来源链接、爬取过程、「根据搜索结果」、查询时间、长段转载。
- 票务类最后一行固定写「仅供参考，非实时价格」。
- 今天/明天/最近一律以北京时间 {today} 为准；旧日期数据不得冒充今天。
- 只回答用户问的地点/主题，不混入其他城市、其他地区。

【当前日期】{today}
"""

_WEATHER_KEYWORDS = ("天气", "气温", "温度", "下雨", "下雪", "冷不冷", "热不热", "穿什么")
_WEATHER_TOKENS = (
    "晴间多云", "雨夹雪", "沙尘暴", "沙尘", "小雨", "中雨", "大雨", "暴雨",
    "多云", "阴", "晴", "雪", "雨", "雾", "霾",
)
_WEATHER_EMOJI = {
    "晴": "🌞", "晴间多云": "⛅", "多云": "⛅", "阴": "☁️",
    "小雨": "🌧️", "中雨": "🌧️", "大雨": "🌧️", "暴雨": "🌧️", "雨": "🌧️",
    "雨夹雪": "🌨️", "雪": "❄️", "沙尘暴": "🌪️", "沙尘": "🌪️",
    "雾": "🌫️", "霾": "🌫️",
}
_WEATHER_DISPLAY = {"晴": "晴天", "阴": "阴天", "沙尘": "沙尘", "沙尘暴": "沙尘暴"}


def _is_weather_question(question):
    q = str(question or "")
    return any(k in q for k in _WEATHER_KEYWORDS)


def _weather_location(question):
    q = re.sub(r"[，。？?！!\n\r\t]", " ", str(question or ""))
    q = re.sub(r"\([^)]*\)|（[^）]*）", " ", q)
    # 先剥掉具体日期/星期词，避免混进地名
    q = re.sub(r"\d{1,2}\s*[月\-.]\s*\d{1,2}\s*[日号]?", " ", q)
    q = re.sub(r"(?:星期|礼拜|周)[一二三四五六日天末]", " ", q)
    q = re.sub(
        r"^(?:请|帮我|给我|麻烦)?\s*"
        r"(?:查询一下|查一下|查询|查|看看|看下|看一下|我想知道|我想问)?\s*"
        r"(?:今天|今日|明天|明日|后天|大后天|现在|当前)?\s*",
        "", q).strip()
    m = re.search(r"([\u4e00-\u9fa5]{2,12}?)(?:今天|今日|明天|明日|后天|大后天|现在|当前)?\s*(?:的)?\s*天气", q)
    if m:
        loc = re.sub(r"^(?:现在|当前|本地|这个|那个)", "", m.group(1))
        # 去掉残留的时间词/连接词/助词，保留「银川市西夏区」这类完整地名
        loc = re.sub(r"(?:今天|今日|明天|明日|大后天|后天|现在|当前)", "", loc)
        loc = re.sub(r"(?:和|与|以及|跟|同)+$", "", loc)
        loc = re.sub(r"(?:的)$", "", loc)
        return loc[:12] or "宁夏"
    return "宁夏"


_WEEKDAY_MAP = {"一": 0, "二": 1, "三": 2, "四": 3, "五": 4, "六": 5, "日": 6, "天": 6}
_DAY_NAMES = {0: "今天", 1: "明天", 2: "后天", 3: "大后天"}


def _weather_days(question):
    """解析用户问的天气时间意图，返回 [(label, date_bj)] 按日期升序。
    没指明时间时默认今天+明天；支持今天/明天/后天/大后天/具体日期/星期几。"""
    q = str(question or "")
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone(timedelta(hours=8)))
    picked = {}

    def add(offset):
        if 0 <= offset <= 7:
            d = now + timedelta(days=offset)
            lab = _DAY_NAMES.get(offset, f"{d.month}月{d.day}日")
            picked[offset] = (lab, d)

    if re.search(r"大后天", q):
        add(3)
    if re.search(r"(?<!大)后天", q):
        add(2)
    if re.search(r"明天|明日", q):
        add(1)
    if re.search(r"今天|今日|今晚|今早|今夜|此刻|现在|当前", q):
        add(0)
    # 具体日期：11月27日 / 11-27 / 11.27
    m = re.search(r"(\d{1,2})\s*[月\-.]\s*(\d{1,2})", q)
    if m:
        try:
            target = now.replace(month=int(m.group(1)), day=int(m.group(2)))
            if target.date() < now.date():
                target = target.replace(year=now.year + 1)
            add((target.date() - now.date()).days)
        except ValueError:
            pass
    # 星期几：找最近的将来那天（今天就是星期X 则算今天）
    m = re.search(r"(?:星期|礼拜|周)([一二三四五六日天])", q)
    if m:
        wd = _WEEKDAY_MAP.get(m.group(1))
        if wd is not None:
            add((wd - now.weekday()) % 7)
    if not picked:
        add(0)
        add(1)
    return [picked[k] for k in sorted(picked)]


def _pick_weather_token(text):
    t = str(text or "")
    best = None
    for token in _WEATHER_TOKENS:
        idx = t.find(token)
        if idx < 0:
            continue
        if best is None or idx < best[0] or (idx == best[0] and len(token) > len(best[1])):
            best = (idx, token)
    return best[1] if best else ""


def _weather_emoji(token):
    return _WEATHER_EMOJI.get(token, "☁️")


def _weather_display(token):
    return _WEATHER_DISPLAY.get(token, token)


def _pick_temps(text):
    t = str(text or "")

    def num_ok(span):
        s, e = span
        tail = t[e:e + 2]
        head = t[max(0, s - 2):s]
        if tail[:1] and tail[0] in "日月年级号%":
            return False
        if head and head[-1] in "日月年第":
            return False
        return True

    m = re.search(
        r"(?:气温|温度)[^。；;\n]{0,40}?"
        r"(-?\d{1,2}(?:\.\d+)?)\s*(?:℃|°C|度)?\s*(?:到|至|~|～|-|—)\s*"
        r"(-?\d{1,2}(?:\.\d+)?)\s*(?:℃|°C|度)?(?!级)", t)
    if not m:
        m = re.search(
            r"(?<!\d)(-?\d{1,2}(?:\.\d+)?)(?!\d)\s*(?:℃|°C|度)?\s*"
            r"(?:到|至|~|～|-|—)\s*"
            r"(?<!\d)(-?\d{1,2}(?:\.\d+)?)(?!\d)\s*(?:℃|°C|度)?(?!级)", t)
    if m and num_ok(m.span(1)) and num_ok(m.span(2)):
        a, b = float(m.group(1)), float(m.group(2))
        if -60 <= a <= 60 and -60 <= b <= 60:
            lo, hi = sorted((int(round(a)), int(round(b))))
            return lo, hi

    nums = []
    for mm in re.finditer(r"(?<!\d)(-?\d{1,2}(?:\.\d+)?)(?!\d)\s*(?:℃|°C|度)?", t):
        if not num_ok(mm.span(1)):
            continue
        v = float(mm.group(1))
        if -60 <= v <= 60:
            nums.append((mm.start(1), int(round(v))))
    for (p1, a), (p2, b) in zip(nums, nums[1:]):
        if 0 < p2 - p1 <= 30 and a <= b:
            return a, b
    return None, None


def _pick_feel(text, low, high):
    m = re.search(r"体感(?:温度)?[^0-9\-]{0,8}(-?\d{1,2})", str(text or ""))
    if m:
        v = int(m.group(1))
        if -60 <= v <= 60:
            return v
    if low is None or high is None:
        return None
    return int(round((low + high) / 2))


def _compact_weather_text(question, content):
    """代码兜底：天气类无论模型多写，都压回用户指定的三行。"""
    raw = str(content or "").strip()
    if not _is_weather_question(question):
        return raw

    loc = _weather_location(question)
    cleaned = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        line = re.sub(r"^[\s\*#>]+", "", line)
        # 只剥列表符和带分隔符的序号（如 "1、"），不能碰 "11月28日" 这种日期开头
        line = re.sub(r"^[\-\u2022·]+\s*", "", line)
        line = re.sub(r"^\d{1,2}[\.、]\s*", "", line)
        line = re.sub(r"\*\*|__|`", "", line)
        line = re.sub(r"^\d+[\.、]\s*", "", line)
        if line:
            cleaned.append(line)

    # 北京时间基准（VM 是 UTC，必须手动 +8）
    from datetime import datetime, timezone, timedelta
    now_bj = datetime.now(timezone(timedelta(hours=8)))
    days = _weather_days(question)
    today_full = f"{now_bj.year}年{now_bj.month}月{now_bj.day}日"

    if not cleaned:
        lines = [f"{loc}{days[0][0]}({days[0][1].year}年{days[0][1].month}月{days[0][1].day}日)天气如下"]
        for lab, d in days:
            lines.append(f"{lab}({d.month}月{d.day}日)未找到可靠数据☁️，气温—°～—°，体感—°")
        lines.append("温馨提醒：暂时没查到可靠数据，稍后再试～")
        return "\n".join(lines)

    # 按时间词给素材分桶：今天/明天/后天/大后天/M月D日
    buckets, cur = {}, None
    for line in cleaned:
        matched = None
        if re.search(r"大后天", line):
            matched = 3
        elif re.search(r"(?<!大)后天", line):
            matched = 2
        elif re.search(r"(明天|明日)", line):
            matched = 1
        elif re.search(r"(今天|今日)", line):
            matched = 0
        else:
            md = re.search(r"(\d{1,2})月(\d{1,2})日", line)
            if md:
                for lab, d in days:
                    if (d.month, d.day) == (int(md.group(1)), int(md.group(2))):
                        matched = (d.date() - now_bj.date()).days
                        break
        if matched is None:
            matched = cur
        cur = matched
        if matched is not None:
            buckets.setdefault(matched, []).append(line)

    all_text = " ".join(cleaned)

    def block_for(offset):
        b = buckets.get(offset)
        if b:
            return " ".join(b)
        return ""

    def build(offset):
        lab, d = picked_map[offset]
        block = block_for(offset)
        # 找不到当天数据时，退而取最近前一天的数据近似，再往前取全天素材
        prev_off = None
        for o in sorted(picked_map):
            if o < offset:
                prev_off = o
        fallback = block_for(prev_off) if prev_off is not None else ""
        use_block = block or fallback or all_text
        token = _pick_weather_token(block) or _pick_weather_token(fallback) or _pick_weather_token(all_text) or "多云"
        low, high = _pick_temps(use_block)
        if low is None or high is None:
            return f"{lab}({d.month}月{d.day}日)未找到可靠数据☁️，气温—°～—°，体感—°", None, None
        feel = _pick_feel(use_block, low, high)
        return (f"{lab}({d.month}月{d.day}日){_weather_display(token)}{_weather_emoji(token)}，"
                f"气温{low}°～{high}°，体感{feel}°"), low, high

    picked_map = {offset: (lab, d) for offset, (lab, d) in
                  [((d.date() - now_bj.date()).days, (lab, d)) for lab, d in days]}
    offsets = sorted(picked_map)

    head_lab, head_d = days[0]
    head_full = f"{head_d.year}年{head_d.month}月{head_d.day}日"
    result = [f"{loc}{head_lab}({head_full})天气如下"]

    day_lines = {}
    for o in offsets:
        line, lo, hi = build(o)
        day_lines[o] = (lo, hi)
        result.append(line)

    # 温馨提醒：按温差/降温/天气动态生成，不写死
    first = offsets[0]
    tips = []
    lo0, hi0 = day_lines[first]
    last_lo = day_lines[offsets[-1]][0]
    if lo0 is not None and hi0 is not None and hi0 - lo0 >= 10:
        tips.append("昼夜温差大，早晚记得添衣")
    if len(offsets) > 1 and last_lo is not None and lo0 is not None and last_lo < lo0 - 3:
        tips.append("后面降温明显，注意保暖")
    if _pick_weather_token(block_for(first) or all_text) in ("大雨", "暴雨", "雪"):
        tips.append("出行注意安全，带好雨具")
    if not tips:
        tips.append("天气变化不大，注意适时增减衣物")
    result.append("温馨提醒：" + "，".join(tips[:2]) + "～")

    return "\n".join(result)


def web_answer(question, extra_system=None, agent=None, timeout=150):
    """用 DeepSeek 服务端联网工具：模型自己搜索、读网页、整理，一次请求返回干净答案。

    返回 dict: {content, tokens_in, tokens_out, tool_steps, annotations}
    """
    from datetime import datetime, timezone, timedelta
    today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    system = WEB_ANSWER_SYSTEM.format(today=today)
    if extra_system:
        system += "\n\n" + str(extra_system)
    if agent:
        agent.set_state("thinking", "联网检索并整理中")

    # 偶发情况：关闭思考后模型可能跳过搜索直接兜底（tool_steps=0）。
    # 这时重试一次并强调必须联网，避免拿到"未找到可靠数据"。
    steps = []
    resp = None
    msg = None
    usage = {}
    for attempt in range(2):
        user_text = question if attempt == 0 else (
            "请务必先联网搜索再回答。" + question)
        body = {
            "model": "deepseek-flash",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_text},
            ],
            "tools": [
                {"type": "deepseek_web_search", "name": "deepseek_web_search"},
            ],
            "thinking": {"type": "disabled"},
            "stream": False,
        }
        resp = _post("/v1/chat/completions", body, timeout=timeout)
        try:
            msg = resp["choices"][0]["message"]
        except Exception:
            raise AIError("响应格式异常: " + json.dumps(resp, ensure_ascii=False)[:300], kind="logic")
        usage = resp.get("usage", {}) or {}
        _record_usage("web_search", "deepseek-flash", usage, agent)
        steps = msg.get("server_tool_calls") or []
        raw_check = (msg.get("content") or "").strip()
        if steps and "未找到可靠数据" not in raw_check:
            break
        if agent:
            agent.log_action("web_retry", f"第{attempt+1}次未实际联网或兜底，重试")
    raw_content = (msg.get("content") or "").strip()
    if agent:
        if msg.get("reasoning_content"):
            agent.log_raw("thinking", msg["reasoning_content"])
        agent.log_raw("web_server_tools",
                      json.dumps(msg.get("server_tool_calls") or [], ensure_ascii=False))
        agent.log_raw("web_answer_content", raw_content)
        if msg.get("annotations"):
            agent.log_raw("web_annotations",
                          json.dumps(msg.get("annotations"), ensure_ascii=False))
    content = _compact_weather_text(question, raw_content)
    if agent and content != raw_content:
        agent.log_action("web_answer_raw", "模型原始返回（压缩前）", raw_content[:300])
    if agent:
        agent.set_state("waiting_api", f"服务端联网完成，{len(steps)} 步工具调用")
        note = "，已按输出模板压缩" if content != raw_content else ""
        agent.log_action("web_answer", f"服务端联网 {len(steps)} 步{note}", content[:200])
    # 从工具调用里提取用过的查询词/网页，供「查看N」追溯
    trace = []
    for s in steps:
        for call in s.get("calls", []) or []:
            action = call.get("action") or {}
            q = action.get("queries") or action.get("url") or ""
            trace.append(f"{call.get('type')} {str(q)[:100]}")
    return {
        "content": content,
        "tokens_in": usage.get("prompt_tokens", 0) or 0,
        "tokens_out": usage.get("completion_tokens", 0) or 0,
        "tool_steps": len(steps),
        "trace": trace,
        "annotations": msg.get("annotations") or [],
    }


# ---------------- Search / Fetch / OCR ----------------

def web_search(query, count=5, ori_question=None):
    """自家多源爬虫搜索（不调用 DeepSeek 服务端联网工具）。"""
    import crawler as _crawler
    import time as _time
    rows = _crawler.search(query, _time.time() + 20)
    out = []
    for r in rows[:max(1, min(50, count))]:
        out.append({
            "title": r.get("title", ""),
            "url": r.get("url", ""),
            "summary": r.get("snippet", ""),
            "site": r.get("source", ""),
            "date": "",
            "score": 0,
        })
    return out


def web_fetch(url):
    """自家网页抓取（不调用 DeepSeek 服务端联网工具）。"""
    import crawler as _crawler
    try:
        page, _ = _crawler._fetch(url, timeout=15)
        text = _crawler._clean_html(page)
    except Exception as e:
        return {"title": "", "content": ""}
    m = re.search(r"<title[^>]*>(.*?)</title>", page, re.S | re.I)
    title = re.sub(r"<[^>]+>", "", html.unescape(m.group(1))).strip() if m else ""
    return {"title": title, "content": text[:12000]}


def _image_data_uri(path):
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    ext = os.path.splitext(path)[1].lower().lstrip(".") or "png"
    if ext == "jpg":
        ext = "jpeg"
    return f"data:image/{ext};base64,{b64}"


def ocr(path, agent=None):
    """OCR：优先 /v1/tools/ocr，失败降级到图片理解。返回文本。"""
    try:
        data_uri = _image_data_uri(path)
        resp = _post("/v1/tools/ocr", {"image": data_uri}, timeout=120)
        text = resp.get("text") or resp.get("content") or ""
        if text:
            return str(text)
    except AIError:
        pass
    return image_understand(path, "请完整识别并转录这张图片里的所有文字，保持原有排版。只输出文字内容。",
                            agent=agent)


def image_understand(path, prompt="请详细描述这张图片的内容。", agent=None):
    data_uri = _image_data_uri(path)
    msg, usage = chat([
        {"role": "system", "content": "你是图像理解助手，用中文准确回答。"},
        {"role": "user", "content": [
            {"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": data_uri}},
        ]},
    ], model=config.get("model_fast"), timeout=120, agent=agent,
        scene="image_understand", thinking=THINK_OFF, allow_local=False)
    content = msg.get("content") or ""
    if agent:
        if msg.get("reasoning_content"):
            agent.log_raw("thinking", msg["reasoning_content"])
        agent.log_raw("image_prompt", prompt)
        agent.log_raw("image_output", content)
    return content


def summarize(text, prompt, model=None, agent=None):
    """轻量整理调用（无工具）。本地模式：Ollama 优先，超时/空输出重试本地一次。

    关键约束（实测）：4核 CPU 上 qwen3:1.7b 对 2600 字素材光 prompt eval 就要 134 秒，
    180 秒超时必挂。所以这里把素材压到 2200 字、本地超时降到 90 秒，
    并且本地模式绝不 fallback 到失效的 DeepSeek 密钥——失败就返回空让上层用素材原文兜底。
    """
    # 本地 CPU 模型实测（E5 老 CPU / qwen3:1.7b）：400字24s、800字22s、1600字49s。
    # 取 900 字上限（约 25-30s），超时 90s 留足余量；在线模型素材放宽到 12000 字。
    limit = 900 if _provider_local() else 12000
    messages = [
        {"role": "system", "content": "你是中文整理助手，输出简洁、结构化、可直接发送的文字。"},
        {"role": "user", "content": prompt + "\n\n---\n" + str(text)[:limit]},
    ]
    mdl = model or config.get("model_fast")
    msg, usage = None, {}
    content = ""
    local_mode = _provider_local()
    # 本地模式只试一次：超时后再试一次也是同样的慢，白等 60 秒；失败直接返回空让上层用素材兜底。
    for attempt in range(1 if local_mode else 1):
        try:
            msg, usage = chat(messages, model=mdl, timeout=90 if local_mode else 90,
                              agent=agent, scene="summarize", thinking=THINK_OFF)
            content = (msg.get("content") or "").strip()
            if content:
                break
        except Exception as e:
            if agent:
                agent.log_action("local_retry", f"本地整理第{attempt+1}次失败：{type(e).__name__}", str(e)[:120])
            msg, usage = None, {}
    # 只有在线模式才允许 DeepSeek 兜底；本地模式密钥已失效，不再徒劳重试浪费 90 秒
    if not content and not local_mode:
        try:
            msg, usage = chat(messages, model=mdl, timeout=90,
                              agent=agent, scene="summarize", thinking=THINK_OFF,
                              allow_local=False)
            content = (msg.get("content") or "").strip()
        except Exception as e:
            if agent:
                agent.log_action("deepseek_unavailable", "DeepSeek 兜底不可用", str(e)[:120])
            msg, usage = None, {}
    if agent:
        if msg and msg.get("reasoning_content"):
            agent.log_raw("thinking", msg["reasoning_content"])
        agent.log_raw("summarize_input", str(text)[:limit])
        agent.log_raw("summarize_output", content)
    return {"content": content, "tokens_in": usage.get("prompt_tokens", 0),
            "tokens_out": usage.get("completion_tokens", 0)}


def classify(text, prompt, model=None, agent=None, thinking=THINK_OFF):
    """意图判定：让模型输出 JSON。默认关闭思考，复杂拆解可传 low。"""
    msg, usage = chat([
        {"role": "system", "content": "只输出合法 JSON，不要任何解释或 markdown 代码块。"},
        {"role": "user", "content": prompt + "\n\n用户输入：" + str(text)[:4000]},
    ], model=model or config.get("model_fast"), timeout=60, agent=agent,
        scene="classify", thinking=thinking)
    raw = (msg.get("content") or "").strip()
    if agent:
        if msg.get("reasoning_content"):
            agent.log_raw("thinking", msg["reasoning_content"])
        agent.log_raw("classify_input", str(text)[:4000])
        agent.log_raw("classify_output", raw)
    if raw.startswith("```"):
        raw = raw.split("```", 2)[1]
        if raw.lower().startswith("json"):
            raw = raw[4:]
    try:
        return json.loads(raw.strip())
    except Exception:
        pass
    # 本地模型 JSON 解析失败 → DeepSeek 重试一次（意图判定必须准）
    if _provider_local():
        try:
            msg, usage = chat([
                {"role": "system", "content": "只输出合法 JSON，不要任何解释或 markdown 代码块。"},
                {"role": "user", "content": prompt + "\n\n用户输入：" + str(text)[:4000]},
            ], model=model or config.get("model_fast"), timeout=60, agent=agent,
                scene="classify", thinking=thinking, allow_local=False)
            raw = (msg.get("content") or "").strip()
            if raw.startswith("```"):
                raw = raw.split("```", 2)[1]
                if raw.lower().startswith("json"):
                    raw = raw[4:]
            return json.loads(raw.strip())
        except Exception:
            pass
    return {"raw": raw}


def check_key():
    """启动时检测密钥是否可用。返回 (ok, 描述)。"""
    try:
        msg, usage = chat([{"role": "user", "content": "回复 OK"}],
                          model=config.get("model_fast"), timeout=25,
                          scene="check_key", thinking=THINK_OFF)
        return True, "密钥有效，模型响应：" + (msg.get("content") or "")[:40]
    except AIError as e:
        return False, str(e)
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def get_balance():
    """查询账户余额。返回 (ok, 文本)。GET /user/balance。"""
    try:
        url = _base() + "/user/balance"
        req = urllib.request.Request(url, headers={"Authorization": "Bearer " + _key()})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8", "replace"))
        lines = []
        available = data.get("is_available")
        lines.append(f"账户可用：{'是' if available else '否'}")
        infos = data.get("balance_infos") or []
        if isinstance(infos, list):
            for b in infos:
                if not isinstance(b, dict):
                    continue
                cur = b.get("currency", "")
                total = b.get("total_balance", "?")
                granted = b.get("granted_balance", "?")
                topped = b.get("topped_up_balance", b.get("purchased_balance", "?"))
                lines.append(f"[{cur}] 总余额 {total} 元（赠金 {granted} / 充值 {topped}）")
        if not infos:
            lines.append("（余额明细为空）")
        return True, "\n".join(lines)
    except AIError as e:
        return False, str(e)
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def usage_text():
    """「查看用量」：全量 token 统计 + 按空闲估价的费用估算。"""
    s = db.usage_summary()
    total = s.get("total") or {}
    today = s.get("today") or {}

    def n(d, k):
        return int(d.get(k) or 0)

    lines = [
        "▎API 用量（按空闲时段价估算，实际以账单为准）",
        f"累计调用：{n(total,'n'):,} 次 · 输入 {n(total,'pin'):,} / 输出 {n(total,'pout'):,} tokens",
        f"其中思考：{n(total,'reason'):,} tokens · 缓存命中 {n(total,'cached'):,}",
    ]
    # 分模型估算费用
    estimate = 0.0
    for m in (s.get("by_model") or []):
        model = m.get("model") or ""
        pin, pout = n(m, "pin"), n(m, "pout")
        cached = 0  # 分组未细分缓存，按未命中保守估算
        estimate += _estimate_cost(model, pin, pout, cached)
        lines.append(f"  · {model}：{n(m,'n')} 次 · 输入{pin:,}/输出{pout:,}")
    lines.append(f"估算费用：约 ¥{estimate:.3f}（高峰时段约为 2 倍）")

    if n(today, "n"):
        lines.append(f"今日：{n(today,'n')} 次 · 输入 {n(today,'pin'):,} / 输出 {n(today,'pout'):,} tokens")

    ok, bal = get_balance()
    lines.append("余额：" + (bal if ok else f"查询失败（{bal}）"))
    return "\n".join(lines)


def api_info_text():
    """API 信息文本：配置 + 密钥状态 + 实时余额。"""
    cfg = config.load()
    lines = [
        "▎API 信息",
        f"地址：{cfg.get('base_url')}",
        f"密钥：{config.mask_key(cfg.get('api_key'))}",
        f"模型：{cfg.get('model_fast')}（主力）/ {cfg.get('model_pro')}（拆解·脚本）",
        f"并发上限：{cfg.get('concurrency')}",
    ]
    if not (cfg.get("api_key") or "").strip():
        lines.append("状态：未设置密钥（发送「设置密钥sk-xxx」配置）")
        return "\n".join(lines)
    ok, bal = get_balance()
    lines.append("余额：" + (bal if ok else f"查询失败（{bal}）"))
    return "\n".join(lines)
