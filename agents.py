#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Agent 执行层：协调器拆解 + 专职 Agent 并行 + 用完即弃 + 实时状态落库 + 错误自愈。"""
import json
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import config
import db
import ai
import bus
import memory
import crawler
import cache

MAX_RETRY = {"syntax": 3, "params": 2, "empty": 1, "network": 2, "auth": 0, "logic": 1}
# 多轮联网(75s) + 本地整理(2×60s) 可能接近 200s，等待上限必须留够，
# 否则 fut.result 超时→抛 TimeoutError→Synthesizer 误判"全部失败"，而后台 Agent 其实还在跑。
AGENT_TIMEOUT = 300


class Agent:
    """一次性 Agent 实例。每步动作 + 实时状态都写库。"""

    def __init__(self, task_id, seq, name, role):
        self.task_id = task_id
        self.seq = seq
        self.name = name
        self.role = role
        self.id = db.add_agent_run(task_id, seq, name, role)
        self._seq = 0
        self._raw_seq = 0
        self.tokens_in = 0
        self.tokens_out = 0
        self.heal_count = 0
        self.cancel_check = lambda: db.is_cancelled(task_id)

    def set_state(self, state, action=""):
        db.set_agent_state(self.id, state, action)
        self._seq += 1
        db.add_agent_action(self.id, self._seq, "state", state, action[:300])

    def log_action(self, action_type, detail, summary=""):
        self._seq += 1
        db.add_agent_action(self.id, self._seq, action_type, detail, summary)

    def log_raw(self, kind, content):
        """原始全量记录：推理、工具入参/出参、网页正文等，不截断。"""
        self._raw_seq += 1
        db.add_agent_raw(self.id, self._raw_seq, kind, content)

    def check_cancel(self):
        if self.cancel_check():
            raise Cancelled()

    def finish(self, state, result="", error=""):
        db.finish_agent(self.id, state, result, error,
                        self.tokens_in, self.tokens_out, self.heal_count)

    def run(self):
        raise NotImplementedError


class Cancelled(Exception):
    pass


class AgentFailure(Exception):
    def __init__(self, message, kind="logic"):
        super().__init__(message)
        self.kind = kind


# ---------------- 工具执行器（搜索 Agent 用） ----------------

def default_tool_executor(agent):
    def execute(name, args):
        agent.check_cancel()
        if name == "web_search":
            q = args.get("query", "")
            agent.set_state("tool_calling", f"web_search「{q}」")
            results = ai.web_search(q, args.get("count", 5), args.get("ori_question"))
            if not results:
                raise AgentFailure("搜索结果为空", kind="empty")
            agent.tokens_in += 1  # 计一次工具调用，非 token，仅统计用
            lines = []
            for r in results:
                lines.append(f"- {r['title']} | {r['site']} | {r['date']}\n  {r['summary'][:400]}\n  链接: {r['url']}")
            return "\n".join(lines)
        if name == "web_fetch":
            url = args.get("url", "")
            agent.set_state("tool_calling", f"web_fetch {url[:80]}")
            page = ai.web_fetch(url)
            if not page.get("content"):
                raise AgentFailure("网页正文为空", kind="empty")
            return "# " + page.get("title", "") + "\n" + page["content"][:12000]
        return f"未知工具: {name}"
    return execute


# ---------------- 协调器 ----------------

COORDINATOR_SYSTEM = """你是任务拆解协调器。把用户的任务拆成若干可并行的子任务，输出严格 JSON。
规则：
- 每个子任务是一个独立、单一目标的任务，尽量并行（无依赖）
- type 只能是：search（需联网查资料/天气/新闻）、document（处理用户发的图片/文件）、general（直接问答/计算/写作）、memory（只需记忆或简单问答）
- name 是给人类看的简短名称（8字以内）
- 子任务必须直接服务于用户的原始问题，严禁自行添加用户没有提到、没有要求的查询（例如用户只问天气，就绝不能拆出查询价格/票价/机票之类的子任务）
- 若任务本身简单，只拆 1 个即可；拿不准就只拆 1 个
- 最多拆 6 个子任务
输出格式：
{"subtasks":[{"name":"查询天气","type":"search","goal":"查询宁夏今天的天气情况，给出温度和天气状况"},...]}"""


# 小模型拆解幻觉过滤：用户没提的领域词擅自出现在子任务里 → 判为幻觉丢弃
_HALLUCIN_DOMAIN = ("价格", "票价", "机票", "火车", "高铁", "门票", "酒店", "住宿",
                    "汇率", "股价", "股票", "基金", "彩票", "外卖", "打车")


def _filter_hallucinated(subs, user_text, attachments, agent=None):
    """过滤拆解幻觉：子任务引入用户问题里没有的领域/动作即丢弃。"""
    t = str(user_text or "")
    has_attach = bool(attachments)
    kept, dropped = [], []
    for s in subs:
        blob = (s.get("name", "") + s.get("goal", ""))
        # 没有附件却要求处理文件 → 幻觉
        if s.get("type") == "document" and not has_attach:
            dropped.append(s.get("name", "?"))
            continue
        # 用户全文没提价格/票务等领域词，子任务却出现 → 幻觉
        if not any(w in t for w in _HALLUCIN_DOMAIN) and any(w in blob for w in _HALLUCIN_DOMAIN):
            dropped.append(s.get("name", "?"))
            continue
        kept.append(s)
    if dropped and not kept:
        # 全被过滤则回退为直接执行用户原话，绝不执行幻觉任务
        kept = [{"name": "执行任务", "type": "search" if _needs_search(t) else "general",
                 "goal": t}]
    if dropped and agent:
        agent.log_action("filter", f"过滤幻觉子任务：{'、'.join(dropped)}", t[:80])
    return kept


def _is_complex_task(text, attachments):
    """简单任务用 flash 低思考；只有明显多段/多目标/长文本才升级 pro 高思考。"""
    t = str(text or "")
    signals = ("分别", "同时", "另外", "再", "然后", "对比", "比较", "以及", "并且",
               "机票", "高铁", "火车", "票价", "价格", "多少", "总结", "分析", "报告")
    multi = sum(1 for s in signals if s in t)
    return len(t) > 160 or multi >= 2 or len(attachments or []) >= 2


class CoordinatorAgent(Agent):
    def __init__(self, task_id, seq, user_text, attachments, history=None, memory_ctx=""):
        super().__init__(task_id, seq, "协调器", "coordinator")
        self.user_text = user_text
        self.attachments = attachments or []
        self.history = history or []
        self.memory_ctx = memory_ctx or ""

    def run(self):
        self.check_cancel()
        self.set_state("thinking", "规则拆解，创建 Agent 列表")
        # 关键性能修复：创建列表绝不调用模型。
        # 之前本地 qwen3:1.7b 生成 JSON 要 40~60 秒，失败还会再等 DeepSeek 60 秒，
        # 用户看到「排队→创建agent列表」卡一分钟就是这里。规则拆解是纯字符串操作，毫秒级。
        subs = fast_plan(self.user_text, self.attachments)
        self.log_action("routing", "规则拆解（零 token，毫秒级）", self.user_text[:120])
        self.log_action("result", f"创建 {len(subs)} 个 Agent", json.dumps(subs, ensure_ascii=False)[:300])
        self.finish("done", json.dumps(subs, ensure_ascii=False))
        return subs


def _needs_search(text):
    kw = ["天气", "价格", "票价", "多少钱", "机票", "高铁", "火车", "新闻", "今天", "最新",
          "最近", "近期", "近日", "股价", "股票", "查", "搜", "赛事", "比分", "成绩",
          "录取", "分数线", "通知", "公告", "政策", "是谁", "什么时候", "哪里", "放假",
          "快递", "疫情", "地震", "灾害", "事故", "通报", "情况", "信息", "进展", "消息",
          "开奖", "彩票", "油价", "汇率", "航班", "景区", "门票"]
    return any(k in str(text or "") for k in kw)


# 普通聊天（不建 Agent）：寒暄、追问、常识问答
_CHAT_GREETINGS = ("你好", "您好", "hi", "hello", "在吗", "在么", "谢谢", "感谢", "好的",
                   "收到", "嗯", "哦", "ok", "是的", "明白", "懂了", "再见", "拜拜",
                   "你是谁", "你叫什么", "你能干什么")
# 带这些指代/追问词的短句，若有最近对话上下文 → 直接答（优先于搜索词，
# 否则「你这查询的是几点的?」会因含「查」被误判成搜索任务）
_FOLLOWUP_HINTS = ("你这", "你刚", "刚才", "刚刚", "上面", "前面", "查询结果", "查的",
                   "几点的", "几点", "什么时间", "哪个时间", "这个", "那个", "什么意思",
                   "为啥", "为什么", "确定吗", "准吗", "还有呢", "然后呢", "是吗")
# 明确需要 Agent 的加工/创作类动词
_AGENT_WRITE_WORDS = ("写", "生成", "创作", "起草", "改写", "润色", "翻译", "总结",
                      "缩写", "扩写", "文案", "邮件", "作文", "代码", "写篇", "写个",
                      "写一份", "编写")

# 空泛的「查」指令：没有任何具体对象，不能拿去搜索（模型会把「查」理解成查案而编故事）
_VAGUE_QUERY_RE = re.compile(
    r"^(我要你|我让你|帮我|请|麻烦|你去|你帮我|给我)?\s*(去|帮忙)?\s*"
    r"(查一查|查一下|查询一下|查查|查询|查|搜一搜|搜一下|搜索一下|搜搜|搜索|搜)"
    r"\s*[，,。！？!?~～]*$")

# 话题词：判断当前提问是否和刚才聊的是同一件事（同类追问直接答，不重建 Agent）
# 注意去掉「高铁/机票/车票」等过细的词——「高铁票呢」接上一句「机票」应算同一票务话题，
# 否则 shares_topic 判不出交集，省略句被当无主语新任务乱搜。
_TOPIC_WORDS = ("票", "航班", "飞机", "机场", "天气", "气温", "温度", "新闻", "地震",
                "油价", "汇率", "股票", "股价", "价格", "分数线", "录取", "快递",
                "门票", "景区", "开奖", "彩票")

# 本地小模型把「查」当「查案」时会编出故事，这些是典型叙事标记（命中 2 个即判跑题）
_OFF_TOPIC_MARKERS = ("被派去", "藏有", "的刀", "说罢", "只见", "凶手", "案发", "此人",
                      "这名", "于是他", "便命人", "手下", "江湖", "镖", "银两")


def is_vague_query(text):
    """只有「查/搜」没有具体对象 → 应反问澄清，不能建 Agent 去搜。"""
    t = re.sub(r"[\s，,。！？!?~～]+", "", str(text or ""))
    return bool(_VAGUE_QUERY_RE.match(t))


def _topic_set(text):
    s = str(text or "")
    return set(w for w in _TOPIC_WORDS if w in s)


def shares_topic(text, history):
    """当前提问与最近对话是否是同一话题（用于把「查一下价格和时间」当追问）。"""
    cur = _topic_set(text)
    if not cur or not history:
        return False
    prior = set()
    for h in history[-2:]:
        prior |= _topic_set(str(h.get("content") or ""))
    return bool(cur & prior)


def looks_off_topic(question, content):
    """模型输出跑题/编故事检测。True = 不应发给用户。"""
    text = str(content or "").strip()
    if not text:
        return True
    if sum(1 for m in _OFF_TOPIC_MARKERS if m in text) >= 2:
        return True
    # 输出与问题一个共同字都没有 → 基本可判定跑题
    q = set(re.sub(r"[^\u4e00-\u9fa5a-zA-Z0-9]", "", str(question or "")))
    a = set(re.sub(r"[^\u4e00-\u9fa5a-zA-Z0-9]", "", text))
    if len(q) >= 4 and len(a) >= 10 and not (q & a):
        return True
    return False


def _strip_query_verbs(text):
    """剥掉「我要你去查询一下」这类动词壳，留下真正的查询对象。"""
    s = str(text or "").strip()
    s = re.sub(r"^(我要你|我让你|帮我|请|麻烦|你去|你帮我|给我)?\s*(去|帮忙)?\s*"
               r"(查一查|查一下|查询一下|查查|查询|查|搜一搜|搜一下|搜索一下|搜搜|搜索|搜)"
               r"\s*[，,]?", "", s)
    return s.strip(" ，,。")


def _prev_concrete_query(history):
    """从最近对话里找上一句『有具体对象』的用户提问，作为上下文锚点。"""
    if not history:
        return ""
    for h in reversed(history):
        if h.get("role") != "user":
            continue
        c = str(h.get("content") or "").strip()
        if c and len(c) >= 6 and not is_vague_query(c):
            return c
    return ""


def resolve_query(text, history):
    """上下文串联：把省略句/空泛指令补成完整查询目标。
    - 「我要你去查」 → 复用上一句的具体问题
    - 「查询一下机票价格和时间」 → 补上上一句的航线/日期等具体对象
    - 其余原样返回
    """
    t = str(text or "").strip()
    prev = _prev_concrete_query(history)
    if not prev:
        return t
    if is_vague_query(t):
        return prev
    prev_subject = _strip_query_verbs(prev)
    cur_subject = _strip_query_verbs(t)
    if (shares_topic(t, history) and cur_subject
            and len(cur_subject) < len(prev_subject)):
        return f"{prev_subject}，{cur_subject}"
    return t


def needs_agent(text, attachments=None, history=None):
    """判断是否需要创建 Agent。
    True  = 查询/搜索/写作/多任务/附件 → 建 Agent 并播报列表（goal 需先经 resolve_query 串联上下文）
    False = 普通聊天/追问/寒暄/常识问答 → 带上下文直接回答，不建 Agent
    """
    t = re.sub(r"\s+", " ", str(text or "")).strip()
    if attachments:
        return True
    # 1) 有上下文的短追问优先（「你这查询的是几点的?」含「查」但不能当搜索）
    if history and len(t) <= 40 and any(k in t for k in _FOLLOWUP_HINTS):
        return False
    # 2) 寒暄 / 极短应答
    tl = t.lower()
    if len(t) <= 12 and any(g in tl for g in _CHAT_GREETINGS):
        return False
    # 3) 空泛的「我要你去查」：有上文则交给 Agent 带上下文重查，无上文则反问澄清
    if is_vague_query(t):
        return bool(_prev_concrete_query(history))
    # 4) 明确写作 / 生成 / 加工
    if any(w in t for w in _AGENT_WRITE_WORDS):
        return True
    # 5) 搜索 / 查询 / 时效信息
    if _needs_search(t):
        return True
    # 6) 明确多任务 / 复杂指令
    if re.search(r"[；;]\s*|\n|另外|同时|分别", t) or _is_complex_task(t, attachments):
        return True
    # 7) 其余（常识问答、观点、闲聊）直接答
    return False


def fast_plan(user_text, attachments=None):
    """规则化任务拆解：零 token、零延迟、不产生幻觉子任务。
    只在用户明确分句（分号/换行/另外/同时/分别）时才拆多个，拿不准一律单任务。
    用户原话永远完整保留为 goal，绝不引入用户没提的领域词（如"查询价格"）。"""
    t = re.sub(r"\s+", " ", str(user_text or "")).strip()
    attachments = attachments or []
    subs = []

    # 附件：一个附件一个 document 子任务
    for a in attachments:
        name = str(a.get("orig_name") or "附件")[:8]
        subs.append({"name": f"处理{name}"[:20], "type": "document",
                     "goal": f"结合用户问题处理附件《{a.get('orig_name') or '附件'}》：{t}"[:2000]})

    # 明确的多段指令才拆：分号、换行、另外/同时/分别/第一第二
    parts = [p.strip(" ，,、。.;；") for p in re.split(r"[；;]\s*|\n+|另外[，,]?|同时[，,]?|分别[，,]?", t)
             if len(p.strip()) >= 4]
    if len(parts) > 1 and not attachments:
        for p in parts[:6]:
            subs.append({"name": p[:8], "type": "search" if _needs_search(p) else "general",
                         "goal": p[:2000]})
    else:
        subs.append({"name": "执行任务", "type": "search" if _needs_search(t) else "general",
                     "goal": t[:2000]})
    return subs[:6]


# ---------------- 搜索 Agent ----------------

SEARCH_SYSTEM = """你是信息检索助手。最终答案会被直接发到 QQ。
- 天气严格只输出三行：地点天气如下 / 今天<天气><emoji>，气温X度到X度，体感X度 / 明天<天气><emoji>，气温X度到X度，体感X度
- 其他查询只输出 3-5 行，不要来源链接、查询时间、爬取过程、解释差异。
- 票务最后一行写"仅供参考，非实时价格"。
- 无法确认就写"未找到可靠数据"，不要编造，不要输出多余文字。"""


class SearchAgent(Agent):
    def __init__(self, task_id, seq, name, goal, memory_ctx=""):
        super().__init__(task_id, seq, name, "search")
        self.goal = goal
        self.memory_ctx = memory_ctx

    def run(self):
        self.check_cancel()
        extra = ""
        if self.memory_ctx:
            extra = "相关记忆（仅作参考，不要写进回答）：\n" + self.memory_ctx[:2000]

        # 第零步：查缓存。命中且未过期→直接返回（零 token）
        try:
            cached, cached_cat = cache.get(self.goal)
            if cached:
                self.log_action("cache_hit", f"缓存命中（{cached_cat}），零 token", cached[:200])
                result = cache.mark(cached, cached_cat)
                self.finish("done", result)
                return result
        except Exception as e:
            self.log_action("error", f"缓存查询异常，忽略：{e}", "")

        # 第一步：坚持不懈的多轮联网研究（不依赖 DeepSeek，搜不到就换词继续）
        self.set_state("tool_calling", "启动多轮联网检索")
        self.log_action("research_start", f"开始联网检索：{self.goal[:80]}", "")
        try:
            result = crawler.research(self.goal, timeout=75, agent=self)
        except Cancelled:
            self.finish("cancelled", "", "用户取消")
            raise
        except Exception as e:
            self.log_action("error", f"联网研究异常：{type(e).__name__}: {e}", "")
            result = {"ok": False, "reason": str(e)[:120], "intent": "general", "sources": []}

        if not result.get("ok") or not result.get("material"):
            reason = result.get("reason", "未知原因")
            self.log_action("error", f"多轮联网均未拿到素材：{reason}", "")
            self.finish("done", "（此项未查到可靠数据：多轮联网搜索均未拿到有效素材）")
            return "（此项未查到可靠数据：多轮联网搜索均未拿到有效素材）"

        intent = result.get("intent", "general")
        for s in result.get("sources", [])[:8]:
            self.log_action("researched_source",
                            f"{s.get('source', '')} {str(s.get('title', ''))[:40]}",
                            str(s.get("url", ""))[:120])
        material = result["material"]
        if result.get("weak"):
            self.log_action("weak_material",
                            f"素材质量偏弱（{result.get('score', '?')}分），仍整理输出供参考",
                            material[:200])

        content = self._organize_crawled(self.goal, material, intent, extra)
        if not content:
            # 模型全挂时也把抓到的原文给用户，绝不返回空
            self.log_action("error", "模型整理失败，使用联网素材原文兜底", "")
            content = "（联网检索到的原始资料，未经模型整理：）\n" + str(material)[:900]
        self.log_action("result", f"联网检索完成（{result.get('rounds', '?')}步留痕，强度{result.get('score', '?')}）",
                        content[:300])
        return self._finalize(self.goal, content, intent)

    def _self_crawl_answer(self, extra=""):
        """自家联网加强兜底：deep_crawl 多源抓素材 → 当前模型整理。零联网 API 费用。"""
        try:
            result = crawler.deep_crawl(self.goal, timeout=40, agent=self)
        except Cancelled:
            self.finish("cancelled", "", "用户取消")
            raise
        except Exception as e:
            self.log_action("error", f"加强联网异常：{type(e).__name__}: {e}", "")
            result = {"ok": False, "reason": str(e)[:120]}
        if not result.get("ok") or not result.get("material"):
            reason = result.get("reason", "未知原因")
            self.log_action("error", f"加强联网失败：{reason}", "")
            self.finish("done", "（此项未查到可靠数据：所有联网搜索源均未拿到有效素材）")
            return "（此项未查到可靠数据：所有联网搜索源均未拿到有效素材）"
        intent = result.get("intent", "general")
        for s in result.get("sources", [])[:8]:
            self.log_action("deep_source", f"{s.get('source', '')} {s.get('title', '')[:40]}", s.get("url", "")[:120])
        content = self._organize_crawled(self.goal, result["material"], intent, extra)
        if not content:
            self.finish("done", "（此项未查到可靠数据：抓取到的素材无法整理出有效答案）")
            return "（此项未查到可靠数据：抓取到的素材无法整理出有效答案）"
        self.log_action("result", "自家加强联网完成（未走 DeepSeek 联网）", content[:300])
        self.finish("done", content)
        return content

    def _finalize(self, question, content, intent):
        """写缓存并落库最终结果（缓存里存干净答案，命中时才加标注）。"""
        try:
            cache.put(question, content, intent)
            self.log_action("cache_write", f"写入缓存（{cache.classify_ttl(question, intent)[0]}）", "")
        except Exception as e:
            self.log_action("error", f"缓存写入失败：{e}", "")
        self.finish("done", content)
        return content

    def _organize_crawled(self, question, material, intent, extra=""):
        """把爬虫抓来的素材交给模型整理成最终格式（关闭思考，走 summarize）。"""
        self.set_state("thinking", "整理爬虫素材")
        if intent == "weather":
            days = ai._weather_days(question)
            _d0 = days[0][1]
            _full = f"{_d0.year}年{_d0.month}月{_d0.day}日"
            _day_desc = "、".join(f"{lab}({_d.month}月{_d.day}日)" for lab, _d in days)
            prompt = (
                "用户问题：" + question + "\n\n"
                "下面是刚抓取的天气网页实时数据。请严格按格式输出，只针对用户问的地点，"
                f"输出用户问的那些天：{_day_desc}：\n"
                f"{{地点}}{days[0][0]}({_full})天气如下\n"
                "每行一天：<时间词>(M月D日)<天气><emoji>，气温X°～X°，体感X°\n"
                "温馨提醒：<一句简短提醒>～\n"
                "硬性要求：地点用用户问的（可含区，如银川市西夏区）；"
                "用户问哪天输出哪天（今天/明天/后天/大后天/具体日期/星期几），没指明默认今天+明天；"
                "从素材里找对应日期带℃或度的真实温度换算成整数，不要编造，"
                "素材没有该日期就写「<时间词>(M月D日)未找到可靠数据☁️，气温—°～—°，体感—°」；"
                "温度用「°」符号、范围用「～」连接（如 0°～16°），禁止写「度」「℃」「到」；"
                "温馨提醒按温差/降温/降雨写一句（如「昼夜温差大，早晚记得添衣」「明天降温明显，注意保暖」）；"
                "不要输出来源、解释、多余段落；体感没有就用当天低温近似，不要写「未知」；"
                "除标题和数据行外只加一行温馨提醒，严禁多余行。"
            )
        else:
            prompt = (
                "用户问题：" + question + "\n\n"
                "下面是刚抓取的相关网页内容。请整理成适合 QQ 阅读的简洁回答："
                "最多 5 行，只保留和问题直接相关的关键数据与结论；"
                "不要来源链接、抓取时间、过程说明、差异说明；票务注明仅供参考非实时价格；"
                "数据不确定就写未找到可靠数据，不要编造。"
            )
        if extra:
            prompt += "\n\n" + extra
        # 本地 CPU 模型实测：900字约25-30秒稳过，1600字要49秒逼近超时。压到900字。
        self.log_raw("crawler_organize_input", material[:900])
        out = ai.summarize(material[:900], prompt, model=config.get("model_fast"), agent=self)
        self.tokens_in += out["tokens_in"]
        self.tokens_out += out["tokens_out"]
        content = (out["content"] or "").strip()
        if intent == "weather":
            content = ai._compact_weather_text(question, content)
        if not content:
            # 两个模型都不可用时的最后兜底：直接返回清洗后的素材摘要（零模型依赖）
            self.log_action("error", "模型整理均失败，使用素材原文兜底", "")
            raw = str(material)[:600]
            return "（联网检索到的原始资料，未经模型整理：）\n" + raw
        return content

    def _deepseek_answer(self, extra=""):
        """DeepSeek 服务端联网兜底（原有链路，含自愈重试）。"""
        last_error = None
        for attempt in range(4):
            self.check_cancel()
            try:
                out = ai.web_answer(self.goal, extra_system=extra, agent=self, timeout=150)
                self.tokens_in += out["tokens_in"]
                self.tokens_out += out["tokens_out"]
                content = (out["content"] or "").strip()
                if not content:
                    raise AgentFailure("模型返回为空", kind="empty")
                for t in out.get("trace", [])[:12]:
                    self.log_action("web_tool", t)
                self.log_action("result", f"兜底检索完成（{out['tool_steps']} 步联网）", content[:300])
                self.finish("done", content)
                return content
            except Cancelled:
                self.finish("cancelled", "", "用户取消")
                raise
            except ai.AIError as e:
                last_error = e
                kind = e.kind
                self.log_action("error", f"兜底第{attempt+1}次失败：{e}", "")
                if kind == "auth":
                    break
                if not _should_retry(kind, attempt, "network" if kind == "network" else "params"):
                    break
                self._heal(kind, attempt)
            except AgentFailure as e:
                last_error = e
                self.log_action("error", f"兜底第{attempt+1}次失败：{e}", "")
                if not _should_retry(e.kind, attempt, e.kind):
                    break
                self._heal(e.kind, attempt)
            except Exception as e:
                last_error = e
                self.log_action("error", f"兜底第{attempt+1}次异常：{type(e).__name__}: {e}", "")
                if not _should_retry("logic", attempt, "logic"):
                    break
                self._heal("logic", attempt)

        err = str(last_error) if last_error else "未知错误"
        self.finish("failed", "", err)
        return f"（此项未查到：{err[:100]}）"

    def _heal(self, kind, attempt):
        self.heal_count += 1
        self.set_state("self_healing", f"自愈重试 {attempt+2}/{MAX_RETRY.get(kind, 1)+1}")
        self.log_action("self_heal", f"检测到{kind}错误，准备第{attempt+2}次尝试")
        time.sleep(min(2 * (attempt + 1), 6))


def _should_retry(kind, attempt, retry_key):
    limit = MAX_RETRY.get(retry_key, 1)
    return attempt < limit


# ---------------- 文档 Agent ----------------

class DocumentAgent(Agent):
    def __init__(self, task_id, seq, name, path, kind, orig_name, goal=""):
        super().__init__(task_id, seq, name, "document")
        self.path = path
        self.kind = kind
        self.orig_name = orig_name
        self.goal = goal

    def run(self):
        self.check_cancel()
        self.set_state("tool_calling", f"解析文档《{self.orig_name}》")
        self.log_action("tool_call", f"开始解析 {self.kind}：{self.orig_name}")
        try:
            if self.kind in ("image", "video"):
                prompt = self.goal or "请详细描述这张图片的内容，如果是截图请提取关键文字信息。"
                text = ai.image_understand(self.path, prompt, agent=self)
            else:
                text = ai.ocr(self.path, agent=self)
                if self.goal:
                    summary = ai.summarize(text, "用户要求：" + self.goal + "\n请基于文档内容回答。", agent=self)
                    text = summary["content"]
                    self.tokens_in += summary["tokens_in"]
                    self.tokens_out += summary["tokens_out"]
            self.log_raw("document_text", text)
            self.log_action("result", f"《{self.orig_name}》解析完成", text[:300])
            self.finish("done", f"【{self.orig_name}】\n{text}")
            return f"【{self.orig_name}】\n{text}"
        except Cancelled:
            self.finish("cancelled", "", "用户取消")
            raise
        except Exception as e:
            self.log_action("error", f"文档解析失败：{e}", "")
            err = str(e)
            if isinstance(e, ai.AIError) and e.kind in ("network", "params"):
                self.heal_count += 1
                self.set_state("self_healing", "文档解析重试 2/2")
                time.sleep(2)
                try:
                    text = ai.ocr(self.path, agent=self) if self.kind != "image" else ai.image_understand(self.path, self.goal or "描述图片", agent=self)
                    self.log_raw("document_text", text)
                    self.finish("done", f"【{self.orig_name}】\n{text}")
                    return f"【{self.orig_name}】\n{text}"
                except Exception as e2:
                    err = str(e2)
            self.finish("failed", "", err)
            return f"（文档《{self.orig_name}》处理失败：{err[:100]}）"


# ---------------- 通用 Agent（问答/整理） ----------------

class GeneralAgent(Agent):
    def __init__(self, task_id, seq, name, goal, model=None, system="", memory_ctx=""):
        super().__init__(task_id, seq, name, "general")
        self.goal = goal
        self.model = model or config.get("model_fast")
        self.system = system or "你是 helpful 的中文 AI 助手，回答准确、简洁、结构化。"
        self.memory_ctx = memory_ctx

    def run(self):
        self.check_cancel()
        self.set_state("thinking", "生成回答")
        try:
            sys = self.system
            if self.memory_ctx:
                sys += "\n\n相关记忆：\n" + self.memory_ctx[:1200]
            messages = [
                {"role": "system", "content": sys},
                {"role": "user", "content": self.goal[:1500]},
            ]
            # 本地模式：一律走 Ollama 直接问答。旧代码走 chat_tool_loop()（tools 强制 DeepSeek），
            # 密钥失效后所有 general 任务（如"宁夏最近地震信息"）直接 401——这是上一轮漏改的路径。
            if ai._provider_local():
                msg, usage = ai.ollama_chat(messages, timeout=90, agent=self,
                                            scene="general", thinking=ai.THINK_OFF)
                content = (msg.get("content") or "").strip()
                self.tokens_in += usage.get("prompt_tokens", 0)
                self.tokens_out += usage.get("completion_tokens", 0)
                if not content:
                    # 本地也空：不碰 DeepSeek，素材/问题原文兜底
                    self.log_action("error", "本地模型返回为空，使用问题原文兜底", "")
                    content = f"（本地模型未能生成回答，原始问题：{self.goal[:300]}）"
            else:
                out = ai.chat_tool_loop(sys, self.goal, default_tool_executor(self),
                                        model=self.model, max_rounds=4, timeout=90, agent=self)
                self.tokens_in += out["tokens_in"]
                self.tokens_out += out["tokens_out"]
                content = (out["content"] or "").strip()
                if not content:
                    raise AgentFailure("回答为空", kind="empty")
            self.log_action("result", "生成完成", content[:300])
            self.finish("done", content)
            return content
        except Cancelled:
            self.finish("cancelled", "", "用户取消")
            raise
        except Exception as e:
            self.log_action("error", f"失败：{e}", "")
            # 失败也不把 401/Timeout 原文暴露给用户，给可读提示
            err = str(e)
            if "401" in err or "Authentication" in err:
                err = "在线接口不可用（密钥失效），请切换本地模型"
            self.finish("done", f"（处理失败：{err[:120]}）")
            return f"（处理失败：{err[:120]}）"


# ---------------- 整理 Agent ----------------

class SynthesizerAgent(Agent):
    def __init__(self, task_id, seq, user_text, results):
        super().__init__(task_id, seq, "整理", "synthesizer")
        self.user_text = user_text
        self.results = results

    def run(self):
        self.check_cancel()
        self.set_state("thinking", "汇总整理最终结果")
        ok = [(r.get("name"), r.get("result")) for r in self.results
              if r.get("result") and not r.get("result").startswith("（处理失败")
              and not r.get("result").startswith("（此项未查到")]
        fail = [(r.get("name"), r.get("error") or (r.get("result") or "")[:80])
                for r in self.results if r not in [x for x in self.results if (x.get("name"), x.get("result")) in ok]]

        # 关键修复：不能再只看 ok 标志。之前 search agent 明明拿到结果并 finish("done")，
        # 只因并发池返回的 state 尚未落库就被判 failed，整条任务被"全部失败"吞掉。
        # 现在只要有任何实质结果就返回，不因个别失败/落库时序把成功结果丢了。
        if not ok:
            text = "这次没有拿到有效结果。\n" + "\n".join(f"- {n}：{e or '未知原因'}" for n, e in fail)
            self.log_action("result", "全部失败", text[:200])
            self.finish("done", text)
            return text

        if len(ok) == 1:
            content = ok[0][1]
            self.log_action("result", "单结果直接返回", content[:200])
            self.finish("done", content)
            return content

        summary_parts = [f"【{n}】\n{r}" for n, r in ok]
        if fail:
            summary_parts.append("【未完成项】\n" + "\n".join(f"- {n}：{e or '未知原因'}" for n, e in fail))
        joined = "\n\n".join(summary_parts)
        self.log_raw("synthesize_input", joined)

        try:
            out = ai.summarize(joined,
                "用户原始需求：" + self.user_text + "\n\n请把下面的多份结果整理成一条适合 QQ 阅读的简洁回复。"
                "最多 6 行，按任务分行；只保留关键数据，不要来源链接、查询时间、解释说明、差异说明。"
                "天气类必须严格三行模板；未完成项压缩成一行说明。",
                model=config.get("model_fast"), agent=self)
            self.tokens_in += out["tokens_in"]
            self.tokens_out += out["tokens_out"]
            text = out["content"] or joined
        except Exception as e:
            self.log_action("error", f"整理调用失败，使用原始拼接：{e}", "")
            text = joined
        # 跑题拦截：模型整理时编出无关叙事 → 用原始结果拼接，不用编造内容
        if text and text != joined and looks_off_topic(self.user_text, text):
            self.log_action("off_topic_blocked", "整理输出跑题，改用原始结果", text[:200])
            text = joined

        # 不再追加查询时间尾注：用户示例里没有，避免最终回复变长
        self.log_raw("synthesize_output", text)
        self.log_action("result", "整理完成", text[:300])
        self.finish("done", text)
        return text


# ---------------- 并行执行一组子任务 ----------------

def run_subtasks(task_id, subtasks, attachments, base_seq=2, cancel_check=None, memory_ctx=""):
    """并行执行子任务，返回 [{name,type,ok,result,error,agent_id}]。
    memory_ctx 只传给 general 子任务（需要背景知识的），search 任务不注入（时效类不花钱塞上下文）。"""
    results = [None] * len(subtasks)
    workers = []
    concurrency = max(1, min(8, int(config.get("concurrency", 4))))
    executor = ThreadPoolExecutor(max_workers=concurrency)
    try:
        # 文档子任务
        doc_subs = []
        for i, s in enumerate(subtasks):
            if s.get("type") == "document":
                doc_subs.append((i, s))
        used_attach = set()

        for i, s in enumerate(subtasks):
            typ = s.get("type")
            name = s.get("name", f"子任务{i+1}")
            goal = s.get("goal", "")
            if typ == "document":
                if not attachments:
                    agent = GeneralAgent(task_id, base_seq + i, name, goal + "\n（用户未成功发送附件）",
                                         memory_ctx=memory_ctx)
                    workers.append((i, agent, executor.submit(_safe_run, agent, cancel_check)))
                    continue
                for a in attachments:
                    if a.get("id") in used_attach:
                        continue
                    used_attach.add(a.get("id"))
                    agent = DocumentAgent(task_id, base_seq + i, f"{name}·{a.get('orig_name','')[:8]}",
                                          a.get("path", ""), a.get("kind", "file"),
                                          a.get("orig_name", "附件"), goal)
                    workers.append((i, agent, executor.submit(_safe_run, agent, cancel_check)))
                continue
            elif typ == "search":
                agent = SearchAgent(task_id, base_seq + i, name, goal)
            else:
                agent = GeneralAgent(task_id, base_seq + i, name, goal, memory_ctx=memory_ctx)

            workers.append((i, agent, executor.submit(_safe_run, agent, cancel_check)))

        for i, agent, fut in workers:
            try:
                text = fut.result(timeout=AGENT_TIMEOUT + 30)
                row = db.query_one("SELECT state,error,result FROM agents_runs WHERE id=?", (agent.id,))
                ok = bool(row and row.get("state") == "done")
                item = {"name": agent.name, "type": agent.role, "ok": ok,
                        "result": text or "", "error": (row or {}).get("error", ""),
                        "agent_id": agent.id}
            except Exception as e:
                item = {"name": agent.name, "type": agent.role, "ok": False,
                        "result": "", "error": f"{type(e).__name__}: {e}", "agent_id": agent.id}
            if results[i] is None:
                results[i] = [item]
            else:
                results[i].append(item)
    finally:
        executor.shutdown(wait=False)

    flat = []
    for group in results:
        if group:
            flat.extend(group)
    return flat


def _safe_run(agent, cancel_check=None):
    if cancel_check:
        agent.cancel_check = cancel_check
    return agent.run()


def agent_list_text(task_id):
    rows = db.agents_for_task(task_id)
    if not rows:
        return "（该任务还没有 Agent）"
    lines = [f"▎Agent 列表（任务 #{task_id}）· {time.strftime('%H:%M:%S')}"]
    for i, r in enumerate(rows, 1):
        st = _state_cn(r)
        lines.append(f"{i}. {r['name']} — {st}")
    return "\n".join(lines)


def agent_detail_text(task_id, index):
    rows = db.agents_for_task(task_id)
    if index < 1 or index > len(rows):
        return f"没有编号 {index} 的 Agent（共 {len(rows)} 个）"
    r = rows[index - 1]
    actions = db.actions_for_agent(r["id"])
    st = _state_cn(r)
    lines = [f"▎Agent {index} · {r['name']}（任务 #{task_id}）· {time.strftime('%H:%M:%S')}",
             f"现在：{st}"]
    if r.get("state") in ("queued", "starting", "thinking", "tool_calling", "waiting_api", "self_healing"):
        lines.append(f"      当前动作：{r.get('current_action') or '—'}（状态更新于 {_ago(r.get('state_ts'))}）")
    lines.append("动作：")
    if actions:
        for a in actions:
            summary = f" → {a['result_summary'][:80]}" if a.get("result_summary") else ""
            lines.append(f"  {a['seq']}. [{a['ts'][11:]}] {a['action_type']}: {a['detail'][:90]}{summary}")
    else:
        lines.append("  （暂无动作记录）")
    if r.get("result"):
        lines.append(f"结果：{r['result'][:300]}")
    elif r.get("error"):
        lines.append(f"错误：{r['error'][:200]}")
    else:
        lines.append("结果：尚未产出")
    return "\n".join(lines)


def _state_cn(r):
    st = r.get("state")
    m = {
        "queued": "排队中", "starting": "启动中", "thinking": "模型思考中",
        "tool_calling": "调用工具中", "waiting_api": "等待接口",
        "self_healing": "自愈重试中", "done": "已完成（已释放）",
        "failed": "失败（已释放）", "cancelled": "已取消",
    }
    base = m.get(st, st)
    if r.get("current_action"):
        base += " · " + r["current_action"][:50]
    if st in ("thinking", "tool_calling", "waiting_api", "self_healing") and r.get("state_ts"):
        ago = _ago(r["state_ts"])
        base += f"（{ago}）"
        try:
            import datetime
            t = datetime.datetime.strptime(r["state_ts"], "%Y-%m-%d %H:%M:%S")
            if (datetime.datetime.now() - t).total_seconds() > AGENT_TIMEOUT:
                base += " ⚠疑似卡住"
        except Exception:
            pass
    return base


def _ago(ts):
    if not ts:
        return "时间未知"
    try:
        import datetime
        t = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        s = (datetime.datetime.now() - t).total_seconds()
        if s < 60:
            return f"距今 {int(s)} 秒"
        if s < 3600:
            return f"距今 {int(s//60)} 分钟"
        return f"距今 {int(s//3600)} 小时"
    except Exception:
        return "时间未知"
