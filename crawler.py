#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""通用爬虫工具：多源搜索 + 跳转解析 + 正文清洗 + 质量闸门。纯标准库，零依赖。

设计目标：作为联网查询的唯一主力（完全不依赖 DeepSeek 服务端联网工具）。
抓不到 / 质量不达标时用 deep_crawl() 加强兜底：多源+改写查询+放宽容错，
再失败就如实返回未找到可靠数据。抓回来的内容经清洗压缩后交模型整理。
"""
import re
import html
import gzip
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Accept-Encoding": "gzip",
}

# 多源搜索：全部实测可用的国内可访问引擎（百度需验证码、DDG 不可达，故不用）
SEARCH_SOURCES = [
    ("360", "https://www.so.com/s?q={q}"),
    ("bing", "https://cn.bing.com/search?q={q}"),
    ("sogou", "https://www.sogou.com/web?query={q}"),
    ("bingintl", "https://www.bing.com/search?q={q}"),
    ("toutiao", "https://so.toutiao.com/search?keyword={q}"),
]
# 加强兜底才启用的额外源（平时省时间）
DEEP_EXTRA_SOURCES = [
    ("sogou", "https://www.sogou.com/web?query={q}"),
]

# 这些域名是跳转中转/导航站，抓到正文也没意义，直接跳过
NAV_DOMAINS = ("hao.360.com", "www.so.com/link", "so.com/link", "bing.com/ck",
               "baidu.com/link", "sogou.com/link")

FETCH_TIMEOUT = 9
MAX_PAGES = 4
MAX_TOTAL_CHARS = 6000
MAX_PAGE_CHARS = 2600

_WEATHER_WORDS = ("晴", "多云", "阴", "小雨", "中雨", "大雨", "暴雨", "雪", "℃", "气温", "天气", "转")
_TICKET_WORDS = ("元", "票价", "价格", "高铁", "动车", "火车", "机票", "航班", "二等座", "一等座", "特价")

# 意图定向站：搜索词被搜索引擎误解时（如"查询银川天气"被当成快递查询），直接抓这些站
# 城市名由 _city_hint() 从问题里提取后拼进 URL
WEATHER_SITES = [
    ("天气网", "https://www.tianqi.com/{city}/"),
    ("中国天气网", "https://www.weather.com.cn/weather/{code}.shtml"),
]
# 常见城市在中国天气网的代码（只列高频，其余靠天气网拼音）
WEATHER_CODES = {
    "银川": "101170101", "北京": "101010100", "上海": "101020100", "广州": "101280101",
    "深圳": "101280601", "杭州": "101210101", "南京": "101190101", "成都": "101270101",
    "西安": "101110101", "重庆": "101040100", "天津": "101030100", "武汉": "101200101",
    "苏州": "101190401", "郑州": "101180101", "长沙": "101250101", "济南": "101120101",
    "青岛": "101120201", "昆明": "101290101", "贵阳": "101260101", "兰州": "101160101",
    "西宁": "101160801", "呼和浩特": "101080101", "太原": "101100101", "石家庄": "101090101",
    "合肥": "101220101", "南昌": "101240101", "福州": "101230101", "厦门": "101230201",
    "南宁": "101300101", "海口": "101310101", "沈阳": "101070101", "大连": "101070201",
    "长春": "101060101", "哈尔滨": "101050101", "拉萨": "101140101", "乌鲁木齐": "101130101",
}
CITY_PINYIN = {
    "银川": "yinchuan", "北京": "beijing", "上海": "shanghai", "广州": "guangzhou",
    "深圳": "shenzhen", "杭州": "hangzhou", "南京": "nanjing", "成都": "chengdu",
    "西安": "xian", "重庆": "chongqing", "天津": "tianjin", "武汉": "wuhan",
    "苏州": "suzhou", "郑州": "zhengzhou", "长沙": "changsha", "济南": "jinan",
    "青岛": "qingdao", "昆明": "kunming", "贵阳": "guiyang", "兰州": "lanzhou",
    "西宁": "xining", "呼和浩特": "huhehaote", "太原": "taiyuan", "石家庄": "shijiazhuang",
    "合肥": "hefei", "南昌": "nanchang", "福州": "fuzhou", "厦门": "xiamen",
    "南宁": "nanning", "海口": "haikou", "沈阳": "shenyang", "大连": "dalian",
    "长春": "changchun", "哈尔滨": "haerbin", "拉萨": "lasa", "乌鲁木齐": "wulumuqi",
    "宁夏": "yinchuan", "北京": "beijing",
}


def _fetch(url, timeout=FETCH_TIMEOUT):
    """抓取网页，自动解 gzip 与常见编码。返回 (text, final_url)。失败抛异常。"""
    req = urllib.request.Request(url, headers=UA)
    r = urllib.request.urlopen(req, timeout=timeout)
    raw = r.read()
    final = r.geturl()
    if r.headers.get("Content-Encoding") == "gzip":
        try:
            raw = gzip.decompress(raw)
        except Exception:
            pass
    enc = "utf-8"
    m = re.search(rb"charset=['\"]?([\w-]+)", raw[:3000], re.I)
    if m:
        enc = m.group(1).decode("ascii", "ignore")
    try:
        text = raw.decode(enc, "ignore")
    except Exception:
        text = raw.decode("utf-8", "ignore")
    return text, final


def _clean_html(page):
    """去掉脚本/样式/导航等噪音，抽出可读正文。"""
    c = re.sub(r"<(script|style|noscript|nav|footer|header|aside|form|iframe)\b[^>]*>.*?</\1>",
               " ", page, flags=re.S | re.I)
    c = re.sub(r"<!--.*?-->", " ", c, flags=re.S)
    c = re.sub(r"<[^>]+>", " ", c)
    c = html.unescape(c)
    c = re.sub(r"[\u200b-\u200f\ufeff]", "", c)
    c = re.sub(r"\s+", " ", c).strip()
    return c


def _abs_url(base, href):
    if not href:
        return ""
    if href.startswith("//"):
        return "https:" + href
    return urllib.parse.urljoin(base, html.unescape(href))


def _parse_360(page, base):
    """解析 360 搜索结果页。返回 [{title,url,snippet}]。"""
    out = []
    positions = []
    for m in re.finditer(r"<h3[^>]*class=[\"'][^\"']*res-title[^\"']*[\"'][^>]*>(.*?)</h3>", page, re.S | re.I):
        positions.append((m.start(), m.end(), m.group(1)))
    for i, (start, end, inner) in enumerate(positions):
        a = re.search(r"href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", inner, re.S)
        if not a:
            continue
        title = re.sub(r"<[^>]+>", "", html.unescape(a.group(2))).strip()
        url = _abs_url(base, a.group(1))
        nxt = positions[i + 1][0] if i + 1 < len(positions) else min(start + 2500, len(page))
        block = page[end:nxt]
        paras = re.findall(r"<p[^>]*>(.*?)</p>", block, re.S | re.I)
        snippet = " ".join(re.sub(r"<[^>]+>", "", html.unescape(p)).strip() for p in paras)
        snippet = re.sub(r"\s+", " ", snippet).strip()
        if title and url:
            out.append({"title": title[:120], "url": url, "snippet": snippet[:400], "source": "360"})
    return out


def _parse_bing(page, base):
    """解析 Bing 搜索结果页。返回 [{title,url,snippet}]。"""
    out = []
    for m in re.finditer(r"<li class=[\"']b_algo[\"'].*?</li>", page, re.S | re.I):
        block = m.group(0)
        a = re.search(r"<h2[^>]*>\s*<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", block, re.S)
        if not a:
            continue
        title = re.sub(r"<[^>]+>", "", html.unescape(a.group(2))).strip()
        url = _abs_url(base, a.group(1))
        p = re.search(r"<p[^>]*>(.*?)</p>", block, re.S | re.I)
        snippet = re.sub(r"<[^>]+>", "", html.unescape(p.group(1))).strip() if p else ""
        snippet = re.sub(r"\s+", " ", snippet)
        if title and url:
            out.append({"title": title[:120], "url": url, "snippet": snippet[:400], "source": "bing"})
    return out


def _parse_sogou(page, base):
    """解析搜狗搜索结果页。"""
    out = []
    # 取带 linkurl 的真实地址，避开 /link? 中转；无则用 href
    for m in re.finditer(r"<h3[^>]*>\s*<a\b([^>]*)>(.*?)</a>", page, re.S | re.I):
        attrs, inner = m.group(1), m.group(2)
        real = re.search(r"linkurl=[\"']?([^\s\"']+)", attrs)
        href = re.search(r"href=[\"']([^\"']+)[\"']", attrs)
        url = real.group(1) if real else (href.group(1) if href else "")
        url = _abs_url(base, url)
        title = re.sub(r"<!--.*?-->", "", inner, flags=re.S)
        title = re.sub(r"<[^>]+>", "", html.unescape(title)).strip()
        nxt = page[m.end():m.end() + 1200]
        p = re.search(r"<p[^>]*>(.*?)</p>", nxt, re.S | re.I)
        snippet = re.sub(r"<[^>]+>", "", html.unescape(p.group(1))).strip() if p else ""
        if title and url and not _is_nav(url):
            out.append({"title": title[:120], "url": url, "snippet": re.sub(r"\s+", " ", snippet)[:400], "source": "sogou"})
    return out


def _parse_toutiao(page, base):
    """解析头条搜索结果页（标题在 div.title 里的 a，链接多为 /search/jump 跳转）。"""
    out = []
    for m in re.finditer(r"<div[^>]*class=[\"'][^\"']*(?:^|\s|-)title[^\"']*[\"'][^>]*>\s*<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
                         page, re.S | re.I):
        href, inner = m.group(1), m.group(2)
        url = _abs_url(base, href.replace("&amp;", "&"))
        title = re.sub(r"<[^>]+>", "", html.unescape(inner)).strip()
        tail = page[m.end():m.end() + 900]
        p = re.search(r"<(?:p|span)[^>]*class=[\"'][^\"']*(?:abstract|desc|summary)[^\"']*[\"'][^>]*>(.*?)</(?:p|span)>",
                      tail, re.S | re.I)
        snippet = re.sub(r"<[^>]+>", "", html.unescape(p.group(1))).strip() if p else ""
        if title and url:
            out.append({"title": title[:120], "url": url, "snippet": re.sub(r"\s+", " ", snippet)[:400], "source": "toutiao"})
    return out


PARSERS = {"360": _parse_360, "bing": _parse_bing, "sogou": _parse_sogou,
           "bingintl": _parse_bing, "toutiao": _parse_toutiao}


def _search_one(name, tpl, query, deadline):
    """单源搜索，超时内返回结果列表。"""
    try:
        url = tpl.format(q=urllib.parse.quote(query))
        page, final = _fetch(url, timeout=max(2, min(FETCH_TIMEOUT, deadline - time.time())))
        return PARSERS[name](page, final)
    except Exception:
        return []


def search(query, deadline, sources=None):
    """多源搜索合并去重。sources 可传额外源列表（用于加强兜底）。"""
    srcs = SEARCH_SOURCES + list(sources or [])
    # 同一引擎的 URL 模板去重（bingintl 与 bing 解析器相同但域名不同，不去重）
    results = []
    seen = set()
    with ThreadPoolExecutor(max_workers=len(srcs)) as ex:
        futs = [ex.submit(_search_one, name, tpl, query, deadline)
                for name, tpl in srcs]
        for f in as_completed(futs):
            try:
                rows = f.result()
            except Exception:
                rows = []
            for r in rows:
                # 保留查询串：360 的 /link?m=xxx 与头条 jump 全靠参数区分，
                # 早先 split("?") 去重会把同引擎所有结果误杀成 1 条
                key = r["url"][:180]
                if key in seen:
                    continue
                seen.add(key)
                results.append(r)
    return results


def _search_query(query, intent):
    """把口语问句改写成搜索引擎友好的查询串。

    实测 Bing 会把「2026年宁夏专升本录取分数线是多少」误解成只搜「2026年」
    （返回百科/日历/放假通知），必须先剥掉疑问词、提取核心名词再搜索。
    """
    q = str(query or "")
    q = re.sub(r"^(?:请|帮我|给我|麻烦)?\s*"
               r"(?:查询一下|查一下|查询|查|看看|看下|看一下|我想知道|我想问)?\s*", "", q)
    # 去掉疑问/语气词（含句中"的是多少"）
    q = re.sub(r"(?:怎么样|是什么|是什么样|多少钱|多少|有哪些|有哪些|吗|呢|啊|呀|？|\?|！|!|。|，|、)", " ", q)
    q = re.sub(r"\s+", " ", q).strip()
    if intent == "weather":
        # 天气类保留完整（定向站能直接用）
        return str(query)
    if intent == "ticket":
        return (q + " 票价").strip()
    return q or str(query)


def _rewrite_query(query, intent):
    """加强兜底用：改写成搜索引擎友好的关键词串。"""
    return _search_query(query, intent)


def _resolve_redirect(page, url):
    """解 JS / meta 跳转，返回真实 URL；解不出返回空。

    注意 JS 两种写法都要支持：
      window.location.replace("URL")   ← replace 后是括号（360 用这个）
      window.location.href = "URL"     ← href 后是等号
    老代码只写了 replace= 形式，导致 360 跳转从来没解出来过。
    """
    patterns = [
        r"(?:window\.)?location\.replace\s*\(\s*['\"]([^'\"]+)",
        r"(?:window\.)?location\.(?:href|assign)\s*(?:=|\()\s*['\"]([^'\"]+)",
        r"<meta[^>]+http-equiv=['\"]refresh['\"][^>]+content=['\"][^'\"]*url=([^'\"\s>]+)",
    ]
    for pat in patterns:
        m = re.search(pat, page, re.I)
        if m:
            return _abs_url(url, m.group(1))
    return ""


def _is_nav(url):
    low = url.lower()
    return any(n in low for n in NAV_DOMAINS)


def _fetch_page(item, deadline):
    """抓一条结果的正文：先试原 URL（含 360/头条跳转链接），再跟跳转。"""
    url = item["url"]
    left = deadline - time.time()
    if left <= 1:
        return item, ""
    try:
        page, final = _fetch(url, timeout=min(FETCH_TIMEOUT, left))
    except Exception:
        return item, ""
    text = _clean_html(page)
    # 导航/中转页：跟一次 JS/meta 跳转；抓到中转页本身（如 so.com/link 提示页）则放弃
    if len(text) < 80 or _is_nav(final) or _is_nav(url):
        target = _resolve_redirect(page, final)
        if target and not _is_nav(target) and deadline - time.time() > 1:
            try:
                page2, final2 = _fetch(target, timeout=min(FETCH_TIMEOUT, deadline - time.time()))
                if not _is_nav(final2):
                    text = _clean_html(page2)
            except Exception:
                pass
        elif _is_nav(final) or _is_nav(url):
            # urllib 已跟过 302 仍停在导航域名 → 这条无用
            return item, ""
    return item, text[:MAX_PAGE_CHARS]


def detect_intent(query):
    """粗判查询意图，用于质量闸门。"""
    q = str(query or "")
    if any(k in q for k in ("天气", "气温", "温度", "下雨", "下雪", "冷不冷", "穿什么")):
        return "weather"
    if any(k in q for k in ("票", "价格", "多少钱", "机票", "高铁", "火车", "动车", "二等座")):
        return "ticket"
    return "general"


def _city_hint(query):
    """从问题里提取城市名，返回 (中文名, 拼音)。未识别返回 ("", "")。"""
    q = str(query or "")
    # 长名优先，避免"银川"先于"川"之类
    for name in sorted(CITY_PINYIN, key=len, reverse=True):
        if name in q:
            return name, CITY_PINYIN[name]
    return "", ""


def _direct_weather(query, deadline, agent=None):
    """天气类定向抓天气站（搜索引擎常把"查询X天气"误解成快递查询）。
    返回 (material, sources)；失败返回 ("", [])。"""
    city, pinyin = _city_hint(query)
    if not city:
        return "", []
    candidates = [f"https://www.tianqi.com/{pinyin}/"]
    code = WEATHER_CODES.get(city)
    if code:
        candidates.append(f"https://www.weather.com.cn/weather/{code}.shtml")
    pieces = []
    sources = []
    for u in candidates:
        if deadline - time.time() <= 1:
            break
        try:
            page, final = _fetch(u, timeout=min(FETCH_TIMEOUT, deadline - time.time()))
            text = _clean_html(page)[:MAX_PAGE_CHARS]
            if text and city in text:
                pieces.append(f"【{city}天气·{u.split('/')[2]}】\n{text}")
                sources.append({"title": f"{city}天气·{u.split('/')[2]}", "url": final,
                                "source": "direct", "text_len": len(text)})
                if agent:
                    try:
                        agent.log_action("crawl_direct", f"定向抓取 {u.split('/')[2]}", f"正文 {len(text)} 字")
                    except Exception:
                        pass
        except Exception:
            continue
    return "\n\n".join(pieces)[:MAX_TOTAL_CHARS], sources


def _query_terms(query):
    """提取查询中的中文双字词，用于相关性判断。"""
    q = re.sub(r"[^\u4e00-\u9fa5]", "", str(query or ""))
    stop = {" query", "请问", "帮我", "一下", "现在", "今天", "这个", "那个", "多少", "什么", "怎么"}
    terms = {q[i:i + 2] for i in range(len(q) - 1)}
    return [t for t in terms if t.strip() and t not in stop]


def _latest_date(material):
    """提取素材里最晚出现的日期（北京时间基准）。返回 date 或 None。"""
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone(timedelta(hours=8)))
    dates = []
    for m in re.finditer(r"(\d{4})[年/\-.](\d{1,2})[月/\-.](\d{1,2})", material):
        try:
            dates.append(datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date())
        except ValueError:
            pass
    for m in re.finditer(r"(?<!\d)(\d{1,2})月(\d{1,2})日", material):
        try:
            d = datetime(now.year, int(m.group(1)), int(m.group(2))).date()
            if d > now.date() + timedelta(days=1):  # 跨年旧内容：12月出现在1月视角
                d = d.replace(year=d.year - 1)
            dates.append(d)
        except ValueError:
            pass
    return max(dates) if dates else None


def _validate(material, intent, query, relax=None):
    """质量闸门：判断素材够不够格交给模型整理。返回 (ok, reason)。
    relax 只放宽通用类相关性阈值，天气/票务的时效校验永远不放宽。"""
    if not material or len(material.strip()) < 60:
        return False, "素材为空或过短"
    if intent == "weather":
        has_word = any(w in material for w in _WEATHER_WORDS)
        # 温度必须带 ℃ / °C / "度" 单位，避免快递单号/日期数字混入
        temps = re.findall(r"-?\d{1,2}(?:\.\d+)?\s*(?:℃|°C|度)", material)
        city, _ = _city_hint(query)
        city_ok = (not city) or (city in material)  # 提取到城市就必须对得上
        if not has_word or len(temps) < 2 or not city_ok:
            return False, f"天气素材不合格(天气词={has_word},带单位温度={len(temps)},城市匹配={city_ok})"
        # 时效校验：天气素材必须含今天/昨天起的日期，旧闻一律拒收
        latest = _latest_date(material)
        if latest is None:
            return False, "天气素材无任何日期，时效性无法确认"
        age = (datetime.now(timezone(timedelta(hours=8))).date() - latest).days
        if age > 1:
            return False, f"天气素材过期(最新日期距今{age}天)，拒绝使用旧数据"
    elif intent == "ticket":
        has_word = any(w in material for w in _TICKET_WORDS)
        # 票价必须是"数字+货币符号/元/块"，避免把日期/时刻当价格
        has_price = bool(re.search(r"\d{2,5}\s*(?:元|块|￥|¥)", material))
        if not has_word or not has_price:
            return False, "票务素材缺少真实票价（需数字+元/¥），建议走兜底"
        # 时效校验：票价类允许稍宽，但超过 3 天的旧数据也拒收
        latest = _latest_date(material)
        if latest is not None:
            age = (datetime.now(timezone(timedelta(hours=8))).date() - latest).days
            if age > 3:
                return False, f"票务素材过期(最新日期距今{age}天)"
    else:
        terms = _query_terms(query)
        if terms:
            hit = sum(1 for t in terms if t in material)
            threshold = relax if relax is not None else 0.12
            if hit / len(terms) < threshold:
                return False, "素材与问题相关性过低"
    return True, ""


def deep_crawl(query, timeout=40, agent=None):
    """自家联网加强兜底（替代 DeepSeek 服务端联网）。

    策略：① 原查询 5 源并发搜索 + 抓最多 8 页；不合格则 ② 改写关键词
    再来一轮（搜狗/Bing/360），放宽通用相关性闸门，但天气/票务时效
    校验照旧——绝不用旧数据冒充今天。全部失败才返回 ok=False。
    """
    started = time.time()
    deadline = started + timeout
    intent = detect_intent(query)

    def _log(t, detail, summary=""):
        if agent:
            try:
                agent.log_action(t, detail, summary)
            except Exception:
                pass

    def _state(state, action):
        if agent:
            try:
                agent.set_state(state, action)
            except Exception:
                pass

    def _collect(q, sources, max_pages, relax=False):
        mats, srcs = [], []
        if deadline - time.time() <= 2:
            return "", []
        _state("tool_calling", "加强联网搜索中")
        sq = _search_query(q, intent)
        rows = search(sq, deadline, sources=sources)
        _log("deep_search", f"加强搜索「{sq}」得到 {len(rows)} 条",
             " | ".join(r["title"][:30] for r in rows[:5]))
        if not rows:
            return "", []
        # 按源均衡取样：避免最快的 bing 独占名额，把 360 的精准结果挤掉
        picked, per_src = [], {}
        for r in rows:
            src = r.get("source", "")
            if per_src.get(src, 0) >= 2:
                continue
            picked.append(r)
            per_src[src] = per_src.get(src, 0) + 1
            if len(picked) >= max_pages:
                break
        with ThreadPoolExecutor(max_workers=min(8, len(picked))) as ex:
            futs = [ex.submit(_fetch_page, r, deadline) for r in picked]
            for f in as_completed(futs):
                try:
                    item, text = f.result()
                except Exception:
                    continue
                if text:
                    mats.append("【" + item["title"] + "】\n" + text)
                elif item.get("snippet"):
                    mats.append("【" + item["title"] + "】\n" + item["snippet"])
                srcs.append({"title": item["title"], "url": item["url"],
                             "source": item.get("source", ""), "text_len": len(text)})
        # 按与问题的相关性排序：命中的关键词越多越靠前，无关页面排后面不干扰整理
        terms = _query_terms(q)
        if terms:
            mats.sort(key=lambda m: sum(1 for t in terms if t in m) / len(terms), reverse=True)
        return "\n\n".join(mats)[:MAX_TOTAL_CHARS * 2], srcs

    # 天气类先定向抓天气站（最可靠）
    materials, sources = [], []
    if intent == "weather":
        direct_mat, direct_src = _direct_weather(query, deadline, agent)
        if direct_mat:
            materials.append(direct_mat)
            sources.extend(direct_src)

    # 第一轮：全源 + 原始查询
    mat, srcs = _collect(query, [], 8)
    if mat:
        materials.append(mat)
        sources.extend(srcs)
    material = "\n\n".join(materials)[:MAX_TOTAL_CHARS * 2]
    ok, reason = _validate(material, intent, query)
    _log("deep_gate", f"加强第1轮判定: {'通过' if ok else '未通过'}",
         reason or f"素材 {len(material)} 字")
    if ok:
        return {"ok": True, "intent": intent, "material": material, "sources": sources,
                "reason": "", "elapsed": round(time.time() - started, 2)}

    # 第二轮：改写关键词再来（换表述提高命中率）
    rewritten = _rewrite_query(query, intent)
    if rewritten != query and deadline - time.time() > 6:
        materials2, srcs2 = [], list(sources)
        if intent == "weather":
            dm, ds = _direct_weather(rewritten, deadline, agent)
            if dm:
                materials2.append(dm)
                srcs2.extend(ds)
        mat2, srcs2b = _collect(rewritten, DEEP_EXTRA_SOURCES, 8)
        if mat2:
            materials2.append(mat2)
            srcs2.extend(srcs2b)
        material2 = "\n\n".join(materials2)[:MAX_TOTAL_CHARS * 2]
        if material2:
            # 通用类放宽相关性阈值（0.12 → 0.04）；时效类绝不放宽
            ok2, reason2 = _validate(material2, intent, query, relax=0.04)
            _log("deep_gate", f"加强第2轮判定: {'通过' if ok2 else '未通过'}",
                 reason2 or f"素材 {len(material2)} 字")
            if ok2:
                _log("deep_ok", f"改写「{rewritten}」后联网成功", material2[:150])
                return {"ok": True, "intent": intent, "material": material2, "sources": srcs2,
                        "reason": "", "elapsed": round(time.time() - started, 2)}
            material = material2
            sources = srcs2
            reason = reason2

    return {"ok": False, "intent": intent, "material": "", "sources": sources,
            "reason": reason or "所有联网方式均未拿到可靠数据",
            "elapsed": round(time.time() - started, 2)}


def crawl(query, timeout=16, max_pages=MAX_PAGES, agent=None):
    """主入口。返回 dict：
    {
      "ok": bool, "intent": str, "material": str,
      "sources": [{title,url,source,text_len}],
      "reason": str, "elapsed": float
    }
    ok=False 时上层切 deep_crawl() 加强兜底（自家联网）。
    """
    started = time.time()
    deadline = started + timeout
    intent = detect_intent(query)

    def _state(state, action):
        if agent:
            try:
                agent.set_state(state, action)
            except Exception:
                pass

    def _log(t, detail, summary=""):
        if agent:
            try:
                agent.log_action(t, detail, summary)
            except Exception:
                pass

    materials = []
    sources = []

    # 天气类优先走定向天气站（搜索引擎对"查询X天气"误判严重）
    if intent == "weather":
        _state("tool_calling", "定向抓取天气站")
        direct_mat, direct_src = _direct_weather(query, deadline, agent)
        if direct_mat:
            materials.append(direct_mat)
            sources.extend(direct_src)

    # 搜索补充：若定向已拿到足够素材，跳过搜索省时间
    need_search = not (intent == "weather" and materials)
    if need_search:
        _state("tool_calling", "爬虫搜索中")
        sq = _search_query(query, intent)
        rows = search(sq, deadline)
        _log("crawl_search", f"多源搜索「{sq}」得到 {len(rows)} 条结果",
             " | ".join(r["title"][:30] for r in rows[:5]))
        if not rows and not materials:
            return {"ok": False, "intent": intent, "material": "", "sources": [],
                    "reason": "搜索引擎无结果且定向抓取失败", "elapsed": round(time.time() - started, 2)}

        _state("tool_calling", "爬虫抓取正文")
        # 按源均衡取样，避免单一引擎独占名额
        picked, per_src = [], {}
        for r in rows:
            src = r.get("source", "")
            if per_src.get(src, 0) >= 2:
                continue
            picked.append(r)
            per_src[src] = per_src.get(src, 0) + 1
            if len(picked) >= max_pages:
                break
        with ThreadPoolExecutor(max_workers=max(1, len(picked))) as ex:
            futs = [ex.submit(_fetch_page, r, deadline) for r in picked]
            for f in as_completed(futs):
                try:
                    item, text = f.result()
                except Exception:
                    continue
                if text:
                    materials.append("【" + item["title"] + "】\n" + text)
                elif item.get("snippet"):
                    materials.append("【" + item["title"] + "】\n" + item["snippet"])
                sources.append({"title": item["title"], "url": item["url"],
                                "source": item.get("source", ""), "text_len": len(text)})
        # 相关素材优先（避免 bing 误解查询时无关页排前面）
        terms = _query_terms(query)
        if terms:
            materials.sort(key=lambda m: sum(1 for t in terms if t in m) / len(terms), reverse=True)

    material = "\n\n".join(materials)[:MAX_TOTAL_CHARS]
    _log("crawl_result", f"抓取 {len(sources)} 页，素材 {len(material)} 字", material[:200])
    if agent:
        try:
            agent.log_raw("crawler_material", material)
        except Exception:
            pass

    ok, reason = _validate(material, intent, query)
    return {"ok": ok, "intent": intent, "material": material if ok else "",
            "sources": sources, "reason": reason,
            "elapsed": round(time.time() - started, 2)}


# ============================================================
# 坚持不懈的多轮联网研究（替代"搜一轮失败就放弃"的老逻辑）
# 设计参照：DeepSeek 深度思考的连续工具调用——搜不到就换词、换站、
# 换年份、从已抓标题里提炼新词继续找，每一步都在 Agent 动作里留痕。
# ============================================================

def _unique(seq):
    seen, out = set(), []
    for x in seq:
        x = str(x or "").strip()
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _query_variants(query, intent):
    """生成递进式搜索词计划：原句 → 核心词 → 站点限定 → 换年份 → 意图特化。"""
    q = str(query or "").strip()
    sq = _search_query(q, intent)
    out = [q, sq]

    # 核心词：剥掉年份、时间词、疑问词，只留实体
    core = re.sub(r"20\d{2}\s*年?", " ", sq)
    core = re.sub(r"(今天|今日|明天|今日|现在|最新|请问|帮我|给我|一下|查询|查|多少|什么|怎么)", " ", core)
    core = re.sub(r"[\s，。？?！!、：:]+", " ", core).strip()
    if len(core) >= 2:
        out += [core, core + " 官方", core + " 通知 公告",
                "site:gov.cn " + core, "site:edu.cn " + core]

    # 年份回退：2026 没结果就看 2025（明确作为历史参考素材，不冒充今年）
    for y in re.findall(r"(20\d{2})", q):
        try:
            prev = str(int(y) - 1)
            out.append(sq.replace(y, prev))
        except Exception:
            pass

    if intent == "weather":
        city, _pin = _city_hint(q)
        if city:
            out.insert(0, f"{city}天气预报")
            out.append(f"{city} 实时天气 气温")
    elif intent == "ticket":
        if core:
            out.append(core + " 票价 时刻表")
    return _unique(out)[:14]


def _derive_queries(materials, query, intent):
    """词用完后不放弃：从已抓到的标题里提炼新词，构造下一轮搜索。"""
    if not materials:
        return []
    titles = " ".join(str(m.get("title") or "") for m in materials[:24])
    try:
        from collections import Counter
        cnt = Counter(_query_terms(titles))
    except Exception:
        return []
    orig = set(_query_terms(query))
    fresh = [w for w, _c in cnt.most_common() if w not in orig and len(w) >= 2][:6]
    if not fresh:
        return []
    base = _search_query(query, intent)
    return _unique([" ".join(fresh[:4]), base + " " + " ".join(fresh[:2])])


def _balanced_pick(rows, limit):
    """按源均衡取样，避免最快的 bing 独占名额挤掉 360/头条的精准结果。"""
    picked, per = [], {}
    for r in rows:
        src = r.get("source", "")
        if per.get(src, 0) >= 2:
            continue
        picked.append(r)
        per[src] = per.get(src, 0) + 1
        if len(picked) >= limit:
            break
    return picked


def _rank_and_join(materials, query, intent):
    """相关度高的素材排前面（模型只读前半段），按字符预算拼接。"""
    terms = _query_terms(query)

    def sc(m):
        # 标题也算相关性：最精准的结果常常正文抓不动（JS 页），
        # 但标题本身已经把问题里的实体说全了，不能因为正文短就排到后面。
        body = str(m.get("title") or "") + "\n" + str(m.get("text") or "")
        if not terms:
            return 0.0
        return sum(1 for t in terms if t in body) / len(terms)

    ranked = sorted(materials, key=sc, reverse=True)
    out, total = [], 0
    budget = MAX_TOTAL_CHARS * 3
    for m in ranked:
        chunk = str(m.get("text") or "")[:MAX_PAGE_CHARS]
        if not chunk or total + len(chunk) > budget:
            continue
        out.append(chunk)
        total += len(chunk)
    return "\n\n".join(out)


def _score_material(material, query, intent):
    """素材评分：关键词覆盖 + 领域硬指标（温度/价格/日期）。"""
    if not material:
        return 0.0
    terms = _query_terms(query)
    hit = (sum(1 for t in terms if t in material) / len(terms)) if terms else 0.0
    score = hit * 100
    if intent == "weather":
        temps = re.findall(r"-?\d{1,2}(?:\.\d+)?\s*(?:℃|°C|度)", material)
        score += min(len(temps), 12) * 2.5
        if any(w in material for w in _WEATHER_WORDS):
            score += 10
        if _latest_date(material):
            score += 10
    elif intent == "ticket":
        if re.search(r"\d{2,5}\s*(?:元|块|￥|¥)", material):
            score += 25
    else:
        if re.search(r"\d", material):
            score += 5
    return min(100.0, score)


def research(query, timeout=75, agent=None, max_rounds=6):
    """坚持不懈的多轮联网研究。

    与 crawl()/deep_crawl() 的区别：
    - 不再"第一轮质量差就直接返回失败"，而是连续多轮换词找；
    - 每轮搜索、抓取、评分、换词都在 agent 动作里留痕（可在「查看N」看到轨迹）；
    - 只要有素材就返回 ok=True（weak=True 表示质量偏弱，由上层标注"仅供参考"），
      绝不因为闸门没过就把已经搜到的东西扔掉。
    """
    started = time.time()
    deadline = started + timeout
    intent = detect_intent(query)
    plans = _query_variants(query, intent)
    materials, sources, trace = [], [], []
    seen_urls = set()

    def _log(kind, detail, summary=""):
        trace.append({"kind": kind, "detail": detail})
        if agent:
            try:
                agent.log_action(kind, detail, summary)
            except Exception:
                pass

    def _state(state, action):
        if agent:
            try:
                agent.set_state(state, action)
            except Exception:
                pass

    _log("research_plan", f"制定 {len(plans)} 个递进搜索词", " → ".join(plans[:6]))

    # 天气：先定向抓权威天气站（搜索引擎对"查询X天气"误判严重）
    if intent == "weather":
        dm, ds = _direct_weather(query, deadline, agent)
        if dm:
            materials.append({"title": "定向天气站", "url": "", "source": "direct", "text": dm})
            sources.extend(ds)
            _log("research_direct", f"定向天气站抓到 {len(dm)} 字", dm[:120])

    for rnd in range(max_rounds):
        left = deadline - time.time()
        if left < 5:
            _log("research_stop", f"时间预算用完（剩 {left:.1f}s），停止第{rnd+1}轮")
            break

        # 每轮取 2~3 个词；词用完就从已抓标题里造新词继续找（关键：不放弃）
        batch = []
        want = 3 if not materials else 2
        while len(batch) < want and plans:
            batch.append(plans.pop(0))
        if not batch:
            batch = _derive_queries(materials, query, intent)[:2]
            if not batch:
                _log("research_stop", "搜索词与衍生词均耗尽，停止")
                break
            _log("research_derive", f"第{rnd+1}轮衍生新词继续找：{' / '.join(batch)}")

        for q in batch:
            if deadline - time.time() < 4:
                break
            _state("tool_calling", f"第{rnd+1}轮搜索「{q[:24]}」")
            rows = search(q, deadline)
            fresh = [r for r in rows if r.get("url", "")[:180] not in seen_urls]
            for r in fresh:
                seen_urls.add(r.get("url", "")[:180])
            _log("research_search",
                 f"第{rnd+1}轮·第{len(trace)+1}步 搜「{q[:48]}」→ {len(rows)}条 / 新 {len(fresh)}条",
                 " | ".join(r["title"][:26] for r in fresh[:4]))

            picked = _balanced_pick(fresh, 5)
            if not picked:
                continue
            with ThreadPoolExecutor(max_workers=min(5, len(picked))) as ex:
                futs = [ex.submit(_fetch_page, r, deadline) for r in picked]
                for f in as_completed(futs):
                    try:
                        item, text = f.result()
                    except Exception:
                        continue
                    # 正文抓不动时，搜索引擎给的摘要就是关键素材（精准结果常是 JS 页，正文只有几个字）
                    snippet = str(item.get("snippet") or "")
                    body = text or ""
                    if len(body) < 80 and len(snippet) > len(body):
                        body = snippet
                    title = str(item.get("title") or "")
                    # 头条 jump 一律 302 到首页，正文永远抓不到；
                    # 但标题本身往往把年份/地区/事件说全了，作为线索保留，不让精准结果凭空消失。
                    if len(body) < 24 and title and any(
                            t in title for t in _query_terms(query)):
                        body = f"【搜索结果标题：{title}】该页面正文无法抓取，仅有标题线索。"
                    if not body:
                        continue
                    materials.append({"title": item.get("title", ""), "url": item.get("url", ""),
                                      "source": item.get("source", ""), "text": body})
                    sources.append({"title": item.get("title", ""), "url": item.get("url", ""),
                                    "source": item.get("source", ""), "text_len": len(body)})
                    _log("research_fetch", f"读「{item.get('title','')[:32]}」→ {len(body)}字",
                         item.get("url", "")[:100])

        # 每轮评分 + 判定：过了就收工，没过就进下一轮（换词/换站/换年份）
        material = _rank_and_join(materials, query, intent)
        score = _score_material(material, query, intent)
        ok, reason = _validate(material, intent, query, relax=0.04)
        threshold = 45 if intent in ("weather", "ticket") else 42
        _log("research_gate",
             f"第{rnd+1}轮判定：{score:.0f}分/阈值{threshold} · 素材{len(material)}字 · 闸门{'通过' if ok else reason}",
             f"累计抓取 {len(materials)} 页")
        # 只有分数真的够才收工。"闸门通过"只是最低门槛（相关性>4%），
        # 之前只看闸门导致第一轮抓到一堆无关长文就停下——必须继续换词找精准结果。
        if material and score >= threshold:
            _log("research_ok", f"第{rnd+1}轮找到足够素材（{score:.0f}分）")
            break
        _log("research_continue", f"第{rnd+1}轮未达标（{score:.0f}<{threshold}），继续换词找（已抓 {len(materials)} 页）")

    material = _rank_and_join(materials, query, intent)
    if not material:
        return {"ok": False, "intent": intent, "material": "", "sources": sources,
                "rounds": len(trace), "trace": trace, "weak": False,
                "reason": f"{max_rounds}轮多源搜索均未拿到任何素材",
                "elapsed": round(time.time() - started, 2)}

    ok, reason = _validate(material, intent, query, relax=0.04)
    score = _score_material(material, query, intent)
    # 关键：有素材就不扔。weak 让上层标注"仅供参考"，但绝不直接说"未找到"。
    weak = not ok or score < 30
    if weak:
        _log("research_weak", f"找到素材但质量偏弱（{score:.0f}分），标注仅供参考后仍交模型整理",
             reason)
    return {"ok": True, "weak": weak, "intent": intent, "material": material,
            "sources": sources, "rounds": len(trace), "trace": trace,
            "reason": reason, "score": round(score, 1),
            "elapsed": round(time.time() - started, 2)}
