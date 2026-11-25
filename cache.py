#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""搜索结果缓存：按时效性分层 TTL，命中直接调库，省钱且不返回旧闻。"""
import re
import time

import db

# 缓存有效期（秒）。稳定事实长期存；时效数据短 TTL 过期自动重查。
TTL = {
    "fact": 30 * 86400,     # 稳定事实：定义/知识/百科/歌词/翻译 → 30 天
    "weather": 30 * 60,     # 天气：30 分钟
    "ticket": 15 * 60,      # 票务：15 分钟
    "news": 2 * 3600,       # 新闻/最新 → 2 小时
    "general": 6 * 3600,    # 其他查询：6 小时
}

# 时效意图关键词（决定是否短 TTL）
_NEWS_WORDS = ("新闻", "最新", "今日", "today", "热点", "实时", "刚刚", "近期", "现在")
_WEATHER_WORDS = ("天气", "气温", "温度", "下雨", "下雪", "冷不冷", "穿什么", "预报")
_TICKET_WORDS = ("票价", "机票", "高铁", "火车", "动车", "二等座", "多少钱", "车票")
_FACT_WORDS = ("是什么", "什么是", "什么意思", "定义", "谁发明", "由来", "歌词",
               "翻译", "怎么读", "英文名", "全称", "简称", "为什么", "介绍",
               "讲讲", "解释一下", "哪个更", "区别")


def normalize(question):
    """问题标准化：去空格/标点/语气词/emoji，小写化，用于命中相同意图。"""
    q = str(question or "")
    # 去 emoji 与标点，保留中文/字母/数字
    q = re.sub(r"[^\w\u4e00-\u9fa5]", "", q)
    for filler in ("请问", "帮我", "一下", "帮我查", "查一下", "告诉我", "我想知道", "现在的", "的"):
        q = q.replace(filler, "")
    return q.lower().strip()


def cache_key(question):
    return normalize(question)


def classify_ttl(question, intent="general"):
    """根据问题和爬虫意图判定缓存类别，返回 (类别, 秒数)。"""
    q = str(question or "")
    if intent == "weather" or any(w in q for w in _WEATHER_WORDS):
        return "weather", TTL["weather"]
    if intent == "ticket" or any(w in q for w in _TICKET_WORDS):
        return "ticket", TTL["ticket"]
    if any(w in q for w in _NEWS_WORDS):
        return "news", TTL["news"]
    if any(w in q for w in _FACT_WORDS):
        return "fact", TTL["fact"]
    return "general", TTL["general"]


def get(question):
    """命中缓存返回 (answer, category)；否则 (None, None)。命中会记 hits+1。"""
    key = cache_key(question)
    if not key:
        return None, None
    row = db.cache_get(key)
    if not row:
        return None, None
    db.cache_hit(key)
    return row["answer"], (row["intent"] or "general")


def put(question, answer, intent="general"):
    """写入缓存。answer 必须是最终要发给用户的文本。"""
    key = cache_key(question)
    if not key or not answer:
        return False
    category, ttl = classify_ttl(question, intent)
    db.cache_put(key, question, answer, category, "search", ttl)
    return True


def mark(answer, category):
    """命中缓存时在回复末尾加简短来源标注。"""
    if not answer:
        return answer
    label = {"fact": "稳定知识", "weather": "天气", "ticket": "票务",
             "news": "资讯", "general": "查询"}.get(category, "查询")
    return f"{answer}\n（{label}缓存命中，零 token）"


def stats_text():
    s = db.cache_stats()
    expired = f"，其中 {s['expired']} 条已过期待更新" if s["expired"] else ""
    return (f"▎搜索缓存库\n"
            f"缓存 {s['total']} 条，累计命中 {s['hits']} 次{expired}\n"
            f"稳定事实30天 / 新闻2小时 / 天气30分 / 票务15分")
