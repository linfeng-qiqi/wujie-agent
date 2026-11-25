#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""配置管理：读取/写入 config.json，密钥脱敏，解析设置命令。"""
import os
import json
import re
import threading

import paths

BASE_DIR = paths.ROOT
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

DEFAULTS = {
    "api_key": "",
    "base_url": "https://api.deepseek.com",
    "model_fast": "deepseek-flash",
    "model_pro": "deepseek-v4-pro",
    "target_qq": "",
    "bot_qq": "",
    "concurrency": 4,
    "silent_text": 2,
    "silent_attach": 8,
    "hard_cap": 30,
    "end_words": ["好了", "发完了", "开始", "执行", "就这些", "go"],
    "doc_keep_default": "7d",       # forever / 7d
    "context_window": 64000,
    "provider": "local",            # local=本地 Ollama（默认）/ deepseek=DeepSeek 在线
    "model_local": "qwen3:1.7b",    # 本地模型名（Ollama 里 ollama list 能看到的）
    "ollama_url": "http://127.0.0.1:11434",
}

_lock = threading.RLock()


def _ensure():
    os.makedirs(BASE_DIR, exist_ok=True)
    if not os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULTS, f, ensure_ascii=False, indent=2)
        os.chmod(CONFIG_PATH, 0o600)


def load():
    _ensure()
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}
    merged = dict(DEFAULTS)
    merged.update(data or {})
    return merged


def get(key, default=None):
    return load().get(key, default)


def set_many(pairs):
    with _lock:
        cfg = load()
        cfg.update(pairs)
        tmp = CONFIG_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        os.replace(tmp, CONFIG_PATH)
        os.chmod(CONFIG_PATH, 0o600)
    return cfg


def mask_key(key):
    if not key:
        return "(未设置)"
    if len(key) <= 10:
        return key[:3] + "****"
    return key[:6] + "****" + key[-4:]


def parse_setting(text):
    """解析 '设置密钥：xxx' / '设置链接xxx'。返回 (kind, value) 或 None。
    只删内容前面的分隔冒号，内容内部的冒号原样保留。"""
    if not text:
        return None
    m = re.match(r"^\s*设置(密钥|链接)\s*[:：]?\s*(.+?)\s*$", text, re.S)
    if not m:
        return None
    kind = "api_key" if m.group(1) == "密钥" else "base_url"
    return kind, m.group(2)


def status_text():
    cfg = load()
    return ("▎当前配置\n"
            f"API 密钥：{mask_key(cfg.get('api_key'))}\n"
            f"API 地址：{cfg.get('base_url')}\n"
            f"模型：{cfg.get('model_fast')}（快）/ {cfg.get('model_pro')}（强）\n"
            f"并发上限：{cfg.get('concurrency')}")
