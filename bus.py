#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""QQ 通道：NapCat WebSocket 收消息 + HTTP API 发消息 + 附件下载校验。纯标准库。"""
import socket
import base64
import os
import json
import re
import struct
import time
import hashlib
import threading
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

import config
import db
import paths

WS_HOST = paths.WS_HOST
WS_PORT = paths.WS_PORT
HTTP_BASE = paths.HTTP_BASE
ATTACH_DIR = paths.ATTACH_DIR
LOG_DIR = paths.LOG_DIR
os.makedirs(ATTACH_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

_log_lock = threading.Lock()


def log(msg):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    with _log_lock:
        print(line, flush=True)
        try:
            with open(os.path.join(LOG_DIR, "assistant.log"), "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass


# ---------------- WebSocket ----------------

def ws_connect():
    s = socket.create_connection((WS_HOST, WS_PORT), timeout=10)
    key = base64.b64encode(os.urandom(16)).decode()
    req = (f"GET / HTTP/1.1\r\nHost: {WS_HOST}:{WS_PORT}\r\n"
           f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
           f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n")
    s.sendall(req.encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = s.recv(4096)
        if not chunk:
            raise ConnectionError("握手无响应")
        resp += chunk
    if b"101" not in resp.split(b"\r\n")[0]:
        raise ConnectionError("握手失败: " + resp[:200].decode(errors="replace"))
    s.settimeout(None)
    return s


def ws_send(sock, data):
    payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
    header = bytearray([0x81])
    n = len(payload)
    if n < 126:
        header.append(0x80 | n)
    elif n < 65536:
        header.append(0x80 | 126)
        header += struct.pack("!H", n)
    else:
        header.append(0x80 | 127)
        header += struct.pack("!Q", n)
    mask = os.urandom(4)
    header += mask
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    sock.sendall(bytes(header) + masked)


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("连接已关闭")
        buf += chunk
    return buf


def ws_recv(sock):
    b1, b2 = _recv_exact(sock, 2)
    opcode = b1 & 0x0F
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack("!H", _recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", _recv_exact(sock, 8))[0]
    if b2 & 0x80:
        mask = _recv_exact(sock, 4)
        payload = _recv_exact(sock, length)
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    else:
        payload = _recv_exact(sock, length)
    if opcode == 0x8:
        raise ConnectionError("收到关闭帧")
    if opcode == 0x9:
        pong = bytearray([0x8A])
        mask = os.urandom(4)
        pong.append(0x80 | len(payload))
        pong += mask
        pong += bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        try:
            sock.sendall(bytes(pong))
        except Exception:
            pass
        return None
    if opcode == 0xA:
        return None
    return payload.decode("utf-8", errors="replace")


# ---------------- HTTP 发送 ----------------

def http_api(action, payload):
    url = f"{HTTP_BASE}/{action}"
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8", "replace"))
    except Exception as e:
        log(f"HTTP API {action} 失败: {type(e).__name__}: {e}")
        return {"retcode": -1, "error": str(e)}


def send_text(user_id, text, group_id=None):
    """发送文本，长消息分段（QQ 单条上限约 4500 字符，保守切 1200）。"""
    segs = [text[i:i + 1200] for i in range(0, len(text), 1200)] or [""]
    results = []
    for seg in segs:
        payload = {"message": [{"type": "text", "data": {"text": seg}}]}
        if group_id:
            payload["group_id"] = group_id
            payload["message_type"] = "group"
            results.append(http_api("send_group_msg", payload))
        else:
            payload["user_id"] = user_id
            results.append(http_api("send_private_msg", payload))
        time.sleep(0.25)
    return results


def send_service(user_id, text):
    """任务播报走独立会话，避免和自动回复混淆。"""
    return send_text(user_id, text)


_NAPCAT_PATH_MAP = ((paths.NAPCAT_DIR, paths.NAPCAT_CONTAINER_DIR),)


def _napcat_path(path):
    """宿主机路径映射成 NapCat 容器内路径。"""
    for host_prefix, container_prefix in _NAPCAT_PATH_MAP:
        if path.startswith(host_prefix):
            return container_prefix + path[len(host_prefix):]
    return path


def send_file(user_id, path, group_id=None, name=None):
    """发送本地文件（OneBot upload_private_file / upload_group_file）。"""
    path = os.path.abspath(str(path or ""))
    if not os.path.isfile(path):
        return {"retcode": -1, "error": f"文件不存在: {path}"}
    payload = {
        "file": _napcat_path(path),
        "name": name or os.path.basename(path),
    }
    if group_id:
        payload["group_id"] = group_id
        return http_api("upload_group_file", payload)
    payload["user_id"] = user_id
    return http_api("upload_private_file", payload)


# ---------------- 消息解析 ----------------

# 纯表情段类型：QQ 小黄脸、魔法表情、骰子、猜拳 —— 一律不识别、不进文本
_STICKER_SEG_TYPES = {"face", "mface", "sticker", "dice", "rps"}

# image 段中这些 sub_type 代表表情包/斗图，不是用户想让 AI 识别的图片
_STICKER_IMAGE_SUBTYPES = {"2", "3", "7", "8", "9"}

# Unicode Emoji 字符（文本里的 😀🎉👍 等），从文字中剥离，不当成任务内容
# 覆盖：杂项符号、装饰符号、补充符号、flag、keycap、肤色修饰、ZWJ、变体选择符
_EMOJI_RE = re.compile(
    "["
    "\U0001F000-\U0001FAFF"  # 表情符号主区（含 emoji 表情、旗帜、手势）
    "\u2600-\u27BF"          # 杂项符号 + 装饰符号（☀✈❤ 等）
    "\u2190-\u21FF"          # 箭头
    "\u2B00-\u2BFF"          # 杂项符号和箭头
    "\uFE0F\uFE0E"           # 变体选择符
    "\u200D"                 # 零宽连接符（组合 emoji）
    "\u20E3"                 # keycap 组合符（1️⃣）
    "]"
)


def strip_emoji(text):
    """去掉文本中的 Unicode emoji 字符，保留普通文字。"""
    if not text:
        return ""
    return _EMOJI_RE.sub("", text)


def _is_sticker_image(data):
    """判断 image 段是不是表情包（QQ 表情包常带 summary 字段或特殊 sub_type）。"""
    st = str(data.get("sub_type", "") or "")
    if st in _STICKER_IMAGE_SUBTYPES:
        return True
    if data.get("summary"):
        return True
    return False


def has_usable_content(message):
    """消息里是否有需要处理的真实内容（文字 / 非表情包附件）。

    纯表情包、纯表情符号的消息返回 False，调用方应直接忽略，
    不建任务、不识别图片、不入库。
    """
    if isinstance(message, str):
        return bool(message.strip())
    for seg in message if isinstance(message, list) else []:
        if not isinstance(seg, dict):
            continue
        t = seg.get("type")
        d = seg.get("data", {}) or {}
        if t == "text" and strip_emoji(d.get("text") or "").strip():
            return True
        if t in ("image", "file", "record", "video"):
            if t == "image" and _is_sticker_image(d):
                continue
            return True
    return False


def parse_segments(message, task_id=None):
    """解析 OneBot 消息段 → (纯文本, 附件列表)。表情包一律忽略。"""
    texts = []
    attachments = []
    if isinstance(message, str):
        return message, attachments
    for seg in message if isinstance(message, list) else []:
        if not isinstance(seg, dict):
            continue
        t = seg.get("type")
        d = seg.get("data", {}) or {}
        if t in _STICKER_SEG_TYPES:
            # 表情符号/表情包：完全忽略，不进文本也不建附件
            continue
        if t == "text":
            texts.append(strip_emoji(d.get("text", "")))
        elif t == "image":
            if _is_sticker_image(d):
                log(f"忽略表情包图片: {str(d.get('file',''))[:40]} (sub_type={d.get('sub_type')})")
                continue
            attachments.append(_make_attachment(task_id, d, "image"))
            texts.append("[图片]")
        elif t == "file":
            attachments.append(_make_attachment(task_id, d, "file"))
            texts.append(f"[文件:{d.get('file', '未知')}]")
        elif t == "record":
            attachments.append(_make_attachment(task_id, d, "audio"))
            texts.append("[语音]")
        elif t == "at":
            texts.append(f"[@{d.get('qq')}]")
        elif t == "video":
            attachments.append(_make_attachment(task_id, d, "video"))
            texts.append("[视频]")
        else:
            texts.append(f"[{t}]")
    return "".join(texts), attachments


def _make_attachment(task_id, data, kind):
    url = data.get("url") or ""
    name = data.get("file") or f"{kind}_{int(time.time())}"
    return {
        "task_id": task_id,
        "kind": kind,
        "orig_name": name,
        "url": url,
        "file_field": data.get("file", ""),
    }


def download_attachment(info, max_wait=40):
    """下载附件到本地并校验完整（大小稳定 + 非空 + 哈希）。返回 (path, sha, size) 或 (None, '', 0)。"""
    kind = info.get("kind", "file")
    name = info.get("orig_name") or "file"
    safe = "".join(c for c in name if c.isalnum() or c in "._-")[:80]
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(ATTACH_DIR, f"{stamp}_{safe}")
    url = info.get("url", "")

    # 1) 优先从 NapCat 本地缓存找（图片文件常在容器挂载目录）
    local_hit = _find_local_cached(info.get("file_field", ""))
    if local_hit:
        try:
            import shutil
            shutil.copyfile(local_hit, path)
            return _verify(path)
        except Exception:
            pass

    # 2) HTTP 下载
    if not url:
        return None, "", 0
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0",
                                                   "Referer": "https://im.qq.com/"})
        with urllib.request.urlopen(req, timeout=max_wait) as resp, open(path, "wb") as f:
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                f.write(chunk)
        return _verify(path)
    except Exception as e:
        log(f"附件下载失败 {safe}: {type(e).__name__}: {e}")
        try:
            os.remove(path)
        except Exception:
            pass
        return None, "", 0


def _find_local_cached(file_field):
    if not file_field:
        return None
    roots = [os.path.join(paths.NAPCAT_DIR, ".config", "QQ"),
             os.path.join(paths.NAPCAT_DIR, "napcat", "cache")]
    base = os.path.basename(file_field)
    for root in roots:
        if not os.path.isdir(root):
            continue
        for dirpath, _, files in os.walk(root):
            if base in files:
                return os.path.join(dirpath, base)
    return None


def _verify(path, stable_checks=2, interval=0.6):
    if not os.path.exists(path):
        return None, "", 0
    last = -1
    for _ in range(stable_checks):
        size = os.path.getsize(path)
        if size == 0:
            return None, "", 0
        if size == last:
            h = hashlib.sha256()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(131072), b""):
                    h.update(chunk)
            return path, h.hexdigest(), size
        last = size
        time.sleep(interval)
    return path, hashlib.sha256(open(path, "rb").read()).hexdigest(), os.path.getsize(path)


# ---------------- 监听循环 ----------------

def listen(on_message, stop_event=None):
    """阻塞监听。on_message(data dict) 由上层处理。断线自动重连。"""
    target = str(config.get("target_qq", ""))
    log(f"===== QQ 通道启动，监听目标: {target} =====")
    sock = None
    while True:
        if stop_event and stop_event.is_set():
            break
        try:
            sock = ws_connect()
            log("WebSocket 已连接，开始监听")
            while True:
                raw = ws_recv(sock)
                if raw is None:
                    continue
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if data.get("post_type") != "message":
                    continue
                sender = data.get("sender", {})
                uid = str(sender.get("user_id", ""))
                if target and uid != target:
                    continue
                # 图片段结构调试：记录 sub_type/summary，便于校准表情包识别规则
                try:
                    segs = data.get("message") or []
                    for s in segs:
                        if isinstance(s, dict) and s.get("type") == "image":
                            sd = s.get("data", {}) or {}
                            log(f"[图片段] sub_type={sd.get('sub_type')!r} summary={sd.get('summary')!r} file={str(sd.get('file',''))[:30]!r}")
                except Exception:
                    pass
                on_message(data)
        except Exception as e:
            log(f"WS 异常: {type(e).__name__}: {e}，5 秒后重连")
            try:
                if sock:
                    sock.close()
            except Exception:
                pass
            time.sleep(5)


def cleanup_expired_docs():
    """删除过期的 7 天文档附件。"""
    rows = db.expired_documents()
    for r in rows:
        path = r.get("path")
        if path and os.path.exists(path):
            try:
                os.remove(path)
                log(f"清理过期文档: {path}")
            except Exception as e:
                log(f"清理失败: {e}")
        db.delete_document(r["id"])
