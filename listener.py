#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QQ 消息监听器 - 零依赖（仅用 Python 标准库）
连接 NapCat 的 WebSocket 接口，只处理指定 QQ 号的消息。
数据目录默认在脚本所在目录，可用环境变量 WUJIE_HOME 覆盖。
"""
import socket
import base64
import os
import json
import struct
import time
import hashlib
import threading
from datetime import datetime

import paths

# ============ 配置 ============
WS_HOST = paths.WS_HOST
WS_PORT = paths.WS_PORT
TARGET_QQ = os.environ.get("WUJIE_TARGET_QQ", "").strip()  # 要监听的 QQ 号，空=不过滤
LOG_DIR = paths.LOG_DIR
LOG_FILE = os.path.join(LOG_DIR, "messages.log")

os.makedirs(LOG_DIR, exist_ok=True)


def log(msg):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def ws_connect(host, port):
    """用标准库完成 WebSocket 握手"""
    s = socket.create_connection((host, port), timeout=10)
    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        f"GET / HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"\r\n"
    )
    s.sendall(req.encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = s.recv(4096)
        if not chunk:
            raise ConnectionError("握手无响应")
        resp += chunk
    if b"101" not in resp.split(b"\r\n")[0]:
        raise ConnectionError("握手失败: " + resp[:200].decode(errors="replace"))
    # 取消超时，避免空闲时误触发重连
    s.settimeout(None)
    log("WebSocket 连接成功")
    return s


def ws_send(sock, data):
    """发送 WebSocket 文本帧（客户端需掩码）"""
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


def ws_recv(sock):
    """接收一帧 WebSocket 文本"""
    def recv_exact(n):
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("连接已关闭")
            buf += chunk
        return buf

    b1, b2 = recv_exact(2)
    opcode = b1 & 0x0F
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(8))[0]
    if b2 & 0x80:
        mask = recv_exact(4)
        payload = recv_exact(length)
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    else:
        payload = recv_exact(length)

    if opcode == 0x8:
        raise ConnectionError("收到关闭帧")
    if opcode == 0x9:
        # 收到服务端 ping，回 pong（客户端帧需掩码）
        pong = bytearray([0x8A])
        mask = os.urandom(4)
        pong.append(0x80 | len(payload))
        pong += mask
        pong += bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        try:
            sock.sendall(bytes(pong))
        except Exception:
            pass
        return None  # ping 不作为消息处理
    return payload.decode("utf-8", errors="replace")


def extract_text(message):
    """从 OneBot 消息段中提取纯文本"""
    if isinstance(message, str):
        return message
    texts = []
    for seg in message:
        if isinstance(seg, dict):
            t = seg.get("type")
            d = seg.get("data", {})
            if t == "text":
                texts.append(d.get("text", ""))
            elif t == "image":
                texts.append("[图片]")
            elif t == "record":
                texts.append("[语音]")
            elif t == "face":
                texts.append(f"[表情:{d.get('id')}]")
            elif t == "at":
                texts.append(f"[ @{d.get('qq')} ]")
            else:
                texts.append(f"[{t}]")
    return "".join(texts)


def main():
    log(f"===== 监听器启动，目标 QQ: {TARGET_QQ} =====")
    while True:
        try:
            sock = ws_connect(WS_HOST, WS_PORT)
            log("正在监听消息...")
            while True:
                raw = ws_recv(sock)
                if raw is None:
                    continue
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # 只处理消息事件
                if data.get("post_type") != "message":
                    continue

                sender = data.get("sender", {})
                user_id = str(sender.get("user_id", ""))
                msg_type = data.get("message_type")
                text = extract_text(data.get("message", []))

                # 只监听目标 QQ（未配置则处理所有消息）
                if TARGET_QQ and user_id != TARGET_QQ:
                    continue

                nickname = sender.get("nickname", "未知")
                scene = "私聊" if msg_type == "private" else f"群聊({data.get('group_id', '')})"
                log(f"【命中】{scene} | {nickname}({user_id}): {text}")

        except Exception as e:
            log(f"连接异常: {type(e).__name__}: {e}，5秒后重连...")
            try:
                sock.close()
            except Exception:
                pass
            time.sleep(5)


if __name__ == "__main__":
    main()
