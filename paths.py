#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""路径与连接配置：集中管理所有目录和 NapCat 地址，全部可用环境变量覆盖。

默认把数据写在项目目录内（clone 下来即可运行）；生产环境用 WUJIE_HOME
指到独立数据盘即可，无需改代码。

环境变量：
    WUJIE_HOME         数据根目录（默认：本文件所在目录）
    WUJIE_NAPCAT_DIR   NapCat 数据目录（默认：<数据根>/napcat-data）
    WUJIE_WS_HOST      NapCat 正向 WS 主机（默认 127.0.0.1）
    WUJIE_WS_PORT      NapCat 正向 WS 端口（默认 3001）
    WUJIE_HTTP_PORT    NapCat HTTP API 端口（默认 3000）
    WUJIE_TARGET_QQ    只处理该 QQ 号的消息（默认空=不过滤）
"""
import os

# 数据根目录：默认脚本所在目录，可用环境变量覆盖
ROOT = os.path.abspath(os.environ.get("WUJIE_HOME") or os.path.dirname(os.path.abspath(__file__)))

# NapCat 数据目录（用于附件定位 / 导出）
NAPCAT_DIR = os.path.abspath(
    os.environ.get("WUJIE_NAPCAT_DIR") or os.path.join(ROOT, "napcat-data"))

ATTACH_DIR = os.path.join(ROOT, "attachments")
LOG_DIR = os.path.join(ROOT, "logs")
EXPORT_DIR = os.path.join(ROOT, "exports")
NAPCAT_CONTAINER_DIR = "/home/container"

# NapCat 连接地址
WS_HOST = os.environ.get("WUJIE_WS_HOST", "127.0.0.1")
WS_PORT = int(os.environ.get("WUJIE_WS_PORT", "3001"))
HTTP_BASE = "http://{}:{}".format(
    WS_HOST, os.environ.get("WUJIE_HTTP_PORT", "3000"))

for _d in (ROOT, ATTACH_DIR, LOG_DIR, EXPORT_DIR):
    os.makedirs(_d, exist_ok=True)
