#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""QQ AI 助手主入口。"""
import os
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import db
import bus
import ai
import core


def main():
    db.init()
    config._ensure()
    bus.log("=" * 50)
    bus.log("QQ AI 助手启动")
    bus.log(f"配置：{config.status_text().replace(chr(10), ' | ')}")

    ok, desc = ai.check_key()
    bus.log(f"密钥检测：{'可用' if ok else '不可用'} - {desc}")
    if not ok:
        bus.log("提示：发送「设置密钥sk-xxxx」配置密钥后即可使用 AI 功能")

    core.start_background()
    try:
        bus.listen(core.on_incoming)
    except KeyboardInterrupt:
        bus.log("收到中断，退出")
    except Exception as e:
        bus.log("主循环异常：" + traceback.format_exc()[-1000:])


if __name__ == "__main__":
    main()
