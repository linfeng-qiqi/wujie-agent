#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""冒烟测试：不联网、不调模型，验证核心逻辑可用。

运行：python3 tests/smoke_test.py
"""
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

passed = 0
failed = 0


def check(name, cond):
    global passed, failed
    if cond:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}")


def main():
    # 用临时目录作数据根，避免污染真实数据
    tmp = tempfile.mkdtemp(prefix="wujie-test-")
    os.environ["WUJIE_HOME"] = tmp

    print("\n[1] 编译所有模块")
    import py_compile
    for fn in ("paths.py", "config.py", "db.py", "bus.py", "ai.py",
               "agents.py", "crawler.py", "memory.py", "cache.py",
               "core.py", "trace.py", "listener.py", "main.py"):
        try:
            py_compile.compile(os.path.join(ROOT, fn), doraise=True)
            check(fn, True)
        except Exception as e:
            check(fn + f" ({e})", False)

    print("\n[2] 路径配置")
    import paths
    check("ROOT 指向临时目录", paths.ROOT == os.path.abspath(tmp))
    check("目录自动创建", all(os.path.isdir(d) for d in
          (paths.ATTACH_DIR, paths.LOG_DIR, paths.EXPORT_DIR)))

    print("\n[3] 数据库初始化")
    import db
    db.init()
    check("tasks 表存在", db.query_one("SELECT COUNT(*) c FROM tasks") is not None)
    check("初始无任务", db.query_one("SELECT COUNT(*) c FROM tasks")["c"] == 0)

    print("\n[4] 任务拆解（规则化，零模型）")
    import agents
    t = agents.fast_plan("查明天银川天气", [])
    check("单任务拆解", isinstance(t, list) and len(t) >= 1)
    check("目标保留原话", "银川" in t[0]["goal"])
    t2 = agents.fast_plan("帮我查一下明天银川的天气；另外帮我写一份请假条给老师", [])
    check("多任务拆分", len(t2) >= 2)
    t3 = agents.fast_plan("查天气;另外写通知", [])
    check("短句不误拆", len(t3) == 1)

    print("\n[5] 分流判断")
    hist = [{"role": "user", "content": "查明天银川天气"},
            {"role": "assistant", "content": "银川明天晴"}]
    check("追问直答", agents.needs_agent("你这查的是几点的?", [], hist) is False)
    check("寒暄直答", agents.needs_agent("你好", [], hist) is False)
    check("查询建Agent", agents.needs_agent("查明天银川天气", [], []) is True)
    check("写作建Agent", agents.needs_agent("帮我写一份通知", [], []) is True)

    print("\n[6] 隐私检查（发布目录不得含真实身份）")
    import glob
    secrets = ("346" + "6415615", "3835" + "427398", "sk-654" + "051",
               "114.66.1" + ".149", "192.168.85" + ".129")
    leaked = []
    for fp in glob.glob(os.path.join(ROOT, "**", "*.py"), recursive=True):
        txt = open(fp, encoding="utf-8").read()
        for s in secrets:
            if s in txt:
                leaked.append(f"{os.path.basename(fp)}:{s}")
    check(f"无敏感信息残留 {leaked if leaked else ''}", not leaked)

    print(f"\n结果：{passed} 通过 / {failed} 失败")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
