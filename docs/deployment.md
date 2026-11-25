# 部署指南

本文给出从零到跑起来的完整步骤。所有路径都可用环境变量覆盖（见 [`../README.md`](../README.md) 配置表），下文以默认路径为例。

## 一、目录规划

默认把代码和数据都放在项目目录内。生产环境建议数据单独放：

```bash
export WUJIE_HOME=/opt/wujie-data        # 数据根目录
export WUJIE_NAPCAT_DIR=/opt/napcat-data  # NapCat 数据目录
```

挂载点结构：

```
<WUJIE_HOME>/
├── config.json        # 配置（权限 600）
├── agent.db           # SQLite 数据库（首次运行自动建）
├── attachments/       # 附件
├── exports/           # 导出的追溯文档
└── logs/              # 日志
```

## 二、Python 环境

```bash
python3 --version    # 需要 3.10+
# 零第三方依赖，无需 pip install
```

## 三、本地模型（Ollama）

```bash
curl -fsSL https://ollama.com/install.sh | sh
ollama pull qwen3:1.7b        # 4 核 4G 推荐
# 性能更好可选：ollama pull qwen3:4b
```

低配机器建议常驻单模型，避免来回加载吃满内存：

```bash
sudo systemctl edit ollama
```

写入：

```ini
[Service]
Environment=OLLAMA_NUM_PARALLEL=1
Environment=OLLAMA_KEEP_ALIVE=30m
Environment=OLLAMA_MAX_LOADED_MODELS=1
```

然后 `sudo systemctl daemon-reload && sudo systemctl restart ollama`。

## 四、NapCat（QQ 协议端）

### 启动容器

```bash
docker run -d --name napcat \
  --restart unless-stopped \
  -p 3000:3000 -p 3001:3001 -p 6099:6099 \
  -v /opt/napcat-data:/home/container \
  mlikiowa/napcat-docker:latest
```

### 配置 OneBot11

把 [`napcat-config-example/onebot11.json`](napcat-config-example/onebot11.json) 放到 `/opt/napcat-data/napcat/config/onebot11.json`，关键项：

- `http.enable = true`，端口 `3000`
- `ws.enable = true`，端口 `3001`（本项目走正向 WS 收消息 + HTTP API 发消息）
- `token` 留空（如果填了 token，需同步到代码连接逻辑）

### WebUI 登录

打开 `http://<服务器IP>:6099/webui`，首次登录用 [`napcat-config-example/webui.json`](napcat-config-example/webui.json) 里的 token（**部署后请立刻改成自己的随机串**）。登录后扫码让机器人 QQ 上线。

## 五、应用配置

```bash
cd /path/to/wujie-qq-agent
cp config.example.json config.json
chmod 600 config.json
```

编辑 `config.json`：

- `target_qq`：你的 QQ 号（只处理这个号发来的消息）
- `bot_qq`：机器人 QQ 号
- `provider`：`local`（默认，零成本）或 `deepseek`（需密钥）
- `api_key`：本地模式留空；在线模式填 DeepSeek 密钥

也可以完全用环境变量运行，不落盘密钥：

```bash
WUJIE_HOME=/opt/wujie-data \
WUJIE_TARGET_QQ=你的QQ号 \
python3 main.py
```

密钥还能在 QQ 里发命令设置：`设置密钥sk-xxxx`（回显脱敏，不写日志）。

## 六、启动

前台运行（先验证）：

```bash
python3 main.py
```

看到 `QQ AI 助手启动` 且 NapCat 在线，给你的机器人发 `菜单` 验证。

### systemd 托管

新建 `/etc/systemd/system/wujie.service`（按需改路径）：

```ini
[Unit]
Description=Wujie QQ Agent
After=network.target docker.service
Requires=docker.service

[Service]
Type=simple
WorkingDirectory=/path/to/wujie-qq-agent
Environment=WUJIE_HOME=/opt/wujie-data
Environment=PYTHONUNBUFFERED=1
ExecStart=/usr/bin/python3 /path/to/wujie-qq-agent/main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now wujie
sudo systemctl status wujie
```

## 七、常见问题

| 现象 | 排查 |
|---|---|
| 发消息没反应 | 检查 NapCat 是否在线、`ws.enable` 是否为 true、端口 3001 是否通 |
| 连接被拒 | `WUJIE_WS_HOST` / `WUJIE_WS_PORT` 是否指向正确地址 |
| 模型回答很慢 | 确认没有残留的 `ollama run` 手动进程占内存；`ollama ps` 看常驻模型 |
| 401 认证失败 | 在线模式密钥失效；本地模式不需要密钥，检查 `provider` 是否为 `local` |
| 查不到内容 | QQ 发 `查看agent列表`、`查看N` 看检索轨迹；可能目标站反爬或无该数据 |
| 命中旧答案 | 结果按时效缓存，发 `清空所有任务` 不会清缓存；等 TTL 过期或手动删 `search_cache` 表 |

## 八、数据与隐私

- 本地模式下，任务、消息、记忆都只存在你自己的 `agent.db`，不上传任何第三方。
- `config.json`、`agent.db`、`napcat-data/` 都已在 `.gitignore` 中，不会被提交到仓库。
- 切换服务器时，拷走 `config.json` + `agent.db`（含 `-wal`/`-shm`）即可恢复数据；QQ 登录态在 NapCat 数据目录里。
