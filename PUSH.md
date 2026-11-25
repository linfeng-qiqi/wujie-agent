# 发布到 GitHub（给你自己的操作清单）

发布目录已收拾干净、已 `git init` 并完成首次提交（分支 `main`）。你只需要：

## 1. 在 GitHub 建空仓库

打开 https://github.com/new：

- Repository name: `wujie-agent`（或你喜欢的名字）
- Description: `不用装 App 的跨平台 Agent 插件：QQ/企微/钉钉即插即用，本地模型 + 自家联网搜索`
- 选 **Public**
- **不要**勾选 "Add a README / .gitignore / license"（我们已经有了）

## 2. 推送

在 `wujie-agent` 目录里执行（把 `你的用户名` 换成你的 GitHub 名）：

```bash
cd wujie-agent
git remote add origin https://github.com/你的用户名/wujie-agent.git
git branch -M main
git push -u origin main
```

如果提示登录，用 GitHub Personal Access Token 当密码（Settings → Developer settings → Personal access tokens 生成，勾 repo 权限）。

## 3. 发布后建议

- 仓库页点一下 **⭐ Star**（自己的项目先占个坑）
- Settings → Features 里打开 **Issues**，方便别人反馈
- 在 About 里加上标签：`python` `agent` `ollama` `chatbot` `local-llm` `cross-platform`

## 改名（可选）

想把「无界」换成别的名字，一条命令全仓库替换：

```bash
# Linux / Mac
grep -rl '无界\|Wujie\|wujie\|WUJIE' . --exclude-dir=.git | xargs sed -i 's/无界/新中文名/g; s/Wujie/NewName/g; s/wujie/newname/g; s/WUJIE/NEWNAME/g'

# 然后重新提交
git add -A && git commit -m "Rename project"
```

## 已做的隐私处理（确认无需再改）

- 真实 API 密钥、两个 QQ 号、服务器 IP、WebUI token —— 全部移除
- 数据库（含真实任务/消息）—— 不带入仓库，运行时自动建空库
- 二维码图片、聊天数据、备份 zip、工作日志 —— 全部排除
- 路径全部改成基于脚本位置 + 环境变量可覆盖，clone 后能直接跑
