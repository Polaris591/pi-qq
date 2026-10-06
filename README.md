# pi-qq

把 [pi](https://github.com/earendil-works/pi) coding agent 接到 QQ 上的桥接。

基于 [NapCat](https://github.com/NapNeko/NapCatQQ)（OneBot 11），一个 QQ 号就能跑，
不需要开发者认证、不需要公网 IP。

## 它能做什么

- **私聊 / 群聊对话**，流式回复分段发送
- **群聊要 @ 才响应**，可自动带上群内最近 N 条消息作为背景
- **引用消息** —— 引用某条提问时，AI 能看到那条的内容
- **@ 识别** —— 知道 @ 的是谁（反查群名片/昵称）
- **两层长期记忆** —— 群公共记忆 + 每个成员的个人记忆，互不串味
- **文件收发** —— 用户发来的文件自动落盘，AI 要交付的文件写进 outbox 自动发回
- **定时任务** —— 用自然语言创建（「每天早上 8 点提醒我喝水」）
- **13 个内置命令** —— `/new` `/model` `/memory` `/task` `/restart` 等
- **卡死自愈** —— systemd 看门狗 + 单轮超时 + WS 自愈

## 架构

```
QQ 客户端 ←→ NapCat (Docker) ←→ bridge.js ←→ pi --mode rpc (每会话一个子进程)
                                    ↑
                              本进程，管会话/记忆/文件/定时任务
```

`bridge.js` 通过 OneBot 11 的 WebSocket 与 NapCat 通信，通过 RPC（JSONL over stdin/stdout）
驱动 pi 子进程。每个 QQ 会话（一个私聊 or 一个群）对应一个长期存活的 pi 进程。

## 前置依赖

跑起来需要三样东西，缺一不可：

**1. Node.js 18+**

```bash
node -v     # 确认版本
```

**2. pi CLI（本项目的核心，必须先装好）**

```bash
npm install -g @earendil-works/pi-coding-agent
pi --version    # 能输出版本号才算装好
```

装好后还要给 pi 配一个模型 provider（API key 之类），详见
[pi 的文档](https://github.com/earendil-works/pi)。**pi 自己不能对话的话，桥接也跑不起来。**

> 如果 pi 不在 PATH 里（比如装在别处），在 `config.json` 里把 `pi.bin`
> 改成它的完整路径，例如 `"/usr/local/bin/pi"`。

**3. NapCat（QQ 侧）**

推荐 Docker 跑：

```bash
docker run -d --name napcat \
  -p 3001:3001 \
  -v /path/to/napcat-data:/app/.config/QQ \
  mlikiowa/napcat-docker:latest
```

首次启动要用手机 QQ 扫码登录（容器里 `/app/qrcode.png`，或看容器日志）。
然后在 NapCat 的 WebUI 里**开启正向 WebSocket**，端口对上 `config.json` 里的 `napcat.url`。

- 可选：systemd（用看门狗和自动重启的话）

## 安装

```bash
git clone https://github.com/Polaris591/pi-qq.git
cd pi-qq
npm install                      # 只依赖 ws

cp config.example.json config.json
vim config.json                  # 至少填 napcat.url 和两个白名单
```

`config.json` 里留空的项会用安装目录下的相对路径，所以本地跑不用配路径。

启动：

```bash
node bridge.js
```

启动成功会看到：

```
连接 NapCat: ws://127.0.0.1:3001
outbox 监控: ...
pi-qq 桥接启动: napcat=ws://127.0.0.1:3001 maxSessions=5
NapCat WebSocket 已连接
登录账号: <你的QQ号> (<昵称>)
```

然后给这个 QQ 发条消息试试。**第一次收到消息时才会启动 pi 子进程**（会慢几秒）。

> 白名单是空的会启动失败吗？不会，但所有消息都会被忽略，日志里会有 ⚠️ 提示。
> 记得把自己 QQ 号填进 `access.privateWhitelist`。

用 systemd 托管（推荐，能拿到看门狗和自动重启）：

```ini
[Unit]
Description=pi-qq bridge
After=network-online.target docker.service
Requires=docker.service

[Service]
Type=notify
NotifyAccess=all
WatchdogSec=180
WorkingDirectory=/path/to/pi-qq
ExecStart=/usr/bin/node /path/to/pi-qq/bridge.js
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## 配置要点

```jsonc
{
  "napcat": {
    "url": "ws://127.0.0.1:3001",   // NapCat 的正向 WS 地址
    "token": ""                      // NapCat 配了 token 就填
  },
  "pi": {
    "provider": "anthropic",         // 留空用 pi 自己的默认
    "model": "claude-sonnet-4-5"
  },
  "access": {
    "privateWhitelist": ["123456789"],   // 谁能私聊
    "groupWhitelist": ["10001"],         // 哪个群能用
    "requireAtInGroup": true             // 群里必须 @
  },
  "persona": {
    "text": "你说话要短，说人话。",        // 性格，见下
    "groupBoundary": "不要透露主人的私事。"
  }
}
```

### 性格怎么调

`persona.text` 是给模型的**行为约束**，不是人设描述。区别很大：

- ❌ 「你是一个温柔可爱的助手」—— 模型基本无视
- ✅ 「不许用『好的』开头」「不确定就直说不确定」—— 这种才真管用

`persona.groupBoundary` 只在群聊生效，用来限制「什么能说」（不泄露主人私事等），
不改变说话风格。

改完要重启才生效（`/restart` 或 `systemctl restart`）。

### 记忆

```
memory/
├── private_<QQ>.md                 # 私聊记忆
└── group_<群号>/
    ├── shared.md                   # 群公共记忆（全群共享）
    └── members/<QQ号>.md            # 成员个人记忆（一人一份）
```

谁发言就把谁的个人记忆附在那条消息前面，所以不会串味。

## 内置命令

| 命令 | 作用 |
|---|---|
| `/new` | 开新会话（清空上下文） |
| `/resume` | 恢复历史会话 |
| `/model` | 切换模型 |
| `/thinking` | 设置思考等级 |
| `/stats` | token 用量与上下文占用 |
| `/compact` | 压缩上下文 |
| `/memory` | 查看长期记忆 |
| `/task` | 查看定时任务 |
| `/reset` | 重启 pi 进程（保留会话） |
| `/restart` | 重启整个桥接（重新加载代码与配置，仅限主人私聊） |
| `/stop` | 中断当前任务 |
| `/status` | 会话状态 |
| `/help` | 帮助 |

## 文件交付

AI 把要给你的文件写进 `outbox/`，桥接 2 秒内自动发到 QQ：

- 放 `outbox/` 根目录 → 发回当前对话
- 放 `outbox/<群号>/` 或 `outbox/<QQ号>/` → 发给对应会话

用户发来的文件落在 `inbox/`。

## 定时任务

在 `tasks/` 写一个 JSON 文件，桥接自动加载：

```json
{
  "id": "morning-news",
  "target": "private_123456789",
  "schedule": { "type": "daily", "time": "08:00" },
  "prompt": "搜索今天的 AI 新闻，用 3 条要点总结。",
  "enabled": true
}
```

`schedule.type` 支持 `daily` / `weekly`（配 `weekdays`，0=周日）/ `every`（配 `minutes`）/ `once`（配 `at`）。
时间按服务器时区。

也可以直接跟 AI 说「每天早上 8 点提醒我喝水」，它会自己写这个文件。

## 测试

```bash
node utest.js
```

纯函数级测试（202 条），不联网、不启 pi。

## 已知限制

- **语音**：只显示 `[语音]`，内容拿不到（需要接 ASR 服务）
- **视频**：只显示 `[视频]`
- **Markdown**：QQ 客户端协议不支持普通账号发 markdown 消息段（需要官方 Bot），
  所以桥接会把 Markdown 降级成纯文本
- **群里多人同时问**：串行排队，不是并行
- **封号风险**：NapCat 走的是客户端协议，理论上存在风险

## 许可证

MIT
