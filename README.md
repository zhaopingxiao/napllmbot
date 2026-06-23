<div align="center">

# Napllmbot

_基于 [NapCat](https://github.com/NapNeko/NapCatQQ) WebSocket + DeepSeek API 的 QQ 群机器人。支持 AI 对话、定时任务、自定义功能模块、新成员欢迎等。_

</div>

## 快速开始

### 1. 环境要求

- Python 3.10+
- [NapCat](https://github.com/NapNeko/NapCatQQ) 已配置并连接到 QQ

### 2. 安装

```bash
git clone https://github.com/zhaopingxiao/napllmbot.git
cd napllmbot
pip install openai napcat
```

### 3. 配置

```bash
cp config.example.json config.json
```

编辑 `config.json`：

```json
{
  "ai": {
    "api_key": "sk-your-deepseek-key",
    "base_url": "https://api.deepseek.com",
    "model": "deepseek-v4-pro"
  },
  "bot": {
    "name": "小机器人儿",
    "host": "0.0.0.0",
    "port": 8080,
    "group_id": 1104983768,
    "max_tool_calls": 10
  },
  "web": {
    "port": 8100,
    "password": "your-web-password"
  }
}
```

| 字段 | 说明 |
|------|------|
| `ai.api_key` | DeepSeek API Key |
| `ai.base_url` | API 地址（可换成其他 OpenAI 兼容接口） |
| `ai.model` | 模型名称 |
| `bot.name` | 机器人名字（上线消息、网页标题等） |
| `bot.host` / `bot.port` | NapCat 反向 WebSocket 连接地址 |
| `bot.group_id` | 监听的 QQ 群号 |
| `bot.max_tool_calls` | 每轮对话最多工具调用次数 |
| `web.port` | Web 管理面板端口 |
| `web.password` | Web 面板登录密码 |

### 4. 运行

```bash
python main.py
```

NapCat 反向 WebSocket 连接到 `ws://host:port` 后机器人自动上线。

## 功能

### AI 对话

- 群内 @机器人 触发对话
- 支持多轮对话历史记忆
- 工具调用（tool calling），最多 10 轮

### 内置工具

| 工具 | 说明 |
|------|------|
| `get_time` | 获取当前时间 |
| `get_group_members` | 获取群成员列表（QQ号、昵称、身份） |
| `add_scheduled_task` | 添加定时任务 |
| `remove_scheduled_task` | 删除定时任务 |
| `list_scheduled_tasks` | 列出所有定时任务 |
| `list_do` | 列出可用自定义功能 |
| `do_specific` | 查看某自定义功能的参数 |
| `start_do` | 执行自定义功能 |

### 定时任务

支持灵活的定时规则：

- **定时触发**：每天 8:30 → `time_type=specific, hour=8, minute=30`
- **间隔触发**：每 30 分钟 → `time_type=interval_m, interval_m=30`
- **日期约束**：每月 15 日、每周一等

AI 可使用 `add_scheduled_task` 工具帮用户创建定时任务。

### 艾特（@某人）

AI 在回复中可艾特别人，格式：

```
[CQ:at,qq=QQ号] 要说的话
```

AI 可通过 `get_group_members` 查找 QQ 号后正确艾特。

### 新成员欢迎

指定账号（`2854196310`）发送 `(Newusr)` 并艾特新人后，机器人自动发送欢迎语。

### Web 管理面板

浏览器访问 `http://localhost:8100` 可查看对话记录。

### /s\ 分条 / \<media> 文件

- 用 `/s\` 分割可发送多条消息
- 用 `<media>文件路径</media>` 可上传文件到群

## 自定义功能模块（do/）

> ⚠️ `do/` 目录不在仓库中，由用户自行创建。

在 `do/` 下新建目录，放入 `main.py`，定义三个变量即可被机器人识别：

```python
# do/example/main.py

do_description = "功能描述（会显示在 list_do 中）"

properties = {
    "type": "object",
    "properties": {
        "param1": {"type": "string", "description": "参数说明"},
    },
    "required": ["param1"]
}

async def start_do(param1: str):
    """执行逻辑"""
    return f"结果: {param1}"
```

## 插件系统（plugins/）

在 `plugins/` 下放 `.py` 文件即可自动加载。插件有两个生命周期钩子：

```python
# plugins/my_plugin.py

plugin_name = "我的插件"

_send = None   # 全局存 send 函数
_chat = None   # 全局存 chat 函数

async def get_function(send, chat):
    """启动时调用，传入 send / chat，存到全局变量。"""
    global _send, _chat
    _send = send
    _chat = chat

async def on_message(data):
    """收到每条群消息时调用。返回 str 则作为回复，None 则放行给 AI。"""
    if data["message"] == "ping":
        await _send("pong")
    return None
```

### data 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `user_id` | int | 发送者 QQ |
| `user_name` | str | 昵称 / 群名片 |
| `message` | str | 消息文本（艾特已转 @name(id)） |
| `group_id` | int | 群号 |
| `group_name` | str | 群名 |
| `at_list` | list[str] | 被艾特的 QQ 号列表 |
| `at_bot` | bool | 本条是否艾特了机器人 |
| `last_ai_ago` | int | 距 AI 上条非 NO_REPLY 消息的条数 |

### API 函数

| API | 说明 |
|------|------|
| `await send(text)` | 直接发消息到群，支持 `[CQ:at,qq=xxx]` 和 `<media>文件</media>` |
| `await chat(要求)` | 让 AI 基于完整对话历史 + 要求生成消息（走 tool-calling） |

### NO_REPLY

AI 或插件返回 `NO_REPLY` 时静默跳过，不发送任何内容。

### 配置

```json
"plugins": { "enabled": true }   // false 禁用所有插件
```

## 项目结构

```
.
├── main.py              # 全部代码
├── config.example.json  # 配置模板
├── config.json          # 实际配置（gitignore）
├── messages.json        # 对话历史（gitignore）
├── scheduled_tasks.json # 定时任务（gitignore）
├── plugins/             # 插件目录
│   └── example_blood_test.py
├── do/                  # 自定义功能模块（gitignore）
├── README.md
└── .gitignore
```

## License

MIT
