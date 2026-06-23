import asyncio
import os
import re
import importlib.util
import json
import time
import threading
from dataclasses import dataclass, asdict
from datetime import datetime, time as dtime, date, timedelta
from urllib.parse import parse_qs, urlparse
import http.server
import secrets
import uuid

from napcat import ReverseWebSocketServer, NapCatClient, GroupMessageEvent
from napcat.types.messages.generated import At, Text as MsgText
from openai import OpenAI


# ============ 配置加载 ============

def _load_config():
    """加载 config.json，不存在则尝试 config.example.json。"""
    for fname in ("config.json", "config.example.json"):
        if os.path.isfile(fname):
            with open(fname, "r", encoding="utf-8") as f:
                return json.load(f)
    raise FileNotFoundError("找不到 config.json 或 config.example.json，请创建配置文件。")


_config = _load_config()

ai_client = OpenAI(
    api_key=_config["ai"]["api_key"],
    base_url=_config["ai"]["base_url"]
)
AI_MODEL = _config["ai"]["model"]

BOT_HOST = _config["bot"]["host"]
BOT_PORT = _config["bot"]["port"]
BOT_GROUP_ID = _config["bot"]["group_id"]
BOT_NAME = _config["bot"]["name"]
MAX_TOOL_CALLS = _config["bot"]["max_tool_calls"]

WEB_PORT = _config["web"]["port"]
WEB_PASSWORD = _config["web"]["password"]

PLUGINS_ENABLED = _config.get("plugins", {}).get("enabled", True)


# ============ 定时任务 ============

_tasks_lock = threading.Lock()
TASKS_FILE = "scheduled_tasks.json"


@dataclass
class ScheduledTask:
    id: str
    name: str
    enabled: bool = True
    month: str = "*"
    day: str = "*"
    weekday: str = ""
    time_type: str = "specific"
    hour: str = "0"
    minute: str = "0"
    interval_m: int = 0
    prompt: str = ""
    created_at: str = ""
    last_run: str = ""


def load_tasks() -> list[ScheduledTask]:
    with _tasks_lock:
        try:
            with open(TASKS_FILE, "rt", encoding="utf-8") as f:
                return [ScheduledTask(**item) for item in json.load(f)]
        except (FileNotFoundError, json.JSONDecodeError):
            return []


def save_tasks(tasks: list[ScheduledTask]):
    with _tasks_lock:
        with open(TASKS_FILE, "wt", encoding="utf-8") as f:
            json.dump([asdict(t) for t in tasks], f, ensure_ascii=False, indent=2)


def _update_task_last_run(task_id: str, last_run: str):
    tasks = load_tasks()
    for t in tasks:
        if t.id == task_id:
            t.last_run = last_run
            break
    save_tasks(tasks)


def _get_times_for_day(task: ScheduledTask) -> list[dtime]:
    if task.time_type == "specific":
        h_val = task.hour
        m_val = int(task.minute)
        return [dtime(h, m_val) for h in range(24)] if h_val == "*" else [dtime(int(h_val), m_val)]
    if task.time_type == "interval_m":
        interval = task.interval_m or 1
        if interval <= 0:
            return []
        result = []
        total = 0
        while total < 24 * 60:
            h, m = divmod(total, 60)
            result.append(dtime(h, m))
            total += interval
        return result
    return []


def _date_matches(d: date, task: ScheduledTask) -> bool:
    if task.weekday:
        try:
            return d.weekday() == int(task.weekday)
        except (ValueError, TypeError):
            return False
    month_ok = (task.month == "*") or (d.month == _safe_int(task.month))
    day_ok = (task.day == "*") or (d.day == _safe_int(task.day))
    return month_ok and day_ok


def _safe_int(s: str) -> int:
    try:
        return int(s)
    except (ValueError, TypeError):
        return -1


def calculate_next_run(task: ScheduledTask, now: datetime | None = None) -> datetime | None:
    if now is None:
        now = datetime.now()
    today = now.date()
    candidate_times = _get_times_for_day(task)
    if not candidate_times:
        return None
    for day_offset in range(367):
        check_date = today + timedelta(days=day_offset)
        if not _date_matches(check_date, task):
            continue
        for t in candidate_times:
            dt = datetime.combine(check_date, t)
            if dt > now:
                return dt
    return None

def get_time():
    """获取当前日期和时间"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def add_scheduled_task(name: str, time_type: str, prompt: str,
                       month: str = "*", day: str = "*", weekday: str = "",
                       hour: str = "0", minute: str = "0",
                       interval_m: int = 0):
    """添加定时任务。time_type: specific=时分, interval_m=每隔X分钟。
    month/day: '*'=每, 或 '1'..'12'/'1'..'31'。weekday: ''=不启用, '0'..'6'。hour: '*'=每时。"""
    tasks = load_tasks()
    new_task = ScheduledTask(
        id=str(uuid.uuid4())[:8],
        name=name,
        month=month, day=day, weekday=weekday,
        time_type=time_type,
        hour=hour, minute=minute,
        interval_m=interval_m,
        prompt=prompt,
        created_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    tasks.append(new_task)
    save_tasks(tasks)
    next_run = calculate_next_run(new_task)
    next_str = next_run.strftime("%Y-%m-%d %H:%M:%S") if next_run else "无法计算"
    return f"定时任务已添加: {name} (ID: {new_task.id})，下次执行时间: {next_str}"


def remove_scheduled_task(id: str):
    """删除定时任务"""
    tasks = load_tasks()
    for t in tasks:
        if t.id == id:
            tasks.remove(t)
            save_tasks(tasks)
            return f"定时任务已删除: {t.name} (ID: {id})"
    return f"未找到定时任务 ID: {id}"


def list_scheduled_tasks():
    """列出所有定时任务"""
    tasks = load_tasks()
    if not tasks:
        return "当前没有定时任务。"
    lines = ["当前定时任务列表:"]
    for t in tasks:
        status = "✅启用" if t.enabled else "❌禁用"
        # 生成可读的规则描述
        if t.weekday:
            wn = ["周一","周二","周三","周四","周五","周六","周日"][int(t.weekday)]
            date_desc = f"每{wn}"
        elif t.month == "*" and t.day == "*":
            date_desc = "每天"
        elif t.month == "*":
            date_desc = f"每月{t.day}日"
        elif t.day == "*":
            date_desc = f"每年{t.month}月每天"
        else:
            date_desc = f"每年{t.month}月{t.day}日"

        if t.time_type == "specific":
            if t.hour == "*":
                time_desc = f"每时{t.minute}分"
            else:
                time_desc = f"{t.hour}时{t.minute}分"
        elif t.time_type == "interval_m":
            time_desc = f"每{t.interval_m}分"
        else:
            time_desc = t.time_type

        next_run = calculate_next_run(t)
        next_str = next_run.strftime("%Y-%m-%d %H:%M:%S") if next_run else "无法计算"
        lines.append(f"  [{t.id}] {t.name} ({date_desc} {time_desc}) - {status} - 下次: {next_str}")
    return "\n".join(lines)


DO_DIR = "do"
_bot_client = None


async def get_group_members():
    """获取本群所有成员列表。返回成员QQ号、昵称、群名片和身份。
    当需要艾特某人或查看群成员信息时调用。"""
    if not _bot_client:
        return "错误: 机器人客户端未就绪，请稍后重试。"
    try:
        members = await _bot_client.get_group_member_list(group_id=BOT_GROUP_ID, no_cache=True)
        lines = [f"群成员列表（共 {len(members)} 人）:", ""]
        for m in members:
            uid = m.get('user_id', '?')
            nick = m.get('nickname', '')
            card = m.get('card', '')
            role = m.get('role', 'member')
            role_map = {'owner': '👑群主', 'admin': '🔰管理', 'member': '👤成员'}
            role_str = role_map.get(role, role)
            display = card if card else nick
            lines.append(f"  [{uid}] {display}  ({role_str})")
        return "\n".join(lines)
    except Exception as e:
        return f"获取群成员失败: {e}"


def list_do():
    """列出所有自定义功能（do 目录下的子模块）"""
    if not os.path.isdir(DO_DIR):
        return "do 目录不存在，暂无自定义功能。"
    items = []
    for name in sorted(os.listdir(DO_DIR)):
        dir_path = os.path.join(DO_DIR, name)
        main_path = os.path.join(dir_path, "main.py")
        if os.path.isdir(dir_path) and os.path.isfile(main_path):
            try:
                spec = importlib.util.spec_from_file_location(f"do_{name}", main_path)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                desc = getattr(mod, "do_description", "(无描述)")
                items.append(f"  [{name}] {desc}")
            except Exception as e:
                items.append(f"  [{name}] (加载失败: {e})")
    if not items:
        return "暂无可用自定义功能。"
    return "可用自定义功能:\n" + "\n".join(items)


def do_specific(name: str):
    """获取指定自定义功能的参数定义"""
    main_path = os.path.join(DO_DIR, name, "main.py")
    if not os.path.isfile(main_path):
        return f"功能不存在: {name}"
    try:
        spec = importlib.util.spec_from_file_location(f"do_{name}", main_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        desc = getattr(mod, "do_description", "")
        props = getattr(mod, "properties", {})
        return json.dumps({"name": name, "description": desc, "properties": props},
                          ensure_ascii=False)
    except Exception as e:
        return f"加载失败: {e}"


async def start_do(name: str, **kwargs):
    """执行指定的自定义功能"""
    main_path = os.path.join(DO_DIR, name, "main.py")
    if not os.path.isfile(main_path):
        return f"功能不存在: {name}"
    try:
        spec = importlib.util.spec_from_file_location(f"do_{name}", main_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        fn = getattr(mod, "start_do", None)
        if not fn:
            return f"功能 {name} 没有 start_do 函数"
        if asyncio.iscoroutinefunction(fn):
            result = await fn(**kwargs)
        else:
            result = fn(**kwargs)
        return str(result)
    except Exception as e:
        return f"执行失败: {e}"


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_time",
            "description": "获取当前的日期和时间。当用户询问时间、日期时调用。",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "add_scheduled_task",
            "description": "添加一个新的定时任务。\n日期规则：每天→month='*' day='*'；每月15日→month='*' day='15'；每年3月15日→month='3' day='15'；每周一→weekday='0'。\n时间规则：8点30分→time_type='specific' hour='8' minute='30'；每时30分→time_type='specific' hour='*' minute='30'；每30分钟→time_type='interval_m' interval_m=30；每60分钟(即每1小时)→time_type='interval_m' interval_m=60。",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "任务名称"},
                    "time_type": {"type": "string", "enum": ["specific", "interval_m"],
                                  "description": "时间类型: specific=时分, interval_m=每隔X分钟"},
                    "prompt": {"type": "string", "description": "触发时发送给AI的提示词"},
                    "month": {"type": "string", "description": "月份: '*'=每月, '1'~'12'=指定月。默认'*'"},
                    "day": {"type": "string", "description": "日期: '*'=每日, '1'~'31'=指定日。默认'*'"},
                    "weekday": {"type": "string", "description": "周几: ''=不启用, '0'=周一...'6'=周日。设置后month/day无效"},
                    "hour": {"type": "string", "description": "小时: '*'=每时, '0'~'23'=指定时。time_type=specific时有效"},
                    "minute": {"type": "string", "description": "分钟: '0'~'59'。time_type=specific时有效"},
                    "interval_m": {"type": "integer", "description": "间隔分钟数，time_type=interval_m时必填。如每1小时=60, 每2小时=120"}
                },
                "required": ["name", "time_type", "prompt"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "remove_scheduled_task",
            "description": "删除一个定时任务。当用户要求取消、删除、移除定时任务时调用。需要先通过list_scheduled_tasks获取任务ID。",
            "parameters": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "要删除的任务ID"}
                },
                "required": ["id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_scheduled_tasks",
            "description": "列出当前所有定时任务。当用户询问有哪些定时任务、查看定时任务列表、查看定时任务状态时调用。",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_group_members",
            "description": "获取本群的成员列表，包括每个成员的QQ号、昵称、群名片和身份（群主/管理/成员）。当需要艾特某人、查看谁在群里、根据名字查找QQ号时调用。",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_do",
            "description": "列出所有可用的自定义功能（do目录下的插件）。返回功能名称和描述。",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "do_specific",
            "description": "获取指定自定义功能的参数定义。先调用list_do查看有哪些功能，再用本工具获取某功能的参数。",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "功能名称，如'html-to-image'"}
                },
                "required": ["name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "start_do",
            "description": "执行指定的自定义功能。先调用do_specific获取参数定义，再调用本工具执行。参数中name指定功能名，其余参数传给该功能。",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "功能名称，如'html-to-image'"}
                },
                "required": ["name"]
            }
        }
    }
]

AVAILABLE_FUNCTIONS = {
    "get_time": get_time,
    "get_group_members": get_group_members,
    "add_scheduled_task": add_scheduled_task,
    "remove_scheduled_task": remove_scheduled_task,
    "list_scheduled_tasks": list_scheduled_tasks,
    "list_do": list_do,
    "do_specific": do_specific,
    "start_do": start_do,
}
WEB_SESSIONS = set()
_last_ai_msg_time = 0     # 上次 AI 真实发言的时间戳（备用）
_msg_count_since_ai = 0   # 距离 AI 上条真实消息的条数（NO_REPLY 不归零）


async def get_name_in_group(client: NapCatClient, group_id: int, user_id: int):
    try:
        member_info = await client.get_group_member_info(group_id=group_id, user_id=user_id)
        return member_info.get('card', member_info.get('nickname', str(user_id)))
    except Exception as e:
        print(f"获取用户 {user_id} 信息失败: {e}")
        return str(user_id)

async def get_at_list(message: str):
    return re.findall(r"\[CQ:at,qq=(\d+)\]", message)


def _parse_cq_at(text: str):
    """将文本中的 [CQ:at,qq=xxx] 转为 NapCat 结构化消息段列表。
    不含 CQ at 码则返回原字符串，否则返回 list[At | Text]。
    """
    if not re.search(r'\[CQ:at,qq=\d+\]', text):
        return text

    segments = []
    parts = re.split(r'\[CQ:at,qq=(\d+)\]', text)
    for i, part in enumerate(parts):
        if i % 2 == 0:
            if part:
                segments.append(MsgText(text=part))
        else:
            segments.append(At(qq=part))
    return segments


def _base_prompt_suffix() -> str:
    """返回所有提示词公用的尾部（艾特方法 + 可选系统 prompt + NO_REPLY 说明）。"""
    s = """

# 艾特（@某人）的方法
如果你在回复中需要艾特某个人（例如提到他、叫他看、回应他），使用以下格式：
[CQ:at,qq=QQ号] 要跟他说的话
注意: [CQ:at,qq=QQ号] 和后面文字之间必须有一个空格。
例如: [CQ:at,qq=123456789] 快来看这个！
你可以先调用 get_group_members 获取群成员列表，根据昵称/群名片找到对应的QQ号，然后用 [CQ:at,qq=QQ号] 格式艾特他。

# 不需要回复时
如果你判断当前消息完全没必要回复（例如对方只是随口一说、语气词、无意义内容等），直接回复 NO_REPLY（纯英文大写，不要带任何其他文字）。但尽可能回复每一条消息，只有在确实没必要回复时才使用 NO_REPLY。"""
    return s


def _is_no_reply(content: str) -> bool:
    """判断 AI 是否返回了 NO_REPLY（不回复标记）。"""
    return content.strip().upper() == "NO_REPLY"


def _segments_to_text(msg) -> str:
    """将结构化消息段转回纯文本（At → @qq号），用于出错降级。"""
    if isinstance(msg, str):
        return msg
    parts = []
    for seg in msg:
        if hasattr(seg, 'qq') and getattr(seg, '_type', '') == 'at':
            parts.append(f"@{getattr(seg, 'qq', '')}")
        elif hasattr(seg, 'text'):
            parts.append(getattr(seg, 'text', ''))
    return ''.join(parts)


async def _send_with_media(client, group_id: int, content: str, reply_to=None):
    """发送消息，解析 <media>路径</media> 自动上传文件。
    例: "你好<media>/tmp/a.png</media>再见" → 发"你好" → 上传文件 → 发"再见"
    """
    parts = re.split(r"<media>(.*?)</media>", content)
    for i, part in enumerate(parts):
        part = part.strip()
        if not part:
            continue
        if i % 2 == 0:
            # 文本段 → 解析 CQ at 码为结构化消息段
            msg = _parse_cq_at(part)
            try:
                if reply_to and i == 0:
                    await reply_to.reply(msg)
                else:
                    await client.send_group_msg(group_id=group_id, message=msg)
            except Exception:
                # 结构化 At 发送失败（如 QQ 号不在群内），降级为纯文本 @xxx
                fallback = _segments_to_text(msg)
                if reply_to and i == 0:
                    await reply_to.reply(fallback)
                else:
                    await client.send_group_msg(group_id=group_id, message=fallback)
        else:
            # 文件路径
            try:
                import os as _os
                fname = _os.path.basename(part)
                await client.upload_group_file(group_id=group_id, file=part, name=fname)
            except Exception as e:
                await client.send_group_msg(group_id=group_id, message=f"[文件上传失败] {part}: {e}")
        time.sleep(1)


# ============ 定时任务执行 ============

async def execute_task(client, task: ScheduledTask, messages: list,
                       all_tools: list, all_functions: dict):
    """执行单个定时任务。"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user_prompt = f"""# 定时任务触发
当前时间: {now_str}
任务名称: {task.name}

{task.prompt}

请根据以上内容执行任务。如果不需要回复，直接回复 NO_REPLY。回复若分多条消息，用 /s\\ 分割。如需发送文件，用 <media>文件完整路径</media> 标记。"""

    messages.append({"role": "user", "content": user_prompt})

    try:
        tool_call_count = 0
        while True:
            response = ai_client.chat.completions.create(
                model=AI_MODEL,
                messages=messages,
                tools=all_tools,
                tool_choice="auto"
            )
            response_message = response.choices[0].message

            if response_message.tool_calls:
                tool_call_count += len(response_message.tool_calls)
                messages.append(response_message.to_dict())

                for tc in response_message.tool_calls:
                    fn = all_functions.get(tc.function.name)
                    try:
                        if fn:
                            args = json.loads(tc.function.arguments) if tc.function.arguments else {}
                            result = await fn(**args) if asyncio.iscoroutinefunction(fn) else fn(**args)
                        else:
                            result = f"[未知工具] {tc.function.name}"
                    except Exception as e:
                        result = f"[工具调用出错] {e}"
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result
                    })

                if tool_call_count >= MAX_TOOL_CALLS:
                    messages.append({
                        "role": "user",
                        "content": "已达到工具调用次数上限，请直接基于已有信息回复。如有多个句子请用/s\\分割。如需发送文件用 <media>完整路径</media> 标记。"
                    })
                    response = ai_client.chat.completions.create(
                        model=AI_MODEL,
                        messages=messages,
                    )
                    response_message = response.choices[0].message
                    break
            else:
                break

        messages.append(response_message.to_dict())

        try:
            with open("messages.json", "wt", encoding="utf-8") as f:
                json.dump(messages, f, ensure_ascii=False)
        except Exception as e:
            print(f"[定时任务] 保存对话记忆失败: {e}")

        if not _is_no_reply(response_message.content):
            global _msg_count_since_ai
            _msg_count_since_ai = 0
            parts = response_message.content.split("/s\\")
            for v in parts:
                v = v.strip()
                if not v:
                    continue
                await _send_with_media(client, BOT_GROUP_ID, v)
                if len(parts) > 1:
                    time.sleep(1)

    except Exception as e:
        print(f"[定时任务] 执行 '{task.name}' 失败: {e}")


async def scheduler_loop(client, messages: list,
                         all_tools: list, all_functions: dict):
    """后台调度循环，每 30 秒检查到期任务。"""

    while True:
        try:
            tasks = load_tasks()
            now = datetime.now()

            for task in tasks:
                if not task.enabled:
                    continue

                next_run = calculate_next_run(task, now)
                if next_run is None:
                    continue

                diff = (next_run - now).total_seconds()
                if diff > 30 or diff < -30:
                    continue

                slot_key = next_run.replace(second=0, microsecond=0)
                already_fired = False
                if task.last_run:
                    try:
                        last_dt = datetime.fromisoformat(task.last_run)
                        if last_dt.replace(second=0, microsecond=0) >= slot_key:
                            already_fired = True
                    except (ValueError, TypeError):
                        pass

                if already_fired:
                    continue

                print(f"[定时任务] 触发: {task.name} @ {now.strftime('%H:%M:%S')}")
                await execute_task(client, task, messages, all_tools, all_functions)
                _update_task_last_run(task.id, now.isoformat())

        except Exception as e:
            print(f"[定时任务] 调度异常: {e}")

        await asyncio.sleep(30)


# ============ 插件系统 ============

PLUGINS_DIR = "plugins"


def load_plugins():
    """加载 plugins/ 目录下所有 .py 文件，返回 [(name, on_message_fn), ...]"""
    plugins = []
    if not os.path.isdir(PLUGINS_DIR):
        return plugins
    for fname in sorted(os.listdir(PLUGINS_DIR)):
        if fname.startswith("_") or not fname.endswith(".py"):
            continue
        path = os.path.join(PLUGINS_DIR, fname)
        try:
            spec = importlib.util.spec_from_file_location(f"plugin_{fname[:-3]}", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            fn = getattr(mod, "on_message", None)
            if fn:
                name = getattr(mod, "plugin_name", fname)
                plugins.append((name, fn))
                print(f"[插件] 已加载: {name}")
        except Exception as e:
            print(f"[插件] 加载失败 {fname}: {e}")
    return plugins


async def init_plugins(plugins, send, chat):
    """调用各插件的 get_function(send, chat)（若存在），让插件存储函数引用。"""
    if not os.path.isdir(PLUGINS_DIR):
        return
    for fname in sorted(os.listdir(PLUGINS_DIR)):
        if fname.startswith("_") or not fname.endswith(".py"):
            continue
        path = os.path.join(PLUGINS_DIR, fname)
        try:
            spec = importlib.util.spec_from_file_location(f"plugin_{fname[:-3]}", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            gf = getattr(mod, "get_function", None)
            if gf:
                await gf(send, chat) if asyncio.iscoroutinefunction(gf) else gf(send, chat)
        except Exception as e:
            print(f"[插件] get_function 失败 {fname}: {e}")


async def run_plugins(plugins, data):
    """依次执行插件 on_message(data)，返回第一个非空结果。"""
    for name, fn in plugins:
        try:
            result = fn(data)
            if asyncio.iscoroutinefunction(fn):
                result = await result
            if result:
                return str(result)
        except Exception as e:
            print(f"[插件] {name} 执行异常: {e}")
    return None


async def on_bot_connected(client: NapCatClient):
    global _bot_client, _msg_count_since_ai
    _bot_client = client
    history = []
    try:
        with open("messages.json", "rt", encoding="utf-8") as f:
            messages = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        messages = []
    bot_qq = client.self_id
    print(f"机器人 {bot_qq} 已上线，开始监听消息...")
    await client.send_group_msg(group_id=BOT_GROUP_ID, message=f"{BOT_NAME}已上线！")

    # 启动定时任务调度器（后台 asyncio Task）
    scheduler_task = asyncio.create_task(
        scheduler_loop(client, messages, TOOLS, AVAILABLE_FUNCTIONS)
    )
    print("⏰ 定时任务调度器已启动")

    # 加载插件
    plugins = load_plugins() if PLUGINS_ENABLED else []

    async def _plugin_send(text: str):
        """send(text) — 插件直接发消息到群，支持艾特和文件。"""
        await _send_with_media(client, BOT_GROUP_ID, text)

    async def _plugin_chat(requirement: str):
        """chat(要求) — 让 AI 基于完整对话历史 + 要求 生成消息并发群。"""
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        chat_prompt = f"""# 新的对话历史
{chr(10).join(f"{h['user_name']}({h['user_id']}): {h['message']}" for h in history)}
# 轮到你发言了
请你根据以前的对话历史和本次新的对话历史发送一条消息：{requirement}
当前时间: {now_str}
如果内容有很多句，就尽可能分条回复，用/s\分割。
如需发送文件（如PPT、图片、文档等），在回复中用 <media>文件完整路径</media> 标记，例如：这是你要的文件<media>/data/report.pptx</media>请查收。
"""
        chat_prompt += _base_prompt_suffix()
        messages.append({"role": "user", "content": chat_prompt})

        tool_call_count = 0
        while True:
            response = ai_client.chat.completions.create(
                model=AI_MODEL, messages=messages,
                tools=TOOLS, tool_choice="auto"
            )
            response_message = response.choices[0].message

            if response_message.tool_calls:
                tool_call_count += len(response_message.tool_calls)
                messages.append(response_message.to_dict())
                for tc in response_message.tool_calls:
                    fn = AVAILABLE_FUNCTIONS.get(tc.function.name)
                    try:
                        if fn:
                            args = json.loads(tc.function.arguments) if tc.function.arguments else {}
                            result = await fn(**args) if asyncio.iscoroutinefunction(fn) else fn(**args)
                        else:
                            result = f"[未知工具] {tc.function.name}"
                    except Exception as e:
                        result = f"[工具调用出错] {e}"
                    messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})

                if tool_call_count >= MAX_TOOL_CALLS:
                    messages.append({"role": "user", "content": "已达到工具调用次数上限，请直接基于已有信息回复。如有多个句子请用/s\分割。如需发送文件用 <media>完整路径</media> 标记。"})
                    response = ai_client.chat.completions.create(model=AI_MODEL, messages=messages)
                    response_message = response.choices[0].message
                    break
            else:
                break

        messages.append(response_message.to_dict())
        with open("messages.json", "wt", encoding="utf-8") as f:
            json.dump(messages, f, ensure_ascii=False)

        if not _is_no_reply(response_message.content):
            global _msg_count_since_ai
            _msg_count_since_ai = 0
            for v in response_message.content.split("/s\\"):
                v = v.strip()
                if not v:
                    continue
                await _send_with_media(client, BOT_GROUP_ID, v)
                time.sleep(1)

    # 初始化插件：传递 send/chat 函数
    await init_plugins(plugins, _plugin_send, _plugin_chat)
    print(f"🔌 插件: {'已启用' if PLUGINS_ENABLED else '已禁用'}，共 {len(plugins)} 个")

    async for event in client:
        if isinstance(event, GroupMessageEvent) and event.group_id == BOT_GROUP_ID:
            group_id = event.group_id
            group_name = event.group_name
            user_id = event.user_id
            user_name = await get_name_in_group(client, group_id, user_id)
            message = event.raw_message
            at_list = await get_at_list(message)
            for qid in at_list:
                name = await get_name_in_group(client, group_id, qid)
                message = message.replace(f"[CQ:at,qq={qid}]", f"@{name}({qid})")

            print("user_id", user_id, "user_name", user_name, "message", message, "at_list", at_list)

            history.append({"group_id": group_id, "group_name": group_name, "user_id": user_id, "user_name": user_name, "message": message, "at_list": at_list})

            _msg_count_since_ai += 1

            # ── 插件消息拦截 ──
            if plugins:
                plugin_data = {
                    "user_id": user_id, "user_name": user_name,
                    "message": message, "group_id": group_id,
                    "group_name": group_name, "at_list": at_list,
                    "last_ai_ago": _msg_count_since_ai,
                    "at_bot": str(bot_qq) in at_list,
                }
                plugin_resp = await run_plugins(plugins, plugin_data)
                if plugin_resp and not _is_no_reply(plugin_resp):
                    _msg_count_since_ai = 0
                    await _plugin_send(plugin_resp)
                    history = []
                    continue

            # ── 新成员进群欢迎 ──
            if user_id == 2854196310 and "(Newusr)" in message and at_list:
                new_qq = at_list[0]
                new_name = await get_name_in_group(client, group_id, new_qq)
                welcome_prompt = f"""# 有新成员进群
{user_name}({user_id}) 通知: @{new_name}({new_qq}) 加入了群聊，请你说一段欢迎语，
欢迎语要求：热情、有群特色，可以适当活泼，把新人融入进群里来，内容适中不废话，艾特新人。
如果内容有很多句，就尽可能分条回复，用/s\分割。
如需发送文件（如PPT、图片、文档等），在回复中用 <media>文件完整路径</media> 标记，例如：这是你要的文件<media>/data/report.pptx</media>请查收。

# 艾特（@某人）的方法
如果你在回复中需要艾特新人，使用以下格式：
[CQ:at,qq={new_qq}] 要跟他说的话
注意: [CQ:at,qq=QQ号] 和后面文字之间必须有一个空格。
"""
                messages.append({"role": "user", "content": welcome_prompt})

                # ---- Tool calling loop ----
                tool_call_count = 0
                while True:
                    response = ai_client.chat.completions.create(
                        model=AI_MODEL,
                        messages=messages,
                        tools=TOOLS,
                        tool_choice="auto"
                    )
                    response_message = response.choices[0].message

                    if response_message.tool_calls:
                        tool_call_count += len(response_message.tool_calls)
                        messages.append(response_message.to_dict())

                        for tc in response_message.tool_calls:
                            fn = AVAILABLE_FUNCTIONS.get(tc.function.name)
                            try:
                                if fn:
                                    args = json.loads(tc.function.arguments) if tc.function.arguments else {}
                                    if asyncio.iscoroutinefunction(fn):
                                        result = await fn(**args)
                                    else:
                                        result = fn(**args)
                                else:
                                    result = f"[未知工具] {tc.function.name}"
                            except Exception as e:
                                result = f"[工具调用出错] {e}"
                            messages.append({
                                "role": "tool",
                                "tool_call_id": tc.id,
                                "content": result
                            })

                        if tool_call_count >= MAX_TOOL_CALLS:
                            messages.append({
                                "role": "user",
                                "content": "已达到工具调用次数上限，请直接基于已有信息回复。如有多个句子请用/s\分割。如需发送文件用 <media>完整路径</media> 标记。"
                            })
                            response = ai_client.chat.completions.create(
                                model=AI_MODEL,
                                messages=messages,
                            )
                            response_message = response.choices[0].message
                            break
                    else:
                        break

                # 保存 + 发送
                messages.append(response_message.to_dict())
                with open("messages.json", "wt", encoding="utf-8") as f:
                    json.dump(messages, f)
                if not _is_no_reply(response_message.content):
                    _msg_count_since_ai = 0
                    end_parts = response_message.content.split("/s\\")
                    first_part = True
                    for v in end_parts:
                        v = v.strip()
                        if not v:
                            continue
                        if first_part:
                            await _send_with_media(client, group_id, v, reply_to=event)
                            first_part = False
                        else:
                            await _send_with_media(client, group_id, v)
                        time.sleep(1)

                history = []
                continue

            if str(bot_qq) in at_list:
                tishi_text = "# 新的对话历史\n"
                for v in history:
                    tishi_text += f"{v['user_name']}({v['user_id']}): {v['message']}\n"
                tishi_text += f"""# 轮到你发言了
{user_name}({user_id})艾特了你，
请你根据以前向你发送的对话历史和本次新的对话历史回复他，如果没有以前对话历史，就直接回复他的问题，或打个招呼，
内容适中，不特别简短，但不特别长，
不带其他任何提示语，只说你要回复的内容，
如果内容有很多句，就尽可能分条回复，用/s\分割。
如需发送文件（如PPT、图片、文档等），在回复中用 <media>文件完整路径</media> 标记，例如：这是你要的文件<media>/data/report.pptx</media>请查收。
"""
                tishi_text += _base_prompt_suffix()
                messages.append({"role": "user", "content": tishi_text})

                # ---- Tool calling loop ----
                tool_call_count = 0
                while True:
                    response = ai_client.chat.completions.create(
                        model=AI_MODEL,
                        messages=messages,
                        tools=TOOLS,
                        tool_choice="auto"
                    )
                    response_message = response.choices[0].message

                    if response_message.tool_calls:
                        tool_call_count += len(response_message.tool_calls)
                        messages.append(response_message.to_dict())

                        for tc in response_message.tool_calls:
                            fn = AVAILABLE_FUNCTIONS.get(tc.function.name)
                            try:
                                if fn:
                                    args = json.loads(tc.function.arguments) if tc.function.arguments else {}
                                    if asyncio.iscoroutinefunction(fn):
                                        result = await fn(**args)
                                    else:
                                        result = fn(**args)
                                else:
                                    result = f"[未知工具] {tc.function.name}"
                            except Exception as e:
                                result = f"[工具调用出错] {e}"
                            messages.append({
                                "role": "tool",
                                "tool_call_id": tc.id,
                                "content": result
                            })

                        if tool_call_count >= MAX_TOOL_CALLS:
                            messages.append({
                                "role": "user",
                                "content": "已达到工具调用次数上限，请直接基于已有信息回复用户。如有多个句子请用/s\分割。如需发送文件用 <media>完整路径</media> 标记。"
                            })
                            # 最后一轮强制禁止工具调用，直接获取文本回复
                            response = ai_client.chat.completions.create(
                                model=AI_MODEL,
                                messages=messages,
                            )
                            response_message = response.choices[0].message
                            break
                    else:
                        break  # 最终文本回复，退出循环

                # 只追加最终回复
                messages.append(response_message.to_dict())
                with open("messages.json", "wt", encoding="utf-8") as f:
                    json.dump(messages, f)
                if not _is_no_reply(response_message.content):
                    _msg_count_since_ai = 0
                    end_message = response_message.content.split("/s\\")
                    first_message = True
                    for v in end_message:
                        v = v.strip()
                        if not v:
                            continue
                        if first_message:
                            await _send_with_media(client, group_id, v, reply_to=event)
                            first_message = False
                        else:
                            await _send_with_media(client, group_id, v)
                        time.sleep(1)

                history = []

    # 连接断开，取消调度器
    scheduler_task.cancel()
    print("[定时任务] 调度器已停止")

# ---- Web 管理面板 ----
_LOGIN_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>__NAME__ · 登录</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0b0b0f;color:#e0e0e0;display:flex;align-items:center;justify-content:center;min-height:100vh}
.card{background:#16161d;border:1px solid #262630;border-radius:16px;padding:44px 36px;width:380px;text-align:center}
.card h1{font-size:52px;margin-bottom:6px}
.card .sub{color:#888;margin-bottom:28px;font-size:14px}
.card input{width:100%;padding:12px 16px;background:#0b0b0f;border:1px solid #333;border-radius:8px;color:#e0e0e0;font-size:16px;outline:none;transition:border-color .2s}
.card input:focus{border-color:#818cf8}
.card button{width:100%;margin-top:16px;padding:12px;background:#818cf8;color:#fff;border:none;border-radius:8px;font-size:16px;cursor:pointer;transition:background .2s;font-weight:600}
.card button:hover{background:#6366f1}
.err{color:#f87171;margin-top:14px;font-size:13px;min-height:20px}
</style>
</head>
<body>
<div class="card">
  <h1>🤖</h1>
  <p class="sub">__NAME__ · 对话记录查看器</p>
  <form method="post" action="/login">
    <input type="password" name="pwd" placeholder="请输入访问密码" required autofocus>
    <button type="submit">登 录</button>
  </form>
  <div class="err">{error}</div>
</div>
</body>
</html>"""

_CHAT_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>__NAME__ · 对话记录</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0b0b0f;color:#e0e0e0}
header{background:#16161d;border-bottom:1px solid #262630;padding:14px 24px;display:flex;align-items:center;gap:16px;position:sticky;top:0;z-index:10}
header h1{font-size:20px;font-weight:700;flex:1}
header .count{color:#888;font-size:13px}
header button,header a{background:#262630;color:#e0e0e0;border:1px solid #333;padding:7px 16px;border-radius:6px;font-size:13px;cursor:pointer;text-decoration:none;transition:background .2s}
header button:hover,header a:hover{background:#333}
main{padding:20px 24px;max-width:900px;margin:0 auto}
.msg{margin-bottom:16px;display:flex;gap:12px}
.msg .avatar{width:36px;height:36px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:18px;flex-shrink:0}
.msg .body{flex:1;min-width:0}
.msg .head{font-size:12px;color:#888;margin-bottom:4px;display:flex;align-items:center;gap:8px}
.msg .head .role{font-weight:600;text-transform:uppercase;letter-spacing:.5px;padding:2px 8px;border-radius:4px;font-size:11px}
.msg .role.user{background:#1e3a5f;color:#60a5fa}
.msg .role.assistant{background:#1a3a2a;color:#4ade80}
.msg .role.tool{background:#2a2a1a;color:#facc15}
.msg .role.system{background:#2a1a2a;color:#c084fc}
.msg .bubble{background:#16161d;border:1px solid #262630;border-radius:10px;padding:12px 16px;line-height:1.6;white-space:pre-wrap;word-break:break-word;font-size:14px}
.msg.assistant .bubble{border-color:#1a3a2a}
.msg.user .bubble{border-color:#1e3a5f}
.msg .tools{margin-top:8px}
.msg .tool-call{background:#12121a;border:1px solid #2a2a1a;border-radius:8px;margin-bottom:6px;overflow:hidden}
.msg .tool-call summary{padding:10px 14px;cursor:pointer;font-size:13px;color:#facc15;font-weight:600;user-select:none}
.msg .tool-call summary:hover{background:#1a1a22}
.msg .tool-call pre{background:#0b0b0f;color:#aaa;padding:10px 14px;font-size:12px;overflow-x:auto;white-space:pre-wrap;max-height:200px;overflow-y:auto}
.msg .tool-result{margin-top:4px;padding:8px 14px;background:#1a1a10;border-left:3px solid #facc15;border-radius:0 6px 6px 0;font-size:13px;color:#ccc}
.empty{text-align:center;color:#555;padding:60px 0;font-size:15px}
.fade{opacity:.5}
@media(max-width:600px){header{padding:10px 14px}header h1{font-size:16px}main{padding:12px 8px}.msg{gap:8px}.msg .bubble{padding:10px 12px;font-size:13px}}
</style>
</head>
<body>
<header>
  <h1>🤖 __NAME__对话记录</h1>
  <span class="count" id="count">--</span>
  <button onclick="refresh()">🔄 刷新</button>
  <a href="/logout">退出</a>
</header>
<main id="list"></main>
<script>
const ROLE_LABEL = {user:'用户',assistant:'__NAME__',tool:'工具',system:'系统'};
const ROLE_ICON = {user:'👤',assistant:'🤖',tool:'🔧',system:'⚙️'};

function esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML}

function fmt(v){
  if(v===null||v===undefined) return '—';
  if(typeof v==='string') return esc(v);
  return esc(JSON.stringify(v,null,2));
}

function renderToolCall(id,fn,args){
  return `<div class="tool-call">
    <details><summary>🔧 tool_call: ${esc(fn)}</summary>
    <pre>${fmt(args)}</pre></details>
  </div>`;
}

function renderMessages(data){
  const list=document.getElementById('list');
  const count=document.getElementById('count');
  if(!data||!data.length){
    list.innerHTML='<div class="empty">📭 暂无对话记录</div>';
    count.textContent='0 条消息';
    return;
  }
  count.textContent=data.length+' 条消息';
  let html='';
  for(const m of data){
    const role=m.role||'unknown';
    const icon=ROLE_ICON[role]||'❓';
    const label=ROLE_LABEL[role]||role;
    let body='';

    if(m.tool_calls){
      for(const tc of m.tool_calls){
        body+=renderToolCall(tc.id,tc.function?.name||'?',tc.function?.arguments||{});
      }
    }
    if(m.content){
      body+='<div class="bubble">'+fmt(m.content)+'</div>';
    }
    if(!m.content&&!m.tool_calls){
      body+='<div class="bubble fade">（空消息）</div>';
    }

    html+='<div class="msg '+esc(role)+'">'
      +'<div class="avatar">'+icon+'</div>'
      +'<div class="body">'
        +'<div class="head"><span class="role '+esc(role)+'">'+esc(label)+'</span></div>'
        +body
      +'</div>'
    +'</div>';
  }
  list.innerHTML=html;
  window.scrollTo(0,document.body.scrollHeight);
}

async function refresh(){
  try{
    document.getElementById('list').innerHTML='<div class="empty">⏳ 加载中...</div>';
    const r=await fetch('/api/messages');
    if(r.status===401){location.href='/';return}
    if(!r.ok) throw new Error(r.status);
    const data=await r.json();
    renderMessages(data);
  }catch(e){
    document.getElementById('list').innerHTML='<div class="empty">❌ 加载失败: '+esc(e.message)+'</div>';
  }
}
refresh();
</script>
</body>
</html>"""

_LOGIN_PAGE = _LOGIN_PAGE.replace("__NAME__", BOT_NAME)
_CHAT_PAGE = _CHAT_PAGE.replace("__NAME__", BOT_NAME)


class WebHandler(http.server.BaseHTTPRequestHandler):
    def _ok(self, content, ct="text/html; charset=utf-8"):
        self.send_response(200); self.send_header("Content-Type", ct); self.end_headers()
        self.wfile.write(content.encode())

    def _redirect(self, to):
        self.send_response(302); self.send_header("Location", to); self.end_headers()

    def _unauth(self):
        self.send_response(401); self.send_header("Content-Type", "text/plain; charset=utf-8"); self.end_headers()
        self.wfile.write(b"Unauthorized")

    def _check_auth(self):
        cookie = self.headers.get("Cookie", "")
        for part in cookie.split(";"):
            kv = part.strip().split("=", 1)
            if len(kv) == 2 and kv[0] == "session" and kv[1] in WEB_SESSIONS:
                return True
        return False

    def _set_session_cookie(self):
        token = secrets.token_hex(32)
        WEB_SESSIONS.add(token)
        self.send_header("Set-Cookie", f"session={token}; Path=/; HttpOnly; Max-Age=86400")

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            self._ok(_LOGIN_PAGE.replace("{error}", ""))
        elif path == "/chat":
            if not self._check_auth(): self._redirect("/"); return
            self._ok(_CHAT_PAGE)
        elif path == "/api/messages":
            if not self._check_auth(): self._unauth(); return
            try:
                with open("messages.json", "rt", encoding="utf-8") as f:
                    data = json.load(f)
                self._ok(json.dumps(data, ensure_ascii=False), "application/json; charset=utf-8")
            except (FileNotFoundError, json.JSONDecodeError):
                self._ok("[]", "application/json; charset=utf-8")
        elif path == "/logout":
            cookie = self.headers.get("Cookie", "")
            for part in cookie.split(";"):
                kv = part.strip().split("=", 1)
                if len(kv) == 2 and kv[0] == "session":
                    WEB_SESSIONS.discard(kv[1])
            self._redirect("/")
        else:
            self._redirect("/")

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/login":
            cl = int(self.headers.get("Content-Length", 0))
            body = parse_qs(self.rfile.read(cl).decode())
            pwd = body.get("pwd", [""])[0]
            if pwd == WEB_PASSWORD:
                self.send_response(302)
                self._set_session_cookie()
                self.send_header("Location", "/chat")
                self.end_headers()
            else:
                self._ok(_LOGIN_PAGE.replace("{error}", "密码错误"), "text/html; charset=utf-8")
        else:
            self._redirect("/")

    def log_message(self, *args):
        pass  # 静默日志

def start_web():
    srv = http.server.HTTPServer(("0.0.0.0", WEB_PORT), WebHandler)
    print(f"🌐 Web 面板已启动: http://localhost:{WEB_PORT}")
    srv.serve_forever()

async def main():
    threading.Thread(target=start_web, daemon=True).start()
    server = ReverseWebSocketServer(
        handler=on_bot_connected,
        host=BOT_HOST,
        port=BOT_PORT
    )
    print("🚀 机器人服务已启动，等待 NapCat 连接...")
    await server.run_forever()

if __name__ == "__main__":
    asyncio.run(main())