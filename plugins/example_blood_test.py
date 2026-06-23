# -*- coding: utf-8 -*-
"""
示例插件：
- 通过 get_function(send, chat) 接收两个 API 函数存到全局变量
- on_message(data) 只接收消息数据
- 距离 AI 上条消息超过 10 条 + 没有艾特机器人的情况下，让 AI 出来说话
"""

plugin_name = "AI主动说话"

# 全局存储 send / chat 函数
_send = None
_chat = None


async def get_function(send, chat):
    """插件加载时由框架调用，传入 send 和 chat 函数。存到全局变量供后续使用。"""
    global _send, _chat
    _send = send
    _chat = chat


async def on_message(data):
    """
    data = {
        "user_id": int, "user_name": str,
        "message": str, "group_id": int,
        "group_name": str, "at_list": list[str],
        "last_ai_ago": int,   # 距离AI上条真实消息的条数
        "at_bot": bool,        # 本条是否艾特了机器人
    }
    返回 str → 作为回复发送；None → 放行给 AI
    """
    # 艾特了机器人 → AI 会回复，插件不抢
    if data["at_bot"]:
        return None

    if data["last_ai_ago"] >= 10:
        await _chat("大家聊了这么久了，出来说句话活跃一下气氛，简短有趣")

    return None
