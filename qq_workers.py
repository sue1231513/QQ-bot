import os
import asyncio
import re
import random
import time
import json
import requests as _requests
from datetime import datetime, timezone, timedelta
 
CST = timezone(timedelta(hours=8))
 
 
def _now_ts() -> str:
    return datetime.now(CST).strftime("%m-%d %H:%M")
 
 
from utils import call_llm, recognize_image_url, recognize_qq_voice  # noqa
from context import (
    build_qq_context, build_group_context,
    save_chat_message, save_group_message,
    get_chat_history_messages, get_group_history,
    get_other_groups_context,
)
from free_tools import TOOL_SCHEMAS as FREE_TOOL_SCHEMAS, TOOL_DISPATCH as FREE_TOOL_DISPATCH
 
QQ_BOT_ID    = os.environ.get("QQ_BOT_ID", "")
QQ_BOT_NAME  = os.environ.get("QQ_BOT_NAME", "Bot")
QQ_OWNER_ID  = os.environ.get("QQ_OWNER_ID", "")
_raw_groups  = os.environ.get("QQ_GROUP_IDS", "")
QQ_GROUP_IDS = set(_raw_groups.split(",")) if _raw_groups else set()
 
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
 
_settings_cache: dict[str, tuple] = {}
_SETTINGS_TTL = 30
 
 
def _cached_get_setting(key: str):
    """从 Supabase bot_settings 表读取配置，带30秒本地缓存"""
    now = time.time()
    cached = _settings_cache.get(key)
    if cached and cached[1] > now:
        return cached[0]
    value = None
    try:
        res = _requests.get(
            f"{SUPABASE_URL}/rest/v1/bot_settings?key=eq.{key}&select=value",
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
            timeout=3,
        )
        data = res.json()
        if data:
            value = data[0].get("value")
    except Exception as e:
        print(f"⚠️ 读取 setting[{key}] 失败: {e}")
    _settings_cache[key] = (value, now + _SETTINGS_TTL)
    return value
 
 
def _get_prompt(key: str, default: str) -> str:
    """优先读 Supabase 动态配置，fallback 到 default"""
    val = _cached_get_setting(key)
    return val if val else default
 
 
# 工具白名单：群聊可用的工具
QQ_GROUP_TOOL_SCHEMAS = [
    s for s in FREE_TOOL_SCHEMAS
    if s["function"]["name"] in ("get_weather", "get_weather_forecast", "web_search", "web_extract", "search_taobao")
]
 
# 私聊额外可用：语音发送
QQ_VOICE_TOOL_SCHEMAS = [
    s for s in FREE_TOOL_SCHEMAS
    if s["function"]["name"] == "send_qq_voice"
]
 
MUTE_KEYWORDS = [k.strip() for k in os.environ.get("MUTE_KEYWORDS", "闭嘴,别讲话,安静").split(",") if k.strip()]
MUTE_DURATION = int(os.environ.get("MUTE_DURATION", "5"))
 
_AT_RE = re.compile(r'\[CQ:at,qq=(\d+)[^\]]*\]')
_CQ_RE = re.compile(r'\[CQ:[^\]]+\]')
 
_member_names: dict[str, dict[str, str]] = {}
_group_names:  dict[str, str] = {}
 
_private_pending: asyncio.Task | None = None
_group_pending: dict[str, asyncio.Task] = {}
 
_mute_until: float = 0.0
 
_POKE_FALLBACKS = ["哎干嘛啊", "？", "说话啊", "戳我干嘛"]
 
 
def _check_mute(text: str, user_id: str) -> bool:
    global _mute_until
    if user_id == QQ_OWNER_ID and any(k in text for k in MUTE_KEYWORDS):
        _mute_until = time.time() + MUTE_DURATION * 60
        print(f"🤐 收到闭嘴指令，群聊静音 {MUTE_DURATION} 分钟")
        return True
    return False
 
 
def _is_muted() -> bool:
    return time.time() < _mute_until
 
 
def _strip_name_prefix(text: str) -> str:
    """去掉 LLM 可能输出的名字前缀"""
    text = re.sub(r'^\[[^\]]{1,30}\]\s*', '', text)
    for sep in ("\uff1a", ":"):
        prefix = f"{QQ_BOT_NAME}{sep}"
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return text
 
 
_GROUP_PREFIX_RE = re.compile(r'^\[[^\]\n]{1,30}\]\s*')
 
 
def _strip_group_prefix(text: str) -> str:
    """去掉 LLM 可能模仿写出的行首 [群名] 前缀"""
    return _GROUP_PREFIX_RE.sub('', text).strip()
 
 
_XML_TOOL_RE = re.compile(
    r'<(?:function_calls|invoke|tool_call|alm\xf6)[^>]*>.*?</(?:function_calls|invoke|tool_call|alm\xf6)>',
    re.DOTALL
)
 
 
def _strip_tool_calls(text: str) -> str:
    return _XML_TOOL_RE.sub('', text).strip()
 
 
# ──────────────────────────────────────────────
# 群聊提示词（无@时）
# 可通过 Supabase bot_settings 表 key=qq_group_chat_prompt 动态覆盖
# ──────────────────────────────────────────────
_GROUP_EXTRA_DEFAULT = """
 
你是{bot_name}，当前在QQ群聊中。
 
【消息格式】
- 消息格式为『发言人（群昵称）：消息内容』，@某人时格式为『@群昵称』
- 回复时直接说内容，不要在前面加『{bot_name}：』或任何名字前缀
- 不要发emoji
 
【群聊规则】
- 顺着对话自然接话，简短为主，通常是1-3句，不要长篇大论
- 可以主动找话题，但不要每句话都抢着说
- 被开玩笑可以自嘲或回怼，不要玻璃心
 
【PASS条件 — 以下情况只输出"PASS"】
- 对方仅简单附和（嗯、哈哈、对、好）
- 话题已结束，无新信息可接
- 对方在与他人交谈，明显不需要你插话
- 你没有立场参与的话题"""
 
# ──────────────────────────────────────────────
# 被@时的群聊提示词
# 可通过 Supabase bot_settings 表 key=qq_group_chat_at_prompt 动态覆盖
# ──────────────────────────────────────────────
_GROUP_EXTRA_AT_DEFAULT = """
 
你是{bot_name}，当前在QQ群聊中。
 
【消息格式】
- 消息格式为『发言人（群昵称）：消息内容』，@某人时格式为『@群昵称』
- 回复时直接说内容，不要在前面加『{bot_name}：』或任何名字前缀
- 不要发emoji
 
【群聊规则】
- 顺着对话自然接话，简短为主，通常是1-3句，不要长篇大论
- 可以主动找话题，但不要每句话都抢着说
- 被开玩笑可以自嘲或回怼，不要玻璃心
- 你被直接 @ 点名了，必须回复，不允许输出 PASS。"""
 
 
def _strip_cq(text: str) -> str:
    return _CQ_RE.sub('', text).strip()
 
 
def _resolve_at(group_id: str, qq_id: str) -> str:
    if qq_id == QQ_BOT_ID:
        return f"@{QQ_BOT_NAME}"
    return f"@{_member_names.get(group_id, {}).get(qq_id, qq_id)}"
 
 
def _extract_image_urls(message_list: list) -> list[str]:
    return [
        seg["data"]["url"]
        for seg in message_list
        if seg.get("type") == "image" and seg.get("data", {}).get("url")
    ]
 
 
async def _extract_reply_text(message_list: list) -> str:
    for seg in message_list:
        if seg.get("type") == "reply":
            msg_id = seg.get("data", {}).get("id")
            if not msg_id:
                continue
            try:
                from qq_bot import get_msg
                msg_data = await get_msg(int(msg_id))
                if not msg_data:
                    continue
                sender_info = msg_data.get("sender", {})
                sender_name = sender_info.get("card") or sender_info.get("nickname", "未知")
                raw = msg_data.get("raw_message", "") or ""
                text = _strip_cq(raw).strip()
                if text:
                    return f"[引用 {sender_name}：{text}]"
            except Exception as e:
                print(f"❌ 获取引用消息失败: {e}")
    return ""
 
 
async def _ensure_member_name(group_id: str, user_id: str) -> str:
    name = _member_names.get(group_id, {}).get(user_id)
    if name:
        return name
    try:
        from qq_bot import get_group_member_info
        info = await get_group_member_info(int(group_id), int(user_id))
        if info:
            name = info.get("card") or info.get("nickname", user_id)
            _member_names.setdefault(group_id, {})[user_id] = name
            return name
    except Exception as e:
        print(f"❌ 获取群成员信息失败: {e}")
    return user_id
 
 
async def _fetch_group_name(group_id: str):
    try:
        from qq_bot import get_group_info
        info = await get_group_info(int(group_id))
        if info:
            name = info.get("group_name", "")
            if name:
                _group_names[group_id] = name
                print(f"📋 获取群名: {group_id} = {name}")
    except Exception as e:
        print(f"❌ 获取群名失败: {e}")
 
 
async def _split_send(send_fn, parts: list[str]):
    for i, part in enumerate(parts):
        await send_fn(part)
        if i < len(parts) - 1:
            await asyncio.sleep(random.uniform(1.0, 2.0))
 
 
def _merge_lines(reply: str) -> list[str]:
    lines = [ln.strip() for ln in reply.split('\n') if ln.strip()]
    if len(lines) <= 1:
        return [reply.strip()] if reply.strip() else []
    merged, buf = [], ""
    for ln in lines:
        if buf and len(buf) < 20 and len(buf + "\n" + ln) < 80:
            buf += "\n" + ln
        else:
            if buf:
                merged.append(buf)
            buf = ln
    if buf:
        merged.append(buf)
    return merged
 
 
async def _private_reply():
    global _private_pending
    try:
        await asyncio.sleep(random.randint(12, 15))
        from qq_bot import send_qq_msg
        system_prompt = await asyncio.to_thread(build_qq_context)
 
        history = get_chat_history_messages(30)
        messages = [{"role": "system", "content": system_prompt}] + history
 
        final_reply = ""
        for _ in range(3):
            content, tool_calls = await call_llm(messages, tools=QQ_GROUP_TOOL_SCHEMAS + QQ_VOICE_TOOL_SCHEMAS)
            if not tool_calls:
                final_reply = content
                break
            assistant_msg: dict = {"role": "assistant"}
            if content:
                assistant_msg["content"] = content
            assistant_msg["tool_calls"] = tool_calls
            messages.append(assistant_msg)
            for tc in tool_calls:
                fn_name = tc["function"]["name"]
                try:
                    fn_args = json.loads(tc["function"]["arguments"])
                except json.JSONDecodeError as exc:
                    print(f"❌ 工具参数解析失败 fn={fn_name}: {exc}")
                    fn_args = {}
                if fn_name in FREE_TOOL_DISPATCH:
                    tool_result = await asyncio.to_thread(FREE_TOOL_DISPATCH[fn_name], fn_args)
                else:
                    tool_result = f"未知工具: {fn_name}"
                messages.append({"role": "tool", "tool_call_id": tc["id"], "content": str(tool_result)})
 
        if not final_reply:
            final_reply, _ = await call_llm(messages, tools=None)
        reply = _strip_tool_calls(final_reply.strip() if final_reply else "")
        if not reply:
            return
        owner_id = int(QQ_OWNER_ID)
        save_chat_message("assistant", reply)
        parts = _merge_lines(reply)
        if parts:
            await _split_send(lambda msg: send_qq_msg("private", owner_id, msg), parts)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"❌ QQ 私聊回复错误: {type(e).__name__}: {e}")
    finally:
        if _private_pending is asyncio.current_task():
            _private_pending = None
 
 
async def _group_reply(group_id: str, delay_range: tuple[int, int] = (15, 15), force_reply: bool = False):
    current_task = asyncio.current_task()
    _completed = False
    _hist_len_at_reply = 0
    try:
        waited, last_count = 0, len(get_group_history(group_id))
        await asyncio.sleep(delay_range[0])
        waited += delay_range[0]
        while waited < 28:
            current_count = len(get_group_history(group_id))
            if current_count > last_count:
                last_count = current_count
                await asyncio.sleep(8)
                waited += 8
            else:
                break
 
        if not force_reply and _is_muted():
            return
 
        from qq_bot import send_qq_msg
        group_label = f"{group_id}-{_group_names.get(group_id, group_id)}"
 
        default_extra = (_GROUP_EXTRA_AT_DEFAULT if force_reply else _GROUP_EXTRA_DEFAULT).format(bot_name=QQ_BOT_NAME)
        prompt_key = "qq_group_chat_at_prompt" if force_reply else "qq_group_chat_prompt"
        extra_prompt = await asyncio.to_thread(_get_prompt, prompt_key, default_extra)
        if force_reply and "PASS" not in extra_prompt:
            extra_prompt += "\n你被直接 @ 点名了，必须回复，不允许输出 PASS。"
 
        system_prompt = await asyncio.to_thread(build_group_context)
        system_prompt += "\n\n" + extra_prompt
 
        other_ctx = get_other_groups_context(group_id, 20)
        if other_ctx:
            system_prompt += "\n\n" + other_ctx
 
        _hist_len_at_reply = len(get_group_history(group_id))
        messages = [{"role": "system", "content": system_prompt}] + get_group_history(group_id, 50)
 
        final_reply = ""
        for _ in range(3):
            content, tool_calls = await call_llm(messages, tools=QQ_GROUP_TOOL_SCHEMAS)
            if not tool_calls:
                final_reply = content
                break
            assistant_msg: dict = {"role": "assistant"}
            if content:
                assistant_msg["content"] = content
            assistant_msg["tool_calls"] = tool_calls
            messages.append(assistant_msg)
            for tc in tool_calls:
                fn_name = tc["function"]["name"]
                try:
                    fn_args = json.loads(tc["function"]["arguments"])
                except json.JSONDecodeError as exc:
                    print(f"❌ 工具参数解析失败 fn={fn_name}: {exc}")
                    fn_args = {}
                if fn_name in FREE_TOOL_DISPATCH:
                    tool_result = await asyncio.to_thread(FREE_TOOL_DISPATCH[fn_name], fn_args)
                else:
                    tool_result = f"未知工具: {fn_name}"
                messages.append({"role": "tool", "tool_call_id": tc["id"], "content": str(tool_result)})
 
        if not final_reply:
            final_reply, _ = await call_llm(messages, tools=None)
        reply = _strip_group_prefix(_strip_name_prefix(_strip_tool_calls(final_reply.strip() if final_reply else "")))
        if not reply or "PASS" in reply.upper() or reply.strip() == "通过":
            _completed = True
            return
        save_group_message(group_id, "assistant", QQ_BOT_NAME, reply, source=_group_names.get(group_id, group_id))
        await send_qq_msg("group", int(group_id), reply)
        print(f"💬 QQ群[{group_label}] 回复: {reply[:60]}")
        _completed = True
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"❌ QQ 群聊回复错误 [group={group_id}]: {type(e).__name__}: {e}", flush=True)
        _completed = True
    finally:
        if _group_pending.get(group_id) is current_task:
            _group_pending.pop(group_id, None)
            # 回复完成后，若期间有新消息积压则补触发一次
            if _completed and delay_range[0] >= 8:
                current_hist = get_group_history(group_id)
                if (len(current_hist) > _hist_len_at_reply
                        and current_hist[-1].get("role") == "user"):
                    group_label = f"{group_id}-{_group_names.get(group_id, group_id)}"
                    print(f"💬 QQ群[{group_label}] 检测到未回复消息，补触发一次")
                    _group_pending[group_id] = asyncio.create_task(_group_reply(group_id, (3, 5)))
 
 
async def _poke_reply(group_id: str | None, poker_name: str, poker_id: int):
    try:
        await asyncio.sleep(random.uniform(1.0, 3.0))
        from qq_bot import send_qq_msg, send_poke
 
        if group_id:
            existing = _group_pending.get(group_id)
            if existing and not existing.done():
                existing.cancel()
                _group_pending.pop(group_id, None)
            system_prompt = await asyncio.to_thread(build_group_context)
            system_prompt += _GROUP_EXTRA_DEFAULT.format(bot_name=QQ_BOT_NAME)
            history = get_group_history(group_id, 10)
        else:
            system_prompt = await asyncio.to_thread(build_qq_context)
            history = get_chat_history_messages(10)
 
        system_prompt += (
            f"\n\n【戳一戳】{poker_name}用QQ的戳一戳功能戳了戳你（{QQ_BOT_NAME}）。"
            "结合上面的聊天记录，用符合当前氛围的方式自然回应，简短一两句。"
            "必须回应，不允许输出 PASS。回复不要带名字前缀。"
        )
 
        poke_msg = f"{poker_name}戳了戳你"
        messages = [{"role": "system", "content": system_prompt}] + history + [
            {"role": "user", "content": poke_msg}
        ]
        content, _ = await call_llm(messages)
        reply = _strip_name_prefix(content.strip() if content else "")
        if not reply or reply == "PASS":
            return
 
        if group_id:
            _gname = _group_names.get(group_id, group_id)
            save_group_message(group_id, "user", poker_name, poke_msg, source=_gname)
            save_group_message(group_id, "assistant", QQ_BOT_NAME, reply, source=_gname)
            await send_qq_msg("group", int(group_id), reply)
        else:
            save_chat_message("user", f"[QQ] {poke_msg}")
            save_chat_message("assistant", reply)
            await send_qq_msg("private", poker_id, reply)
 
        if random.random() < 0.5:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            await send_poke(poker_id, int(group_id) if group_id else None)
 
    except Exception as e:
        print(f"❌ QQ 戳一戳回复错误: {type(e).__name__}: {e}")
 
 
async def handle_qq_event(data: dict):
    global _private_pending
    post_type = data.get("post_type")
 
    if _cached_get_setting("qq_bot_paused") == "true":
        return
 
    if post_type == "notice":
        if data.get("notice_type") == "notify" and data.get("sub_type") == "poke":
            target_id = str(data.get("target_id", ""))
            if target_id != QQ_BOT_ID:
                return
            user_id    = data.get("user_id", 0)
            group_id   = str(data.get("group_id", "")) if data.get("group_id") else None
            poker_name = _member_names.get(group_id or "", {}).get(str(user_id), str(user_id))
            group_name = _group_names.get(group_id or "", "")
            location   = f"群{group_id}-{group_name}" if group_id else "私聊"
            print(f"👉 被戳了一戳: {poker_name} ({location})")
            asyncio.create_task(_poke_reply(group_id, poker_name, user_id))
        return
 
    if post_type != "message":
        return
 
    message_type = data.get("message_type", "")
    message      = data.get("raw_message", "") or ""
    user_id      = str(data.get("user_id", ""))
    sender       = data.get("sender", {})
    message_list = data.get("message", [])
 
    # ── 私聊 ─────────────────────────────────────────────────────────────
    if message_type == "private":
        if QQ_OWNER_ID and user_id != QQ_OWNER_ID:
            return
        clean = _strip_cq(message)
 
        # 语音消息
        voice_segs = [seg for seg in message_list if seg.get("type") == "record"]
        if voice_segs:
            voice_url = voice_segs[0].get("data", {}).get("url", "")
            if voice_url:
                try:
                    recognized = await recognize_qq_voice(voice_url)
                except Exception as e:
                    print(f"❌ QQ语音识别失败: {type(e).__name__}: {e}")
                    recognized = None
                if recognized:
                    print(f"🎤 [QQ语音] 识别结果: {recognized}")
                    save_chat_message("user", f"[QQ语音] {recognized}")
                    if _private_pending and not _private_pending.done():
                        _private_pending.cancel()
                    _private_pending = asyncio.create_task(_private_reply())
                else:
                    print("⚠️ [QQ语音] 识别结果为空，跳过")
            return
 
        # 图片消息
        image_urls = _extract_image_urls(message_list)
        if image_urls:
            try:
                img_desc = await recognize_image_url(image_urls[0], clean or "")
                img_caption = f"，配文：{clean}" if clean else ""
                clean = f"[图片{img_caption}，视觉识别：{img_desc}]"
                print(f"🖼️ [QQ私聊] 识图完成: {img_desc[:40]}")
            except Exception as e:
                print(f"❌ QQ私聊识图失败: {type(e).__name__}: {e}")
                clean = f"[图片，配文：{clean}](识别失败)" if clean else "[图片](识别失败)"
 
        # 引用消息
        reply_prefix = await _extract_reply_text(message_list)
        if reply_prefix:
            clean = f"{reply_prefix} {clean}" if clean else reply_prefix
        if not clean:
            return
        print(f"📨 [QQ私聊] {clean[:80]}")
        save_chat_message("user", f"[QQ] {clean}")
        if _private_pending and not _private_pending.done():
            _private_pending.cancel()
        _private_pending = asyncio.create_task(_private_reply())
 
    # ── 群聊 ─────────────────────────────────────────────────────────────
    elif message_type == "group":
        group_id = str(data.get("group_id", ""))
        if QQ_GROUP_IDS and group_id not in QQ_GROUP_IDS:
            return
 
        sender_name = sender.get("card") or sender.get("nickname", "未知")
        group_name  = data.get("group_name", "")
        if group_name:
            _group_names[group_id] = group_name
        elif group_id not in _group_names:
            asyncio.create_task(_fetch_group_name(group_id))
 
        group_label = f"{group_id}-{_group_names.get(group_id, group_id)}"
 
        _member_names.setdefault(group_id, {})
        _member_names[group_id][user_id] = sender_name
        _member_names[group_id][QQ_BOT_ID] = QQ_BOT_NAME
 
        is_at_bot = f"[CQ:at,qq={QQ_BOT_ID}]" in message
 
        # 异步预取被@者的群昵称
        for at_id in _AT_RE.findall(message):
            if at_id != QQ_BOT_ID and at_id not in _member_names.get(group_id, {}):
                asyncio.create_task(_ensure_member_name(group_id, at_id))
 
        clean = re.sub(r'\[CQ:at,qq=(\d+)[^\]]*\]', lambda m: _resolve_at(group_id, m.group(1)), message)
        clean = _strip_cq(clean).strip()
 
        # 静音指令
        if _check_mute(clean, user_id):
            from qq_bot import send_qq_msg
            await send_qq_msg("group", int(group_id), f"好，静音 {MUTE_DURATION} 分钟。")
            return
 
        # 图片识别
        image_urls = _extract_image_urls(message_list)
        if image_urls:
            try:
                img_desc = await recognize_image_url(image_urls[0], clean or "")
                img_caption = f"，配文：{clean}" if clean else ""
                clean = f"[图片{img_caption}，视觉识别：{img_desc}]"
                print(f"🖼️ [QQ群{group_label}] 识图完成: {img_desc[:40]}")
            except Exception as e:
                print(f"❌ QQ群聊识图失败: {type(e).__name__}: {e}")
                clean = f"[图片，配文：{clean}](识别失败)" if clean else "[图片](识别失败)"
        elif not clean:
            clean = "（发了个表情）"
 
        # 引用消息
        reply_prefix = await _extract_reply_text(message_list)
        if reply_prefix:
            clean = f"{reply_prefix} {clean}"
 
        # 静音期间非@消息只存不回
        if not is_at_bot and _is_muted():
            save_group_message(group_id, "user", sender_name, clean, source=_group_names.get(group_id, group_id))
            return
 
        save_group_message(group_id, "user", sender_name, clean, source=_group_names.get(group_id, group_id))
        print(f"💬 [QQ群{group_label}] {sender_name}: {clean[:60]}")
 
        if is_at_bot:
            # 被@：取消旧任务，立即快速回复
            existing = _group_pending.get(group_id)
            if existing and not existing.done():
                existing.cancel()
            _group_pending[group_id] = asyncio.create_task(
                _group_reply(group_id, (2, 4), force_reply=True)
            )
        else:
            existing = _group_pending.get(group_id)
            if image_urls:
                # 图片消息也快速触发
                if existing and not existing.done():
                    existing.cancel()
                _group_pending[group_id] = asyncio.create_task(_group_reply(group_id, (3, 5)))
            else:
                # 普通消息：已有任务时不打断，等当前任务的 finally 里补触发
                if existing and not existing.done():
                    return
                _group_pending[group_id] = asyncio.create_task(_group_reply(group_id))
