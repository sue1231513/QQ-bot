import os
import asyncio
import json
import uuid
import time
import requests
 
import websockets
from websockets.exceptions import ConnectionClosed
 
NAPCAT_WS_URL    = os.environ.get("NAPCAT_WS_URL", "ws://napcat.zeabur.internal:3001")
NAPCAT_WS_TOKEN  = os.environ.get("NAPCAT_WS_TOKEN", "")
PUSHPLUS_TOKEN   = os.environ.get("PUSHPLUS_TOKEN", "")
 
_last_notify_time: float = 0.0
_NOTIFY_COOLDOWN  = 1800  # 30 分钟内只推一次
 
 
def _notify_disconnect(reason: str = ""):
    """断线时推送微信通知，30 分钟冷却"""
    global _last_notify_time
    if not PUSHPLUS_TOKEN:
        return
    now = time.time()
    if now - _last_notify_time < _NOTIFY_COOLDOWN:
        return
    _last_notify_time = now
    try:
        now_str = time.strftime("%H:%M:%S")
        content = f"NapCat QQ 连接已断开，请前往 NapCat 网页重新登录。<br>原因：{reason or '未知'}<br>时间：{now_str}"
        requests.get(
            "http://www.pushplus.plus/send",
            params={"token": PUSHPLUS_TOKEN, "title": "⚠️ QQ 机器人掉线了", "content": content, "template": "html"},
            timeout=8,
        )
        print("📱 已发送断线微信通知")
    except Exception as e:
        print(f"⚠️ 推送通知失败: {e}")
 
 
_ws_conn  = None
_send_lock = asyncio.Lock()
_pending_replies: dict[str, asyncio.Future] = {}
 
 
async def _send_action(action: str, params: dict):
    global _ws_conn
    if not _ws_conn:
        print("❌ QQ WS 未连接，无法发送")
        return
    payload = json.dumps({"action": action, "params": params, "echo": str(uuid.uuid4())})
    async with _send_lock:
        try:
            await _ws_conn.send(payload)
            if action not in ("send_private_msg", "send_group_msg"):
                print(f"📤 发送 action={action} params={params}")
        except Exception as e:
            print(f"❌ QQ 发送失败: {e}")
 
 
async def _send_action_and_wait(action: str, params: dict, timeout: float = 5.0) -> dict | None:
    """发送 action 并等待 NapCat 响应"""
    global _ws_conn
    if not _ws_conn:
        return None
    echo = str(uuid.uuid4())
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()
    _pending_replies[echo] = fut
    payload = json.dumps({"action": action, "params": params, "echo": echo})
    async with _send_lock:
        try:
            await _ws_conn.send(payload)
        except Exception as e:
            print(f"❌ QQ 发送失败: {e}")
            _pending_replies.pop(echo, None)
            return None
    try:
        return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
    except asyncio.TimeoutError:
        _pending_replies.pop(echo, None)
        return None
 
 
async def get_msg(msg_id: int) -> dict | None:
    """获取消息详情"""
    result = await _send_action_and_wait("get_msg", {"message_id": msg_id})
    if result and result.get("status") == "ok":
        return result.get("data")
    return None
 
 
async def get_group_member_info(group_id: int, user_id: int) -> dict | None:
    """获取群成员信息"""
    result = await _send_action_and_wait(
        "get_group_member_info",
        {"group_id": group_id, "user_id": user_id, "no_cache": False}
    )
    if result and result.get("status") == "ok":
        return result.get("data")
    return None
 
 
async def get_group_info(group_id: int) -> dict | None:
    """获取群信息"""
    result = await _send_action_and_wait(
        "get_group_info",
        {"group_id": group_id, "no_cache": False}
    )
    if result and result.get("status") == "ok":
        return result.get("data")
    return None
 
 
async def send_qq_msg(target_type: str, target_id: int, message: str):
    """发送QQ消息。target_type: 'private' | 'group'"""
    if target_type == "private":
        await _send_action("send_private_msg", {"user_id": target_id, "message": message})
    else:
        await _send_action("send_group_msg", {"group_id": target_id, "message": message})
 
 
async def send_poke(user_id: int, group_id: int | None = None):
    """戳一戳某人。群聊传 group_id，私聊不传"""
    if group_id:
        await _send_action("group_poke", {"group_id": group_id, "user_id": user_id})
    else:
        await _send_action("friend_poke", {"user_id": user_id})
 
 
RECONNECT_INITIAL_DELAY = 2
RECONNECT_MAX_DELAY     = 60
RECONNECT_BACKOFF       = 2
 
_last_heartbeat_time:   float = 0.0
_last_offline_log_time: float = 0.0
_OFFLINE_LOG_INTERVAL         = 300  # 离线日志5分钟打一次，避免刷屏
 
 
async def handle_napcat_ws_forward(scope, receive, send):
    """正向WS模式：NapCat主动连进来"""
    global _ws_conn, _last_heartbeat_time
 
    # 验证token
    headers_dict = {k.decode("utf-8").lower(): v.decode("utf-8") for k, v in scope.get("headers", [])}
    auth = headers_dict.get("authorization", "").replace("Bearer ", "").replace("bearer ", "").strip()
    if NAPCAT_WS_TOKEN and auth != NAPCAT_WS_TOKEN:
        await send({"type": "websocket.close", "code": 1008})
        return
 
    await send({"type": "websocket.accept"})
    print("✅ NapCat 正向WS 已连接！")
 
    class _WSAdapter:
        async def send(self, text):
            await send({"type": "websocket.send", "text": text})
 
    _ws_conn = _WSAdapter()
    _last_heartbeat_time = time.time()
 
    from qq_workers import handle_qq_event
 
    try:
        while True:
            msg = await receive()
            if msg["type"] == "websocket.disconnect":
                print("⚠️ NapCat 断开连接")
                _notify_disconnect("NapCat WebSocket 主动断开")
                break
            if msg["type"] != "websocket.receive":
                continue
            raw = msg.get("text", "")
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
 
            if data.get("post_type") == "meta_event" and data.get("meta_event_type") == "heartbeat":
                _last_heartbeat_time = time.time()
                if not data.get("status", {}).get("online", True):
                    global _last_offline_log_time
                    _now = time.time()
                    if _now - _last_offline_log_time >= _OFFLINE_LOG_INTERVAL:
                        _last_offline_log_time = _now
                        print("⚠️ [Heartbeat] QQ 账号已离线！")
                        _notify_disconnect("心跳检测到 QQ 账号离线")
                continue
 
            if "KickedOffLine" in raw or (
                data.get("post_type") == "meta_event"
                and data.get("sub_type") in ("disable", "offline")
            ):
                _notify_disconnect("QQ 账号被踢下线")
                continue
 
            echo = data.get("echo")
            if echo and echo in _pending_replies:
                fut = _pending_replies.pop(echo)
                if not fut.done():
                    fut.set_result(data)
                continue
 
            if data.get("post_type"):
                asyncio.create_task(handle_qq_event(data))
    except Exception as e:
        print(f"❌ NapCat 正向WS 错误: {e}")
        _notify_disconnect(f"NapCat WS 异常断开: {e}")
    finally:
        _ws_conn = None
        for fut in list(_pending_replies.values()):
            if not fut.done():
                fut.cancel()
        _pending_replies.clear()
        print("NapCat 连接已关闭")
 
 
async def async_qq_bot():
    """正向WS模式：监控心跳超时，检测僵死连接"""
    print("🔌 QQ Bot 正向WS模式，等待 NapCat 连入 /qq-ws ...")
    HEARTBEAT_TIMEOUT = 90  # 90秒无心跳视为僵死
    CHECK_INTERVAL = 30
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        if _ws_conn is None:
            continue
        if _last_heartbeat_time <= 0:
            continue
        now = time.time()
        if now - _last_heartbeat_time > HEARTBEAT_TIMEOUT:
            print(f"⚠️ [Heartbeat] 超过{HEARTBEAT_TIMEOUT}秒未收到心跳，连接可能已僵死")
            _notify_disconnect(f"超过{HEARTBEAT_TIMEOUT}秒未收到 NapCat 心跳，连接可能已僵死")
