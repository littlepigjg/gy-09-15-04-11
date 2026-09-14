"""WebSocket处理器 - 实时日志和状态推送"""
import asyncio
import json
import logging
from typing import Dict, Set, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class WebSocketManager:
    """WebSocket连接管理器
    
    管理所有WebSocket连接，支持:
    1. 按工作流运行ID订阅
    2. 实时日志推送
    3. 任务状态变化推送
    4. 连接心跳检测
    """
    
    def __init__(self):
        # 活跃连接 {connection_id: WebSocket}
        self._connections: Dict[str, object] = {}
        
        # 订阅关系 {run_id: set(connection_ids)}
        self._subscriptions: Dict[str, Set[str]] = {}
        
        # 连接信息
        self._connection_info: Dict[str, Dict] = {}
        
        # 消息队列（用于离线消息）
        self._message_queues: Dict[str, list] = {}
    
    def register_connection(self, connection_id: str, websocket: object) -> None:
        """注册新连接"""
        self._connections[connection_id] = websocket
        self._connection_info[connection_id] = {
            "connected_at": datetime.now().isoformat(),
            "subscriptions": set()
        }
        logger.info(f"WebSocket连接注册: {connection_id}")
    
    def unregister_connection(self, connection_id: str) -> None:
        """注销连接"""
        # 移除所有订阅
        if connection_id in self._connection_info:
            for run_id in self._connection_info[connection_id]["subscriptions"]:
                if run_id in self._subscriptions:
                    self._subscriptions[run_id].discard(connection_id)
        
        self._connections.pop(connection_id, None)
        self._connection_info.pop(connection_id, None)
        logger.info(f"WebSocket连接注销: {connection_id}")
    
    async def subscribe(self, connection_id: str, run_id: str) -> bool:
        """订阅工作流运行"""
        if connection_id not in self._connections:
            return False
        
        if run_id not in self._subscriptions:
            self._subscriptions[run_id] = set()
        
        self._subscriptions[run_id].add(connection_id)
        self._connection_info[connection_id]["subscriptions"].add(run_id)
        
        # 发送已缓存的消息
        if run_id in self._message_queues:
            for msg in self._message_queues[run_id]:
                await self._send_to_connection(connection_id, msg)
            self._message_queues[run_id].clear()
        
        logger.debug(f"连接 {connection_id} 订阅运行 {run_id}")
        return True
    
    async def unsubscribe(self, connection_id: str, run_id: str) -> bool:
        """取消订阅"""
        if run_id in self._subscriptions:
            self._subscriptions[run_id].discard(connection_id)
        
        if connection_id in self._connection_info:
            self._connection_info[connection_id]["subscriptions"].discard(run_id)
        
        return True
    
    async def broadcast_to_run(
        self, 
        run_id: str, 
        event_type: str, 
        data: dict
    ) -> None:
        """向订阅了指定运行的所有连接广播消息"""
        message = {
            "type": "event",
            "event": event_type,
            "run_id": run_id,
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
        
        # 获取订阅者
        subscribers = self._subscriptions.get(run_id, set())
        
        if not subscribers:
            # 缓存消息
            if run_id not in self._message_queues:
                self._message_queues[run_id] = []
            self._message_queues[run_id].append(message)
            # 只保留最近100条消息
            if len(self._message_queues[run_id]) > 100:
                self._message_queues[run_id] = self._message_queues[run_id][-100:]
            return
        
        # 并发发送给所有订阅者
        tasks = []
        for conn_id in subscribers.copy():
            tasks.append(self._send_to_connection(conn_id, message))
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def broadcast_to_all(self, event_type: str, data: dict) -> None:
        """向所有连接广播消息"""
        message = {
            "type": "event",
            "event": event_type,
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
        
        tasks = []
        for conn_id in list(self._connections.keys()):
            tasks.append(self._send_to_connection(conn_id, message))
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_to_connection(
        self, 
        connection_id: str, 
        message: dict
    ) -> bool:
        """向指定连接发送消息"""
        websocket = self._connections.get(connection_id)
        if not websocket:
            return False
        
        try:
            if hasattr(websocket, 'send'):
                await websocket.send(json.dumps(message, ensure_ascii=False))
                return True
        except Exception as e:
            logger.error(f"发送消息失败: {e}")
            # 连接可能已断开
            self.unregister_connection(connection_id)
            return False
    
    async def handle_message(
        self, 
        connection_id: str, 
        message: str
    ) -> Optional[dict]:
        """处理客户端消息"""
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return {"type": "error", "message": "无效的JSON格式"}
        
        msg_type = data.get("type")
        
        if msg_type == "subscribe":
            # 订阅工作流运行
            run_id = data.get("run_id")
            if run_id:
                await self.subscribe(connection_id, run_id)
                return {"type": "response", "action": "subscribe", "success": True}
        
        elif msg_type == "unsubscribe":
            # 取消订阅
            run_id = data.get("run_id")
            if run_id:
                await self.unsubscribe(connection_id, run_id)
                return {"type": "response", "action": "unsubscribe", "success": True}
        
        elif msg_type == "ping":
            # 心跳
            return {"type": "pong", "timestamp": datetime.now().isoformat()}
        
        elif msg_type == "get_subscriptions":
            # 获取当前订阅
            if connection_id in self._connection_info:
                subs = list(self._connection_info[connection_id]["subscriptions"])
                return {"type": "response", "subscriptions": subs}
        
        return {"type": "error", "message": f"未知消息类型: {msg_type}"}
    
    def get_connection_count(self) -> int:
        """获取连接数"""
        return len(self._connections)
    
    def get_subscription_stats(self) -> Dict:
        """获取订阅统计"""
        return {
            "total_connections": len(self._connections),
            "total_subscriptions": sum(
                len(subs) for subs in self._subscriptions.values()
            ),
            "active_runs": len(self._subscriptions)
        }


class StatusBroadcaster:
    """状态广播器 - 连接调度器和WebSocket"""
    
    def __init__(self, ws_manager: WebSocketManager):
        self.ws_manager = ws_manager
    
    async def on_status_change(
        self, 
        event_type: str, 
        run_id: str, 
        data: dict
    ) -> None:
        """状态变化回调（供调度器调用）"""
        await self.ws_manager.broadcast_to_run(run_id, event_type, data)
