/**
 * WebSocket客户端 - 实时通信
 */
class WebSocketClient {
    constructor(url) {
        this.url = url;
        this.ws = null;
        this.connectionId = null;
        this.isConnected = false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        this.reconnectDelay = 1000;
        
        // 事件处理
        this.handlers = {};
        
        // 心跳
        this.heartbeatInterval = null;
        this.heartbeatTimeout = 30000;
        
        // 消息队列（断连时缓存）
        this.messageQueue = [];
    }
    
    connect() {
        return new Promise((resolve, reject) => {
            try {
                this.ws = new WebSocket(this.url);
                
                this.ws.onopen = () => {
                    console.log('WebSocket连接成功');
                    this.isConnected = true;
                    this.reconnectAttempts = 0;
                    
                    this.emit('connected');
                    this.startHeartbeat();
                    this.flushMessageQueue();
                    
                    resolve();
                };
                
                this.ws.onmessage = (event) => {
                    this.handleMessage(event.data);
                };
                
                this.ws.onerror = (error) => {
                    console.error('WebSocket错误:', error);
                    this.emit('error', error);
                    reject(error);
                };
                
                this.ws.onclose = () => {
                    console.log('WebSocket连接关闭');
                    this.isConnected = false;
                    this.stopHeartbeat();
                    this.emit('disconnected');
                    
                    // 尝试重连
                    this.scheduleReconnect();
                };
                
            } catch (error) {
                reject(error);
            }
        });
    }
    
    disconnect() {
        this.stopHeartbeat();
        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
    }
    
    // ==================== 消息处理 ====================
    
    handleMessage(data) {
        try {
            const message = JSON.parse(data);
            
            // 连接确认
            if (message.type === 'connected') {
                this.connectionId = message.connection_id;
                this.emit('connection_established', message);
                return;
            }
            
            // 事件消息
            if (message.type === 'event') {
                this.emit(message.event, message.data);
                return;
            }
            
            // 响应消息
            if (message.type === 'response') {
                this.emit('response', message);
                return;
            }
            
            // 心跳响应
            if (message.type === 'pong') {
                this.emit('pong', message);
                return;
            }
            
            // 错误消息
            if (message.type === 'error') {
                this.emit('error', message);
                return;
            }
            
        } catch (error) {
            console.error('解析消息失败:', error);
        }
    }
    
    // ==================== 发送消息 ====================
    
    send(message) {
        const data = typeof message === 'string' ? message : JSON.stringify(message);
        
        if (this.isConnected && this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(data);
            return true;
        } else {
            // 缓存消息
            this.messageQueue.push(data);
            return false;
        }
    }
    
    flushMessageQueue() {
        while (this.messageQueue.length > 0) {
            const message = this.messageQueue.shift();
            this.send(message);
        }
    }
    
    // ==================== 订阅 ====================
    
    subscribe(runId) {
        return this.send({
            type: 'subscribe',
            run_id: runId
        });
    }
    
    unsubscribe(runId) {
        return this.send({
            type: 'unsubscribe',
            run_id: runId
        });
    }
    
    ping() {
        return this.send({
            type: 'ping'
        });
    }
    
    getSubscriptions() {
        return this.send({
            type: 'get_subscriptions'
        });
    }
    
    // ==================== 心跳 ====================
    
    startHeartbeat() {
        this.stopHeartbeat();
        this.heartbeatInterval = setInterval(() => {
            if (this.isConnected) {
                this.ping();
            }
        }, this.heartbeatTimeout);
    }
    
    stopHeartbeat() {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
            this.heartbeatInterval = null;
        }
    }
    
    // ==================== 重连 ====================
    
    scheduleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.log('达到最大重连次数，停止重连');
            this.emit('reconnect_failed');
            return;
        }
        
        this.reconnectAttempts++;
        const delay = this.reconnectDelay * Math.pow(1.5, this.reconnectAttempts - 1);
        
        console.log(`${delay}ms后尝试第${this.reconnectAttempts}次重连...`);
        this.emit('reconnecting', { attempt: this.reconnectAttempts, delay });
        
        setTimeout(() => {
            this.connect().catch(() => {});
        }, delay);
    }
    
    // ==================== 事件系统 ====================
    
    on(event, handler) {
        if (!this.handlers[event]) {
            this.handlers[event] = [];
        }
        this.handlers[event].push(handler);
        return this;
    }
    
    off(event, handler) {
        if (this.handlers[event]) {
            const index = this.handlers[event].indexOf(handler);
            if (index > -1) {
                this.handlers[event].splice(index, 1);
            }
        }
        return this;
    }
    
    emit(event, data) {
        if (this.handlers[event]) {
            this.handlers[event].forEach(handler => {
                try {
                    handler(data);
                } catch (error) {
                    console.error(`事件处理器错误 (${event}):`, error);
                }
            });
        }
    }
}

// 导出
window.WebSocketClient = WebSocketClient;
