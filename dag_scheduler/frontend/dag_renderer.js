/**
 * 增强版DAG渲染器 - 小地图、动画、连线拖拽
 */
class DAGRenderer {
    constructor(svgElement) {
        this.svg = svgElement;
        this.canvasGroup = document.getElementById('canvasGroup');
        this.nodesLayer = document.getElementById('nodesLayer');
        this.connectionsLayer = document.getElementById('connectionsLayer');
        
        // 缩放和平移
        this.scale = 1;
        this.translateX = 0;
        this.translateY = 0;
        this.minScale = 0.1;
        this.maxScale = 3;
        
        // 节点尺寸
        this.nodeWidth = 180;
        this.nodeHeight = 70;
        
        // 连接线缓存
        this.connections = new Map();
        
        // 连线模式
        this.connectingMode = false;
        this.connectingFrom = null;
        this.tempLine = null;
        
        // 动画
        this.animationFrame = null;
        
        // 小地图
        this.minimap = null;
        this.minimapRect = null;
        
        this.initEvents();
        this.initMinimap();
    }
    
    initEvents() {
        // 鼠标滚轮缩放
        this.svg.addEventListener('wheel', (e) => {
            e.preventDefault();
            const delta = e.deltaY > 0 ? 0.9 : 1.1;
            const rect = this.svg.getBoundingClientRect();
            const centerX = e.clientX - rect.left;
            const centerY = e.clientY - rect.top;
            this.zoom(delta, centerX, centerY);
        });
        
        // 鼠标拖拽平移
        let isDragging = false;
        let startX, startY;
        
        this.svg.addEventListener('mousedown', (e) => {
            // 右键或中键拖拽
            if (e.button === 1 || e.button === 2) {
                e.preventDefault();
                isDragging = true;
                startX = e.clientX - this.translateX;
                startY = e.clientY - this.translateY;
                this.svg.style.cursor = 'grabbing';
                return;
            }
            
            // 左键点击空白区域拖拽
            if (e.target === this.svg || e.target.id === 'canvasGroup') {
                isDragging = true;
                startX = e.clientX - this.translateX;
                startY = e.clientY - this.translateY;
                this.svg.style.cursor = 'grabbing';
            }
        });
        
        document.addEventListener('mousemove', (e) => {
            if (isDragging) {
                this.translateX = e.clientX - startX;
                this.translateY = e.clientY - startY;
                this.updateTransform();
                this.updateMinimap();
            }
            
            // 连线模式
            if (this.connectingMode && this.tempLine) {
                const rect = this.svg.getBoundingClientRect();
                const x = (e.clientX - rect.left - this.translateX) / this.scale;
                const y = (e.clientY - rect.top - this.translateY) / this.scale;
                this.tempLine.setAttribute('x2', x);
                this.tempLine.setAttribute('y2', y);
            }
        });
        
        document.addEventListener('mouseup', () => {
            isDragging = false;
            this.svg.style.cursor = 'default';
        });
        
        // 右键菜单
        this.svg.addEventListener('contextmenu', (e) => {
            e.preventDefault();
        });
        
        // 键盘快捷键
        document.addEventListener('keydown', (e) => {
            // Ctrl+0: 重置视图
            if (e.ctrlKey && e.key === '0') {
                e.preventDefault();
                this.resetView();
            }
            // Ctrl++: 放大
            if (e.ctrlKey && (e.key === '+' || e.key === '=')) {
                e.preventDefault();
                this.zoomIn();
            }
            // Ctrl+-: 缩小
            if (e.ctrlKey && e.key === '-') {
                e.preventDefault();
                this.zoomOut();
            }
        });
    }
    
    updateTransform() {
        this.canvasGroup.setAttribute('transform', 
            `translate(${this.translateX}, ${this.translateY}) scale(${this.scale})`
        );
    }
    
    zoom(factor, centerX, centerY) {
        const newScale = Math.max(this.minScale, Math.min(this.maxScale, this.scale * factor));
        const scaleChange = newScale / this.scale;
        
        this.translateX = centerX - (centerX - this.translateX) * scaleChange;
        this.translateY = centerY - (centerY - this.translateY) * scaleChange;
        this.scale = newScale;
        
        this.updateTransform();
        this.updateMinimap();
    }
    
    setZoom(scale) {
        this.scale = Math.max(this.minScale, Math.min(this.maxScale, scale));
        this.updateTransform();
        this.updateMinimap();
    }
    
    zoomIn() {
        this.zoom(1.2, this.svg.clientWidth / 2, this.svg.clientHeight / 2);
    }
    
    zoomOut() {
        this.zoom(0.8, this.svg.clientWidth / 2, this.svg.clientHeight / 2);
    }
    
    resetView() {
        this.scale = 1;
        this.translateX = 0;
        this.translateY = 0;
        this.updateTransform();
        this.updateMinimap();
    }
    
    fitView(nodes) {
        if (!nodes || nodes.length === 0) return;
        
        let minX = Infinity, minY = Infinity;
        let maxX = -Infinity, maxY = -Infinity;
        
        nodes.forEach(node => {
            const x = node.position?.x || 0;
            const y = node.position?.y || 0;
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x + this.nodeWidth);
            maxY = Math.max(maxY, y + this.nodeHeight);
        });
        
        const padding = 100;
        const contentWidth = maxX - minX + padding * 2;
        const contentHeight = maxY - minY + padding * 2;
        
        const svgWidth = this.svg.clientWidth;
        const svgHeight = this.svg.clientHeight;
        
        this.scale = Math.min(
            svgWidth / contentWidth,
            svgHeight / contentHeight,
            1.5
        );
        
        this.translateX = (svgWidth - contentWidth * this.scale) / 2 - minX * this.scale + padding * this.scale;
        this.translateY = (svgHeight - contentHeight * this.scale) / 2 - minY * this.scale + padding * this.scale;
        
        this.updateTransform();
        this.updateMinimap();
    }
    
    // ==================== 小地图 ====================
    
    initMinimap() {
        // 创建小地图容器
        const minimapContainer = document.createElement('div');
        minimapContainer.className = 'minimap-container';
        minimapContainer.innerHTML = `
            <svg class="minimap" width="180" height="120">
                <rect class="minimap-bg" width="100%" height="100%" fill="#1e1e1e"/>
                <g class="minimap-content"></g>
                <rect class="minimap-viewport" fill="rgba(33, 150, 243, 0.3)" stroke="#2196F3" stroke-width="1"/>
            </svg>
        `;
        
        document.getElementById('canvasContainer').appendChild(minimapContainer);
        
        this.minimap = minimapContainer.querySelector('.minimap');
        this.minimapContent = minimapContainer.querySelector('.minimap-content');
        this.minimapViewport = minimapContainer.querySelector('.minimap-viewport');
        
        // 小地图点击定位
        this.minimap.addEventListener('click', (e) => {
            const rect = this.minimap.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;
            
            // 计算对应的主画布位置
            const minimapScale = this.calculateMinimapScale();
            const mainX = x / minimapScale;
            const mainY = y / minimapScale;
            
            // 移动视图
            const svgRect = this.svg.getBoundingClientRect();
            this.translateX = svgRect.width / 2 - mainX * this.scale;
            this.translateY = svgRect.height / 2 - mainY * this.scale;
            
            this.updateTransform();
            this.updateMinimap();
        });
    }
    
    calculateMinimapScale() {
        const bounds = this.getContentBounds();
        if (!bounds) return 0.1;
        
        const scaleX = 180 / (bounds.width + 200);
        const scaleY = 120 / (bounds.height + 200);
        return Math.min(scaleX, scaleY, 0.5);
    }
    
    getContentBounds() {
        const nodes = this.nodesLayer.querySelectorAll('.dag-node');
        if (nodes.length === 0) return null;
        
        let minX = Infinity, minY = Infinity;
        let maxX = -Infinity, maxY = -Infinity;
        
        nodes.forEach(node => {
            const transform = node.getAttribute('transform');
            const match = transform.match(/translate\(([^,]+),\s*([^)]+)\)/);
            if (match) {
                const x = parseFloat(match[1]);
                const y = parseFloat(match[2]);
                minX = Math.min(minX, x);
                minY = Math.min(minY, y);
                maxX = Math.max(maxX, x + this.nodeWidth);
                maxY = Math.max(maxY, y + this.nodeHeight);
            }
        });
        
        return { minX, minY, maxX, maxY, width: maxX - minX, height: maxY - minY };
    }
    
    updateMinimap() {
        if (!this.minimapContent) return;
        
        // 清空并重绘小地图内容
        this.minimapContent.innerHTML = '';
        
        const bounds = this.getContentBounds();
        if (!bounds) return;
        
        const minimapScale = this.calculateMinimapScale();
        const offsetX = (180 - bounds.width * minimapScale) / 2 - bounds.minX * minimapScale;
        const offsetY = (120 - bounds.height * minimapScale) / 2 - bounds.minY * minimapScale;
        
        // 绘制节点
        this.nodesLayer.querySelectorAll('.dag-node').forEach(node => {
            const transform = node.getAttribute('transform');
            const match = transform.match(/translate\(([^,]+),\s*([^)]+)\)/);
            if (match) {
                const x = parseFloat(match[1]) * minimapScale + offsetX;
                const y = parseFloat(match[2]) * minimapScale + offsetY;
                const w = this.nodeWidth * minimapScale;
                const h = this.nodeHeight * minimapScale;
                
                const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
                rect.setAttribute('x', x);
                rect.setAttribute('y', y);
                rect.setAttribute('width', w);
                rect.setAttribute('height', h);
                rect.setAttribute('fill', '#4d4d4d');
                rect.setAttribute('rx', 2);
                this.minimapContent.appendChild(rect);
            }
        });
        
        // 绘制连线
        this.connectionsLayer.querySelectorAll('.connection-line').forEach(line => {
            // 简化：不绘制连线，只绘制节点
        });
        
        // 更新视口指示器
        const svgRect = this.svg.getBoundingClientRect();
        const viewportX = (-this.translateX / this.scale) * minimapScale + offsetX;
        const viewportY = (-this.translateY / this.scale) * minimapScale + offsetY;
        const viewportW = (svgRect.width / this.scale) * minimapScale;
        const viewportH = (svgRect.height / this.scale) * minimapScale;
        
        this.minimapViewport.setAttribute('x', Math.max(0, viewportX));
        this.minimapViewport.setAttribute('y', Math.max(0, viewportY));
        this.minimapViewport.setAttribute('width', Math.min(180, viewportW));
        this.minimapViewport.setAttribute('height', Math.min(120, viewportH));
    }
    
    // ==================== 节点渲染 ====================
    
    renderNode(task, instance = null) {
        const existing = document.getElementById(`node-${task.id}`);
        if (existing) {
            existing.remove();
        }
        
        const x = task.position?.x || 0;
        const y = task.position?.y || 0;
        
        const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        g.id = `node-${task.id}`;
        g.classList.add('dag-node');
        g.setAttribute('transform', `translate(${x}, ${y})`);
        g.dataset.taskId = task.id;
        
        // 节点背景
        const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        rect.classList.add('node-body');
        rect.setAttribute('width', this.nodeWidth);
        rect.setAttribute('height', this.nodeHeight);
        
        // 根据任务类型设置不同颜色
        const typeColors = {
            'shell': '#4d4d4d',
            'python': '#3572A5',
            'http': '#e44d26',
            'subworkflow': '#9b59b6',
            'condition': '#f39c12',
            'parallel': '#27ae60',
            'merge': '#34495e'
        };
        rect.style.fill = typeColors[task.type] || '#4d4d4d';
        
        // 根据运行状态添加样式
        if (instance) {
            rect.classList.add(instance.status);
        }
        
        g.appendChild(rect);
        
        // 类型图标
        const icons = {
            'shell': '⚙',
            'python': '🐍',
            'http': '🌐',
            'subworkflow': '📦',
            'condition': '❓',
            'parallel': '⚡',
            'merge': '🔗'
        };
        
        const icon = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        icon.classList.add('node-icon');
        icon.setAttribute('x', 15);
        icon.setAttribute('y', 30);
        icon.textContent = icons[task.type] || '⚙';
        g.appendChild(icon);
        
        // 标题
        const title = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        title.classList.add('node-title');
        title.setAttribute('x', this.nodeWidth / 2 + 10);
        title.setAttribute('y', 25);
        title.textContent = task.name || task.id;
        g.appendChild(title);
        
        // 命令预览
        const subtitle = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        subtitle.classList.add('node-subtitle');
        subtitle.setAttribute('x', this.nodeWidth / 2 + 10);
        subtitle.setAttribute('y', 42);
        const cmdPreview = task.command && task.command.length > 15 
            ? task.command.substring(0, 15) + '...' 
            : (task.command || '');
        subtitle.textContent = cmdPreview;
        g.appendChild(subtitle);
        
        // 状态指示器
        if (instance && instance.status) {
            const statusColors = {
                'pending': '#9e9e9e',
                'waiting': '#ff9800',
                'running': '#2196f3',
                'success': '#4caf50',
                'failed': '#f44336',
                'timeout': '#ff5722',
                'skipped': '#9e9e9e'
            };
            
            const statusDot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            statusDot.setAttribute('cx', this.nodeWidth - 15);
            statusDot.setAttribute('cy', 15);
            statusDot.setAttribute('r', 6);
            statusDot.setAttribute('fill', statusColors[instance.status] || '#9e9e9e');
            statusDot.classList.add('status-indicator');
            g.appendChild(statusDot);
        }
        
        // 禁用标记
        if (task.enabled === false) {
            g.classList.add('disabled');
        }
        
        this.nodesLayer.appendChild(g);
        
        // 更新小地图
        this.updateMinimap();
        
        return g;
    }
    
    updateNodeStatus(taskId, status) {
        const node = document.getElementById(`node-${taskId}`);
        if (!node) return;
        
        const rect = node.querySelector('.node-body');
        if (rect) {
            rect.classList.remove(
                'pending', 'waiting', 'running', 'success', 'failed', 
                'timeout', 'retrying', 'skipped', 'cancelled'
            );
            rect.classList.add(status);
        }
        
        const statusDot = node.querySelector('.status-indicator');
        if (statusDot) {
            const statusColors = {
                'pending': '#9e9e9e',
                'waiting': '#ff9800',
                'running': '#2196f3',
                'success': '#4caf50',
                'failed': '#f44336',
                'timeout': '#ff5722',
                'skipped': '#9e9e9e'
            };
            statusDot.setAttribute('fill', statusColors[status] || '#9e9e9e');
            
            // 运行时添加脉冲动画
            if (status === 'running') {
                statusDot.classList.add('pulsing');
            } else {
                statusDot.classList.remove('pulsing');
            }
        }
    }
    
    selectNode(taskId) {
        this.nodesLayer.querySelectorAll('.dag-node.selected').forEach(n => {
            n.classList.remove('selected');
        });
        
        const node = document.getElementById(`node-${taskId}`);
        if (node) {
            node.classList.add('selected');
        }
    }
    
    clearSelection() {
        this.nodesLayer.querySelectorAll('.dag-node.selected').forEach(n => {
            n.classList.remove('selected');
        });
    }
    
    // ==================== 连接线渲染 ====================
    
    renderConnection(fromId, toId, isActive = false) {
        const key = `${fromId}-${toId}`;
        
        const existing = document.getElementById(`conn-${key}`);
        if (existing) {
            existing.remove();
        }
        
        const fromNode = document.getElementById(`node-${fromId}`);
        const toNode = document.getElementById(`node-${toId}`);
        
        if (!fromNode || !toNode) return;
        
        const fromTransform = fromNode.getAttribute('transform');
        const toTransform = toNode.getAttribute('transform');
        
        const fromX = parseFloat(fromTransform.match(/translate\(([^,]+)/)[1]) + this.nodeWidth;
        const fromY = parseFloat(fromTransform.match(/,\s*([^)]+)/)[1]) + this.nodeHeight / 2;
        
        const toX = parseFloat(toTransform.match(/translate\(([^,]+)/)[1]);
        const toY = parseFloat(toTransform.match(/,\s*([^)]+)/)[1]) + this.nodeHeight / 2;
        
        // 贝塞尔曲线
        const controlPointOffset = Math.max(50, Math.abs(toX - fromX) * 0.4);
        const path = `M ${fromX} ${fromY} C ${fromX + controlPointOffset} ${fromY}, ${toX - controlPointOffset} ${toY}, ${toX} ${toY}`;
        
        const pathElement = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        pathElement.id = `conn-${key}`;
        pathElement.classList.add('connection-line');
        pathElement.setAttribute('d', path);
        
        if (isActive) {
            pathElement.classList.add('active');
        }
        
        // 添加动画类
        pathElement.dataset.from = fromId;
        pathElement.dataset.to = toId;
        
        // 悬停效果
        pathElement.addEventListener('mouseenter', () => {
            pathElement.classList.add('hover');
        });
        pathElement.addEventListener('mouseleave', () => {
            pathElement.classList.remove('hover');
        });
        
        this.connectionsLayer.appendChild(pathElement);
        this.connections.set(key, { fromId, toId });
        
        return pathElement;
    }
    
    removeConnection(fromId, toId) {
        const key = `${fromId}-${toId}`;
        const element = document.getElementById(`conn-${key}`);
        if (element) {
            element.remove();
        }
        this.connections.delete(key);
    }
    
    animateConnection(fromId, toId) {
        const key = `${fromId}-${toId}`;
        const element = document.getElementById(`conn-${key}`);
        if (element) {
            element.classList.add('flowing');
        }
    }
    
    stopConnectionAnimation(fromId, toId) {
        const key = `${fromId}-${toId}`;
        const element = document.getElementById(`conn-${key}`);
        if (element) {
            element.classList.remove('flowing');
        }
    }
    
    // ==================== 连线模式 ====================
    
    startConnecting(fromTaskId) {
        this.connectingMode = true;
        this.connectingFrom = fromTaskId;
        
        const fromNode = document.getElementById(`node-${fromTaskId}`);
        if (!fromNode) return;
        
        const transform = fromNode.getAttribute('transform');
        const fromX = parseFloat(transform.match(/translate\(([^,]+)/)[1]) + this.nodeWidth;
        const fromY = parseFloat(transform.match(/,\s*([^)]+)/)[1]) + this.nodeHeight / 2;
        
        this.tempLine = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        this.tempLine.setAttribute('x1', fromX);
        this.tempLine.setAttribute('y1', fromY);
        this.tempLine.setAttribute('x2', fromX);
        this.tempLine.setAttribute('y2', fromY);
        this.tempLine.setAttribute('stroke', '#2196F3');
        this.tempLine.setAttribute('stroke-width', '2');
        this.tempLine.setAttribute('stroke-dasharray', '5,5');
        
        this.connectionsLayer.appendChild(this.tempLine);
    }
    
    finishConnecting(toTaskId) {
        if (!this.connectingMode || !this.connectingFrom) return null;
        
        const fromId = this.connectingFrom;
        
        // 清理临时线
        if (this.tempLine) {
            this.tempLine.remove();
            this.tempLine = null;
        }
        
        this.connectingMode = false;
        this.connectingFrom = null;
        
        return fromId !== toTaskId ? { from: fromId, to: toTaskId } : null;
    }
    
    cancelConnecting() {
        if (this.tempLine) {
            this.tempLine.remove();
            this.tempLine = null;
        }
        this.connectingMode = false;
        this.connectingFrom = null;
    }
    
    // ==================== 清空 ====================
    
    clear() {
        this.nodesLayer.innerHTML = '';
        this.connectionsLayer.innerHTML = '';
        this.connections.clear();
        this.updateMinimap();
    }
    
    // ==================== 自动布局 ====================
    
    autoLayout(tasks) {
        if (!tasks || tasks.length === 0) return;
        
        const levels = this.calculateLevels(tasks);
        
        const horizontalGap = 80;
        const verticalGap = 120;
        const startX = 100;
        const startY = 100;
        
        const positions = {};
        
        levels.forEach((level, levelIndex) => {
            const levelWidth = level.length * (this.nodeWidth + horizontalGap) - horizontalGap;
            const maxLevelWidth = this.getMaxLevelWidth(levels);
            const offsetX = startX + (maxLevelWidth - levelWidth) / 2;
            
            level.forEach((task, taskIndex) => {
                positions[task.id] = {
                    x: offsetX + taskIndex * (this.nodeWidth + horizontalGap),
                    y: startY + levelIndex * (this.nodeHeight + verticalGap)
                };
            });
        });
        
        return positions;
    }
    
    calculateLevels(tasks) {
        const taskMap = new Map(tasks.map(t => [t.id, t]));
        const inDegree = new Map();
        const adjacency = new Map();
        
        tasks.forEach(task => {
            inDegree.set(task.id, 0);
            adjacency.set(task.id, []);
        });
        
        tasks.forEach(task => {
            (task.dependencies || []).forEach(depId => {
                if (adjacency.has(depId)) {
                    adjacency.get(depId).push(task.id);
                    inDegree.set(task.id, (inDegree.get(task.id) || 0) + 1);
                }
            });
        });
        
        const levels = [];
        const queue = [];
        
        inDegree.forEach((degree, taskId) => {
            if (degree === 0) queue.push(taskId);
        });
        
        while (queue.length > 0) {
            const level = [...queue];
            levels.push(level.map(id => taskMap.get(id)));
            
            queue.length = 0;
            level.forEach(taskId => {
                (adjacency.get(taskId) || []).forEach(neighborId => {
                    const newDegree = inDegree.get(neighborId) - 1;
                    inDegree.set(neighborId, newDegree);
                    if (newDegree === 0) {
                        queue.push(neighborId);
                    }
                });
            });
        }
        
        return levels;
    }
    
    getMaxLevelWidth(levels) {
        return Math.max(...levels.map(level => 
            level.length * (this.nodeWidth + 80) - 80
        ), 0);
    }
    
    getNodePosition(taskId) {
        const node = document.getElementById(`node-${taskId}`);
        if (!node) return null;
        
        const transform = node.getAttribute('transform');
        return {
            x: parseFloat(transform.match(/translate\(([^,]+)/)[1]),
            y: parseFloat(transform.match(/,\s*([^)]+)/)[1])
        };
    }
    
    updateNodePosition(taskId, x, y) {
        const node = document.getElementById(`node-${taskId}`);
        if (node) {
            node.setAttribute('transform', `translate(${x}, ${y})`);
            this.updateMinimap();
        }
    }
}

// 导出
window.DAGRenderer = DAGRenderer;
