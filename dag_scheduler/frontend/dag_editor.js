/**
 * DAG编辑器 - 核心编辑逻辑
 */
class DAGEditor {
    constructor(renderer) {
        this.renderer = renderer;
        
        // 当前工作流
        this.currentWorkflow = null;
        this.tasks = [];
        this.selectedTaskId = null;
        
        // 拖拽状态
        this.isDraggingNode = false;
        this.draggedNode = null;
        this.dragOffset = { x: 0, y: 0 };
        
        // 连接模式
        this.isConnecting = false;
        this.connectingFrom = null;
        
        // 事件回调
        this.onTaskSelect = null;
        this.onTaskUpdate = null;
        this.onWorkflowChange = null;
        
        this.initEvents();
    }
    
    initEvents() {
        const canvas = this.renderer.svg;
        
        // 点击画布取消选择
        canvas.addEventListener('click', (e) => {
            if (e.target === canvas || e.target.id === 'canvasGroup') {
                this.deselectAll();
            }
        });
        
        // 节点点击和拖拽
        this.renderer.nodesLayer.addEventListener('mousedown', (e) => {
            const nodeEl = e.target.closest('.dag-node');
            if (!nodeEl) return;
            
            const taskId = nodeEl.dataset.taskId;
            
            // 右键菜单
            if (e.button === 2) {
                e.preventDefault();
                this.showContextMenu(taskId, e.clientX, e.clientY);
                return;
            }
            
            // 选择节点
            this.selectTask(taskId);
            
            // 开始拖拽
            this.isDraggingNode = true;
            this.draggedNode = nodeEl;
            
            const pos = this.renderer.getNodePosition(taskId);
            this.dragOffset = {
                x: e.clientX / this.renderer.scale - pos.x,
                y: e.clientY / this.renderer.scale - pos.y
            };
            
            e.stopPropagation();
        });
        
        // 鼠标移动
        document.addEventListener('mousemove', (e) => {
            if (this.isDraggingNode && this.draggedNode) {
                const taskId = this.draggedNode.dataset.taskId;
                const newX = e.clientX / this.renderer.scale - this.dragOffset.x;
                const newY = e.clientY / this.renderer.scale - this.dragOffset.y;
                
                this.renderer.updateNodePosition(taskId, newX, newY);
                this.updateNodeData(taskId, { x: newX, y: newY });
                this.refreshConnections();
            }
        });
        
        // 鼠标释放
        document.addEventListener('mouseup', () => {
            if (this.isDraggingNode) {
                this.isDraggingNode = false;
                this.draggedNode = null;
            }
        });
        
        // 双击编辑节点
        this.renderer.nodesLayer.addEventListener('dblclick', (e) => {
            const nodeEl = e.target.closest('.dag-node');
            if (nodeEl) {
                const taskId = nodeEl.dataset.taskId;
                this.openTaskProperties(taskId);
            }
        });
        
        // 禁用右键菜单
        canvas.addEventListener('contextmenu', (e) => {
            e.preventDefault();
        });
        
        // 键盘快捷键
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Delete' && this.selectedTaskId) {
                this.deleteTask(this.selectedTaskId);
            } else if (e.key === 'Escape') {
                this.deselectAll();
            }
        });
    }
    
    // ==================== 工作流管理 ====================
    
    loadWorkflow(workflow) {
        this.currentWorkflow = workflow;
        this.tasks = workflow.tasks || [];
        
        this.renderer.clear();
        this.renderAll();
        
        // 隐藏空状态
        document.getElementById('canvasEmpty').style.display = 'none';
        
        // 更新UI
        this.updateWorkflowInfo();
    }
    
    createNewWorkflow(name) {
        this.currentWorkflow = {
            id: this.generateId(),
            name: name || '未命名工作流',
            tasks: [],
            max_concurrency: 5,
            schedule: null
        };
        this.tasks = [];
        
        this.renderer.clear();
        document.getElementById('canvasEmpty').style.display = 'none';
        
        this.updateWorkflowInfo();
        this.emitChange();
        
        return this.currentWorkflow;
    }
    
    generateId() {
        return Math.random().toString(36).substring(2, 10);
    }
    
    // ==================== 任务管理 ====================
    
    addTask(type, position) {
        const taskTemplates = {
            'shell': { name: 'Shell命令', command: 'echo "Hello World"' },
            'python': { name: 'Python脚本', command: 'python -c "print(1)"' },
            'http': { name: 'HTTP请求', command: 'curl http://example.com' }
        };
        
        const template = taskTemplates[type] || taskTemplates['shell'];
        
        const task = {
            id: this.generateId(),
            name: template.name,
            command: template.command,
            dependencies: [],
            timeout: 300,
            max_retries: 3,
            retry_delay: 5,
            position: position || { x: 100, y: 100 }
        };
        
        this.tasks.push(task);
        this.renderer.renderNode(task);
        this.updateWorkflowInfo();
        this.emitChange();
        
        return task;
    }
    
    updateTask(taskId, updates) {
        const task = this.tasks.find(t => t.id === taskId);
        if (!task) return;
        
        Object.assign(task, updates);
        this.renderer.renderNode(task);
        this.refreshConnections();
        this.emitChange();
    }
    
    updateNodeData(taskId, position) {
        const task = this.tasks.find(t => t.id === taskId);
        if (task) {
            task.position = position;
        }
    }
    
    deleteTask(taskId) {
        const index = this.tasks.findIndex(t => t.id === taskId);
        if (index === -1) return;
        
        // 移除依赖此任务的引用
        this.tasks.forEach(t => {
            const depIndex = t.dependencies.indexOf(taskId);
            if (depIndex > -1) {
                t.dependencies.splice(depIndex, 1);
            }
        });
        
        // 移除任务
        this.tasks.splice(index, 1);
        
        // 清除节点和连接
        const nodeEl = document.getElementById(`node-${taskId}`);
        if (nodeEl) nodeEl.remove();
        
        this.refreshConnections();
        
        if (this.selectedTaskId === taskId) {
            this.selectedTaskId = null;
        }
        
        this.updateWorkflowInfo();
        this.emitChange();
    }
    
    addDependency(fromId, toId) {
        const task = this.tasks.find(t => t.id === toId);
        if (!task) return;
        
        // 检查是否已存在
        if (task.dependencies.includes(fromId)) return;
        
        // 检查循环依赖
        if (this.wouldCreateCycle(fromId, toId)) {
            alert('不能添加会导致循环依赖的连接');
            return;
        }
        
        task.dependencies.push(fromId);
        this.renderer.renderConnection(fromId, toId);
        this.emitChange();
    }
    
    removeDependency(fromId, toId) {
        const task = this.tasks.find(t => t.id === toId);
        if (!task) return;
        
        const index = task.dependencies.indexOf(fromId);
        if (index > -1) {
            task.dependencies.splice(index, 1);
        }
        
        this.renderer.removeConnection(fromId, toId);
        this.emitChange();
    }
    
    wouldCreateCycle(fromId, toId) {
        // BFS检查是否会形成环
        const visited = new Set();
        const queue = [toId];
        
        while (queue.length > 0) {
            const current = queue.shift();
            if (current === fromId) return true;
            if (visited.has(current)) continue;
            
            visited.add(current);
            const task = this.tasks.find(t => t.id === current);
            if (task) {
                queue.push(...task.dependencies);
            }
        }
        
        return false;
    }
    
    // ==================== 渲染 ====================
    
    renderAll() {
        this.tasks.forEach(task => {
            this.renderer.renderNode(task);
        });
        
        this.refreshConnections();
        
        // 自适应视图
        if (this.tasks.length > 0) {
            setTimeout(() => {
                this.renderer.fitView(this.tasks);
            }, 100);
        }
    }
    
    refreshConnections() {
        // 清除所有连接
        this.renderer.connectionsLayer.innerHTML = '';
        
        // 重新绘制
        this.tasks.forEach(task => {
            (task.dependencies || []).forEach(depId => {
                this.renderer.renderConnection(depId, task.id);
            });
        });
    }
    
    // ==================== 选择操作 ====================
    
    selectTask(taskId) {
        this.selectedTaskId = taskId;
        this.renderer.selectNode(taskId);
        
        if (this.onTaskSelect) {
            const task = this.tasks.find(t => t.id === taskId);
            this.onTaskSelect(task);
        }
    }
    
    deselectAll() {
        this.selectedTaskId = null;
        this.renderer.clearSelection();
        
        if (this.onTaskSelect) {
            this.onTaskSelect(null);
        }
    }
    
    // ==================== 属性面板 ====================
    
    openTaskProperties(taskId) {
        const task = this.tasks.find(t => t.id === taskId);
        if (!task) return;
        
        this.selectTask(taskId);
        
        // 填充表单
        document.getElementById('taskId').value = task.id;
        document.getElementById('taskName').value = task.name;
        document.getElementById('taskCommand').value = task.command;
        document.getElementById('taskTimeout').value = task.timeout;
        document.getElementById('taskMaxRetries').value = task.max_retries;
        document.getElementById('taskRetryDelay').value = task.retry_delay;
        
        // 渲染依赖列表
        this.renderDependenciesList(task);
        
        // 显示面板
        document.getElementById('taskProperties').style.display = 'block';
        document.getElementById('workflowProperties').style.display = 'none';
    }
    
    renderDependenciesList(task) {
        const container = document.getElementById('taskDependencies');
        container.innerHTML = '';
        
        this.tasks.forEach(t => {
            if (t.id === task.id) return;
            
            const div = document.createElement('div');
            div.className = 'dep-item';
            
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.checked = task.dependencies.includes(t.id);
            checkbox.addEventListener('change', (e) => {
                if (e.target.checked) {
                    this.addDependency(t.id, task.id);
                } else {
                    this.removeDependency(t.id, task.id);
                }
            });
            
            const label = document.createElement('span');
            label.textContent = `${t.name} (${t.id})`;
            
            div.appendChild(checkbox);
            div.appendChild(label);
            container.appendChild(div);
        });
    }
    
    // ==================== 自动布局 ====================
    
    autoLayout() {
        if (this.tasks.length === 0) return;
        
        const positions = this.renderer.autoLayout(this.tasks);
        
        this.tasks.forEach(task => {
            if (positions[task.id]) {
                task.position = positions[task.id];
            }
        });
        
        this.renderAll();
        this.emitChange();
    }
    
    // ==================== 事件 ====================
    
    emitChange() {
        if (this.onWorkflowChange) {
            this.onWorkflowChange(this.getWorkflowData());
        }
    }
    
    getWorkflowData() {
        return {
            ...this.currentWorkflow,
            tasks: this.tasks
        };
    }
    
    // ==================== 右键菜单 ====================
    
    showContextMenu(taskId, x, y) {
        // 移除旧菜单
        const existing = document.querySelector('.context-menu');
        if (existing) existing.remove();
        
        const menu = document.createElement('div');
        menu.className = 'context-menu';
        menu.style.cssText = `
            position: fixed;
            left: ${x}px;
            top: ${y}px;
            background: var(--bg-medium);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            padding: 5px 0;
            z-index: 1000;
            min-width: 150px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        `;
        
        const items = [
            { text: '编辑', action: () => this.openTaskProperties(taskId) },
            { text: '删除', action: () => this.deleteTask(taskId), className: 'danger' },
            { text: '添加依赖到...', action: () => this.startConnectMode(taskId) }
        ];
        
        items.forEach(item => {
            const btn = document.createElement('div');
            btn.textContent = item.text;
            btn.style.cssText = `
                padding: 8px 15px;
                cursor: pointer;
                color: ${item.className === 'danger' ? 'var(--danger-color)' : 'var(--text-primary)'};
            `;
            btn.addEventListener('mouseenter', () => {
                btn.style.background = 'var(--bg-lighter)';
            });
            btn.addEventListener('mouseleave', () => {
                btn.style.background = 'transparent';
            });
            btn.addEventListener('click', () => {
                item.action();
                menu.remove();
            });
            menu.appendChild(btn);
        });
        
        document.body.appendChild(menu);
        
        // 点击其他地方关闭
        setTimeout(() => {
            document.addEventListener('click', function closeMenu() {
                menu.remove();
                document.removeEventListener('click', closeMenu);
            });
        }, 0);
    }
    
    startConnectMode(fromTaskId) {
        this.isConnecting = true;
        this.connectingFrom = fromTaskId;
        
        // 改变光标
        this.renderer.svg.style.cursor = 'crosshair';
        
        // 提示用户
        alert('请点击目标任务节点完成连接');
        
        // 临时添加点击事件
        const handler = (e) => {
            const nodeEl = e.target.closest('.dag-node');
            if (nodeEl && nodeEl.dataset.taskId !== fromTaskId) {
                this.addDependency(fromTaskId, nodeEl.dataset.taskId);
            }
            
            this.isConnecting = false;
            this.connectingFrom = null;
            this.renderer.svg.style.cursor = 'grab';
            this.renderer.nodesLayer.removeEventListener('click', handler);
        };
        
        this.renderer.nodesLayer.addEventListener('click', handler);
    }
    
    // ==================== 更新UI ====================
    
    updateWorkflowInfo() {
        const nameEl = document.getElementById('workflowName');
        const countEl = document.getElementById('wfTaskCount');
        
        if (this.currentWorkflow) {
            nameEl.textContent = this.currentWorkflow.name;
        }
        
        if (countEl) {
            countEl.value = this.tasks.length;
        }
    }
}

// 导出
window.DAGEditor = DAGEditor;
