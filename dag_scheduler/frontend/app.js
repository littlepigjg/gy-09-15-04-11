/**
 * 主应用入口
 */
class App {
    constructor() {
        // 初始化组件
        this.renderer = new DAGRenderer(document.getElementById('dagCanvas'));
        this.editor = new DAGEditor(this.renderer);
        
        // WebSocket
        this.ws = new WebSocketClient('ws://localhost:8765');
        
        // 当前运行
        this.currentRunId = null;
        this.subscribedRuns = new Set();
        
        // API基础路径
        this.apiBase = '';
        
        this.init();
    }
    
    init() {
        this.bindEvents();
        this.connectWebSocket();
        this.loadWorkflows();
    }
    
    // ==================== 事件绑定 ====================
    
    bindEvents() {
        // 新建工作流
        document.getElementById('btnNewWorkflow').addEventListener('click', () => {
            this.showNewWorkflowModal();
        });
        
        document.getElementById('btnConfirmNew').addEventListener('click', () => {
            const name = document.getElementById('newWorkflowName').value.trim();
            if (name) {
                this.createNewWorkflow(name);
                this.hideNewWorkflowModal();
            }
        });
        
        document.getElementById('btnCancelNew').addEventListener('click', () => {
            this.hideNewWorkflowModal();
        });
        
        document.getElementById('btnCloseNewModal').addEventListener('click', () => {
            this.hideNewWorkflowModal();
        });
        
        // 工具栏按钮
        document.getElementById('btnRunWorkflow').addEventListener('click', () => {
            this.runWorkflow();
        });
        
        document.getElementById('btnStopWorkflow').addEventListener('click', () => {
            this.stopWorkflow();
        });
        
        document.getElementById('btnAutoLayout').addEventListener('click', () => {
            this.editor.autoLayout();
        });
        
        document.getElementById('btnZoomIn').addEventListener('click', () => {
            this.renderer.zoomIn();
        });
        
        document.getElementById('btnZoomOut').addEventListener('click', () => {
            this.renderer.zoomOut();
        });
        
        document.getElementById('btnFitView').addEventListener('click', () => {
            this.renderer.fitView(this.editor.tasks);
        });
        
        document.getElementById('btnSave').addEventListener('click', () => {
            this.saveWorkflow();
        });
        
        document.getElementById('btnValidate').addEventListener('click', () => {
            this.validateWorkflow();
        });
        
        // 刷新按钮
        document.getElementById('btnRefreshWorkflows').addEventListener('click', () => {
            this.loadWorkflows();
        });
        
        document.getElementById('btnRefreshRuns').addEventListener('click', () => {
            this.loadRunHistory();
        });
        
        // 属性面板
        document.getElementById('btnCloseProperties').addEventListener('click', () => {
            document.getElementById('taskProperties').style.display = 'none';
            document.getElementById('workflowProperties').style.display = 'block';
        });
        
        document.getElementById('btnDeleteTask').addEventListener('click', () => {
            if (this.editor.selectedTaskId) {
                if (confirm('确定要删除这个任务吗？')) {
                    this.editor.deleteTask(this.editor.selectedTaskId);
                    document.getElementById('taskProperties').style.display = 'none';
                    document.getElementById('workflowProperties').style.display = 'block';
                }
            }
        });
        
        // 任务表单提交
        document.getElementById('taskForm').addEventListener('submit', (e) => {
            e.preventDefault();
            this.saveTaskProperties();
        });
        
        // 工作流表单
        document.getElementById('wfName').addEventListener('change', (e) => {
            if (this.editor.currentWorkflow) {
                this.editor.currentWorkflow.name = e.target.value;
                document.getElementById('workflowName').textContent = e.target.value;
            }
        });
        
        // 日志面板
        document.getElementById('btnClearLog').addEventListener('click', () => {
            document.getElementById('logOutput').innerHTML = '';
        });
        
        document.getElementById('btnToggleLog').addEventListener('click', () => {
            const logPanel = document.getElementById('logPanel');
            const btn = document.getElementById('btnToggleLog');
            if (logPanel.style.height === '40px') {
                logPanel.style.height = '200px';
                btn.textContent = '-';
            } else {
                logPanel.style.height = '40px';
                btn.textContent = '+';
            }
        });
        
        // 日志标签页
        document.querySelectorAll('.log-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('.log-tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                // TODO: 切换日志内容
            });
        });
        
        // 拖拽添加任务
        document.querySelectorAll('.toolbox-item').forEach(item => {
            item.addEventListener('dragstart', (e) => {
                e.dataTransfer.setData('taskType', item.dataset.type);
            });
        });
        
        document.getElementById('dagCanvas').addEventListener('dragover', (e) => {
            e.preventDefault();
        });
        
        document.getElementById('dagCanvas').addEventListener('drop', (e) => {
            e.preventDefault();
            const type = e.dataTransfer.getData('taskType');
            if (type) {
                const rect = e.target.getBoundingClientRect();
                const x = (e.clientX - rect.left) / this.renderer.scale - this.renderer.translateX / this.renderer.scale;
                const y = (e.clientY - rect.top) / this.renderer.scale - this.renderer.translateY / this.renderer.scale;
                
                this.editor.addTask(type, { x, y });
            }
        });
        
        // 运行详情关闭
        document.getElementById('btnCloseRunDetails').addEventListener('click', () => {
            document.getElementById('runDetails').style.display = 'none';
            document.getElementById('workflowProperties').style.display = 'block';
        });
        
        // 编辑器回调
        this.editor.onTaskSelect = (task) => {
            if (task) {
                this.editor.openTaskProperties(task.id);
            }
        };
        
        this.editor.onWorkflowChange = (data) => {
            this.markUnsaved();
        };
    }
    
    // ==================== WebSocket ====================
    
    async connectWebSocket() {
        try {
            await this.ws.connect();
            
            this.ws.on('connected', () => {
                this.updateConnectionStatus(true);
                this.log('系统', '已连接到调度服务器', 'info');
            });
            
            this.ws.on('disconnected', () => {
                this.updateConnectionStatus(false);
                this.log('系统', '与服务器断开连接', 'warning');
            });
            
            this.ws.on('reconnecting', (data) => {
                this.log('系统', `正在尝试第${data.attempt}次重连...`, 'info');
            });
            
            // 任务事件
            this.ws.on('task_start', (data) => {
                this.log(data.task_id, `任务开始执行 (第${data.attempt}次尝试)`, 'info');
                this.renderer.updateNodeStatus(data.task_id, 'running');
            });
            
            this.ws.on('task_complete', (data) => {
                this.log(data.task_id, '任务执行完成', 'success');
                this.renderer.updateNodeStatus(data.task_id, 'success');
            });
            
            this.ws.on('task_error', (data) => {
                this.log(data.task_id, `任务执行失败: ${data.error}`, 'error');
                this.renderer.updateNodeStatus(data.task_id, 'failed');
            });
            
            this.ws.on('task_timeout', (data) => {
                this.log(data.task_id, `任务执行超时 (${data.timeout}秒)`, 'warning');
                this.renderer.updateNodeStatus(data.task_id, 'timeout');
            });
            
            this.ws.on('task_retry', (data) => {
                this.log(data.task_id, `重试中 (${data.attempt}/${data.max_retries})`, 'info');
                this.renderer.updateNodeStatus(data.task_id, 'retry');
            });
            
            this.ws.on('task_log', (data) => {
                this.log(data.task_id, data.log, 'output');
            });
            
            // 工作流事件
            this.ws.on('workflow_start', (data) => {
                this.log('系统', `工作流开始执行: ${data.workflow_id}`, 'info');
                this.updateRunStatus(data);
            });
            
            this.ws.on('workflow_complete', (data) => {
                this.log('系统', `工作流执行完成: ${data.status}`, data.status === 'success' ? 'success' : 'error');
                this.updateRunStatus(data);
                this.loadRunHistory();
            });
            
            this.ws.on('workflow_error', (data) => {
                this.log('系统', `工作流执行错误: ${data.error}`, 'error');
            });
            
        } catch (error) {
            this.log('系统', '连接服务器失败', 'error');
        }
    }
    
    updateConnectionStatus(connected) {
        const statusEl = document.getElementById('wsStatus');
        const dot = statusEl.querySelector('.status-dot');
        const text = statusEl.querySelector('span:last-child');
        
        if (connected) {
            dot.classList.remove('disconnected');
            text.textContent = '已连接';
        } else {
            dot.classList.add('disconnected');
            text.textContent = '未连接';
        }
    }
    
    // ==================== API调用 ====================
    
    async api(url, options = {}) {
        try {
            const response = await fetch(`${this.apiBase}${url}`, {
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                },
                ...options
            });
            
            const data = await response.json();
            
            if (!response.ok) {
                throw new Error(data.error || '请求失败');
            }
            
            return data;
        } catch (error) {
            this.log('系统', `API错误: ${error.message}`, 'error');
            throw error;
        }
    }
    
    async loadWorkflows() {
        try {
            const data = await this.api('/api/workflows');
            this.renderWorkflowList(data.workflows);
        } catch (error) {
            console.error('加载工作流失败:', error);
        }
    }
    
    async loadWorkflow(workflowId) {
        try {
            const data = await this.api(`/api/workflows/${workflowId}`);
            this.editor.loadWorkflow(data);
            this.updateWorkflowForm(data);
            this.updateToolbar(true);
        } catch (error) {
            console.error('加载工作流详情失败:', error);
        }
    }
    
    async saveWorkflow() {
        if (!this.editor.currentWorkflow) {
            this.log('系统', '没有可保存的工作流', 'warning');
            return;
        }
        
        try {
            const data = this.editor.getWorkflowData();
            
            const response = await this.api(`/api/workflows/${data.id}`, {
                method: 'PUT',
                body: JSON.stringify(data)
            });
            
            this.log('系统', '工作流保存成功', 'success');
            this.unsavedChanges = false;
            
        } catch (error) {
            this.log('系统', `保存失败: ${error.message}`, 'error');
        }
    }
    
    async createWorkflow(name) {
        try {
            const workflow = this.editor.createNewWorkflow(name);
            
            const response = await this.api('/api/workflows', {
                method: 'POST',
                body: JSON.stringify(workflow)
            });
            
            this.log('系统', `工作流创建成功: ${name}`, 'success');
            this.loadWorkflows();
            
        } catch (error) {
            this.log('系统', `创建失败: ${error.message}`, 'error');
        }
    }
    
    async validateWorkflow() {
        if (!this.editor.currentWorkflow) return;
        
        try {
            // 先保存
            await this.saveWorkflow();
            
            const data = await this.api(`/api/workflows/${this.editor.currentWorkflow.id}/validate`, {
                method: 'POST'
            });
            
            if (data.valid) {
                this.log('系统', 'DAG验证通过', 'success');
            } else {
                this.log('系统', `DAG验证失败: ${data.errors.join('; ')}`, 'error');
            }
        } catch (error) {
            this.log('系统', `验证失败: ${error.message}`, 'error');
        }
    }
    
    async runWorkflow() {
        if (!this.editor.currentWorkflow) return;
        
        try {
            // 先保存
            await this.saveWorkflow();
            
            const data = await this.api(`/api/workflows/${this.editor.currentWorkflow.id}/run`, {
                method: 'POST'
            });
            
            this.currentRunId = data.run.run_id;
            this.log('系统', `工作流开始运行: ${this.currentRunId}`, 'info');
            
            // 订阅运行
            this.ws.subscribe(this.currentRunId);
            this.subscribedRuns.add(this.currentRunId);
            
            // 显示运行详情
            this.showRunDetails(data.run);
            
        } catch (error) {
            this.log('系统', `启动失败: ${error.message}`, 'error');
        }
    }
    
    async stopWorkflow() {
        if (!this.currentRunId) return;
        
        try {
            await this.api(`/api/runs/${this.currentRunId}/stop`, {
                method: 'POST'
            });
            
            this.log('系统', '工作流已停止', 'warning');
            
        } catch (error) {
            this.log('系统', `停止失败: ${error.message}`, 'error');
        }
    }
    
    async loadRunHistory() {
        try {
            const data = await this.api('/api/runs');
            this.renderRunHistory(data.runs);
        } catch (error) {
            console.error('加载运行历史失败:', error);
        }
    }
    
    // ==================== 渲染 ====================
    
    renderWorkflowList(workflows) {
        const container = document.getElementById('workflowList');
        
        if (!workflows || workflows.length === 0) {
            container.innerHTML = '<div class="empty-state">暂无工作流</div>';
            return;
        }
        
        container.innerHTML = workflows.map(wf => `
            <div class="list-item" data-id="${wf.id}">
                <div class="list-item-title">${wf.name}</div>
                <div class="list-item-meta">
                    ${wf.task_count} 个任务 · ${wf.schedule || '无调度'}
                </div>
            </div>
        `).join('');
        
        // 绑定点击事件
        container.querySelectorAll('.list-item').forEach(item => {
            item.addEventListener('click', () => {
                container.querySelectorAll('.list-item').forEach(i => i.classList.remove('active'));
                item.classList.add('active');
                this.loadWorkflow(item.dataset.id);
            });
        });
    }
    
    renderRunHistory(runs) {
        const container = document.getElementById('runHistory');
        
        if (!runs || runs.length === 0) {
            container.innerHTML = '<div class="empty-state">暂无运行记录</div>';
            return;
        }
        
        container.innerHTML = runs.slice(0, 20).map(run => `
            <div class="list-item" data-run-id="${run.run_id}">
                <div class="list-item-title">
                    <span class="status-badge ${run.status}">${run.status}</span>
                    ${run.run_id}
                </div>
                <div class="list-item-meta">
                    ${run.start_time ? new Date(run.start_time).toLocaleString() : '-'}
                </div>
            </div>
        `).join('');
        
        // 绑定点击事件
        container.querySelectorAll('.list-item').forEach(item => {
            item.addEventListener('click', () => {
                this.viewRunDetails(item.dataset.runId);
            });
        });
    }
    
    updateWorkflowForm(workflow) {
        document.getElementById('wfName').value = workflow.name;
        document.getElementById('wfMaxConcurrency').value = workflow.max_concurrency;
        document.getElementById('wfSchedule').value = workflow.schedule || '';
        document.getElementById('wfTaskCount').value = workflow.tasks.length;
    }
    
    updateToolbar(hasWorkflow) {
        document.getElementById('btnRunWorkflow').disabled = !hasWorkflow;
        document.getElementById('btnStopWorkflow').disabled = true;
    }
    
    showRunDetails(run) {
        document.getElementById('runDetails').style.display = 'block';
        document.getElementById('workflowProperties').style.display = 'none';
        
        document.getElementById('runId').textContent = run.run_id;
        document.getElementById('runStatus').textContent = run.status;
        document.getElementById('runStatus').className = `value status-badge ${run.status}`;
        document.getElementById('runStartTime').textContent = run.start_time 
            ? new Date(run.start_time).toLocaleString() : '-';
        document.getElementById('runEndTime').textContent = run.end_time 
            ? new Date(run.end_time).toLocaleString() : '-';
        
        // 渲染任务状态
        const taskList = document.getElementById('taskStatusList');
        if (run.task_instances) {
            taskList.innerHTML = Object.entries(run.task_instances).map(([taskId, instance]) => `
                <div class="task-status-item">
                    <span>${taskId}</span>
                    <span class="status-badge ${instance.status}">${instance.status}</span>
                </div>
            `).join('');
        }
    }
    
    updateRunStatus(data) {
        if (document.getElementById('runDetails').style.display !== 'none') {
            document.getElementById('runStatus').textContent = data.status;
            document.getElementById('runStatus').className = `value status-badge ${data.status}`;
        }
        
        // 更新节点状态
        if (data.task_instances) {
            Object.entries(data.task_instances).forEach(([taskId, instance]) => {
                this.renderer.updateNodeStatus(taskId, instance.status);
            });
        }
    }
    
    async viewRunDetails(runId) {
        try {
            const data = await this.api(`/api/runs/${runId}`);
            this.showRunDetails(data);
            
            // 订阅日志
            this.ws.subscribe(runId);
            this.subscribedRuns.add(runId);
            
        } catch (error) {
            this.log('系统', `加载运行详情失败: ${error.message}`, 'error');
        }
    }
    
    // ==================== 任务属性 ====================
    
    saveTaskProperties() {
        if (!this.editor.selectedTaskId) return;
        
        const updates = {
            name: document.getElementById('taskName').value,
            command: document.getElementById('taskCommand').value,
            timeout: parseInt(document.getElementById('taskTimeout').value) || 300,
            max_retries: parseInt(document.getElementById('taskMaxRetries').value) || 3,
            retry_delay: parseInt(document.getElementById('taskRetryDelay').value) || 5
        };
        
        this.editor.updateTask(this.editor.selectedTaskId, updates);
        this.log('系统', '任务属性已更新', 'success');
    }
    
    // ==================== 日志 ====================
    
    log(source, message, type = 'info') {
        const output = document.getElementById('logOutput');
        const line = document.createElement('div');
        line.className = `log-line ${type}`;
        
        const timestamp = new Date().toLocaleTimeString();
        line.textContent = `[${timestamp}] [${source}] ${message}`;
        
        output.appendChild(line);
        output.scrollTop = output.scrollHeight;
    }
    
    // ==================== 模态框 ====================
    
    showNewWorkflowModal() {
        document.getElementById('newWorkflowModal').classList.add('active');
        document.getElementById('newWorkflowName').value = '';
        document.getElementById('newWorkflowName').focus();
    }
    
    hideNewWorkflowModal() {
        document.getElementById('newWorkflowModal').classList.remove('active');
    }
    
    // ==================== 工具 ====================
    
    markUnsaved() {
        document.title = '* DAG工作流调度引擎';
    }
    
    unsaved() {
        return this.unsavedChanges;
    }
}

// 启动应用
document.addEventListener('DOMContentLoaded', () => {
    window.app = new App();
});
