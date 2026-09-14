"""Flask主应用 - REST API + WebSocket"""
import os
import sys
import json
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from functools import wraps

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import websockets
import websockets.server

from models import WorkflowDefinition, TaskDefinition, generate_id
from dag_engine import DAGParser, DAGScheduler, DAGValidationError
from storage import AtomicJSONStorage
from scheduler import WorkflowRunner
from websocket_handler import WebSocketManager, StatusBroadcaster

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 路径配置
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
FRONTEND_DIR = BASE_DIR / "frontend"

# 初始化Flask应用
app = Flask(__name__, static_folder=str(FRONTEND_DIR))
CORS(app)

# 初始化存储
storage = AtomicJSONStorage(str(DATA_DIR))

# 初始化WebSocket管理器
ws_manager = WebSocketManager()
broadcaster = StatusBroadcaster(ws_manager)

# 初始化工作流运行器
runner = WorkflowRunner(storage)
runner.scheduler.on_status_change(broadcaster.on_status_change)


# ==================== REST API ====================

@app.route('/')
def index():
    """主页"""
    return send_from_directory(str(FRONTEND_DIR), 'index.html')

@app.route('/<path:path>')
def static_files(path):
    """静态文件"""
    return send_from_directory(str(FRONTEND_DIR), path)


# ---------- 工作流定义 API ----------

@app.route('/api/workflows', methods=['GET'])
def list_workflows():
    """列出所有工作流"""
    workflows = storage.list_workflows()
    return jsonify({"workflows": workflows})


@app.route('/api/workflows', methods=['POST'])
def create_workflow():
    """创建工作流"""
    data = request.get_json()
    
    if not data or 'name' not in data:
        return jsonify({"error": "缺少工作流名称"}), 400
    
    try:
        # 生成ID
        workflow_id = data.get('id') or generate_id()
        
        # 构建工作流数据
        workflow_data = {
            "id": workflow_id,
            "name": data['name'],
            "tasks": data.get('tasks', []),
            "schedule": data.get('schedule'),
            "max_concurrency": data.get('max_concurrency', 5)
        }
        
        # 解析并验证
        workflow = DAGParser.from_json(workflow_data)
        
        # 保存
        storage.save_workflow(workflow)
        
        return jsonify({
            "success": True,
            "workflow": workflow.to_dict()
        })
        
    except DAGValidationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"创建工作流失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/workflows/<workflow_id>', methods=['GET'])
def get_workflow(workflow_id):
    """获取工作流详情"""
    workflow = storage.load_workflow(workflow_id)
    if not workflow:
        return jsonify({"error": "工作流不存在"}), 404
    
    # 获取DAG信息
    dag_scheduler = DAGScheduler(workflow)
    sort_result = dag_scheduler.topological_sort()
    critical_path, path_length = dag_scheduler.calculate_critical_path()
    
    result = workflow.to_dict()
    result["dag_info"] = {
        "levels": sort_result.levels,
        "has_cycle": sort_result.has_cycle,
        "cycle_nodes": sort_result.cycle_nodes,
        "critical_path": critical_path,
        "critical_path_length": path_length
    }
    
    return jsonify(result)


@app.route('/api/workflows/<workflow_id>', methods=['PUT'])
def update_workflow(workflow_id):
    """更新工作流"""
    existing = storage.load_workflow(workflow_id)
    if not existing:
        return jsonify({"error": "工作流不存在"}), 404
    
    data = request.get_json()
    
    try:
        # 合并数据
        workflow_data = {
            "id": workflow_id,
            "name": data.get('name', existing.name),
            "tasks": data.get('tasks', [t.to_dict() for t in existing.tasks]),
            "schedule": data.get('schedule', existing.schedule),
            "max_concurrency": data.get('max_concurrency', existing.max_concurrency)
        }
        
        # 验证
        workflow = DAGParser.from_json(workflow_data)
        
        # 保存
        storage.save_workflow(workflow)
        
        return jsonify({
            "success": True,
            "workflow": workflow.to_dict()
        })
        
    except DAGValidationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"更新工作流失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/workflows/<workflow_id>', methods=['DELETE'])
def delete_workflow(workflow_id):
    """删除工作流"""
    if storage.delete_workflow(workflow_id):
        return jsonify({"success": True})
    return jsonify({"error": "工作流不存在"}), 404


@app.route('/api/workflows/<workflow_id>/validate', methods=['POST'])
def validate_workflow(workflow_id):
    """验证工作流"""
    workflow = storage.load_workflow(workflow_id)
    if not workflow:
        return jsonify({"error": "工作流不存在"}), 404
    
    dag_scheduler = DAGScheduler(workflow)
    errors = dag_scheduler.validate()
    
    if errors:
        return jsonify({
            "valid": False,
            "errors": errors
        })
    
    sort_result = dag_scheduler.topological_sort()
    return jsonify({
        "valid": True,
        "levels": sort_result.levels,
        "critical_path": dag_scheduler.calculate_critical_path()[0]
    })


# ---------- 工作流运行 API ----------

@app.route('/api/workflows/<workflow_id>/run', methods=['POST'])
def start_workflow_run(workflow_id):
    """启动工作流运行"""
    workflow = storage.load_workflow(workflow_id)
    if not workflow:
        return jsonify({"error": "工作流不存在"}), 404
    
    try:
        # 启动运行（同步方式，内部会创建线程）
        run = runner.scheduler.start_workflow_sync(workflow_id)
        
        return jsonify({
            "success": True,
            "run": run.to_dict()
        })
    except Exception as e:
        logger.error(f"启动工作流失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/runs/<run_id>', methods=['GET'])
def get_run(run_id):
    """获取运行详情"""
    run = storage.load_workflow_run(run_id)
    if not run:
        return jsonify({"error": "运行不存在"}), 404
    
    return jsonify(run.to_dict())


@app.route('/api/runs/<run_id>/stop', methods=['POST'])
def stop_workflow_run(run_id):
    """停止工作流运行"""
    try:
        loop = asyncio.new_event_loop()
        success = loop.run_until_complete(
            runner.scheduler.stop_workflow(run_id)
        )
        loop.close()
        
        if success:
            return jsonify({"success": True})
        return jsonify({"error": "运行不存在或已结束"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/runs/<run_id>/retry/<task_id>', methods=['POST'])
def retry_task(run_id, task_id):
    """重试任务"""
    try:
        loop = asyncio.new_event_loop()
        success = loop.run_until_complete(
            runner.scheduler.retry_task(run_id, task_id)
        )
        loop.close()
        
        if success:
            return jsonify({"success": True})
        return jsonify({"error": "无法重试任务"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/runs', methods=['GET'])
def list_runs():
    """列出运行记录"""
    workflow_id = request.args.get('workflow_id')
    runs = storage.list_workflow_runs(workflow_id)
    return jsonify({"runs": runs})


@app.route('/api/runs/<run_id>/logs/<task_id>', methods=['GET'])
def get_task_logs(run_id, task_id):
    """获取任务日志"""
    logs = storage.get_task_logs(run_id, task_id)
    return jsonify({"logs": logs})


@app.route('/api/runs/<run_id>/logs', methods=['GET'])
def get_run_logs(run_id):
    """获取运行的所有日志"""
    logs = storage.get_workflow_logs(run_id)
    return jsonify({"logs": logs})


# ---------- 版本管理 API ----------

@app.route('/api/workflows/<workflow_id>/versions', methods=['GET'])
def list_workflow_versions(workflow_id):
    """列出工作流版本"""
    versions = storage.list_workflow_versions(workflow_id)
    return jsonify({"versions": versions})


@app.route('/api/workflows/<workflow_id>/versions/<version>', methods=['GET'])
def get_workflow_version(workflow_id, version):
    """获取特定版本"""
    wf_version = storage.load_workflow_version(workflow_id, version)
    if not wf_version:
        return jsonify({"error": "版本不存在"}), 404
    return jsonify(wf_version.to_dict())


@app.route('/api/workflows/<workflow_id>/versions', methods=['POST'])
def create_workflow_version(workflow_id):
    """创建新版本"""
    workflow = storage.load_workflow(workflow_id)
    if not workflow:
        return jsonify({"error": "工作流不存在"}), 404
    
    data = request.get_json() or {}
    version = data.get('version')
    description = data.get('description', '')
    
    # 如果未指定版本号，自动递增
    if not version:
        parts = workflow.version.split('.')
        parts[-1] = str(int(parts[-1]) + 1)
        version = '.'.join(parts)
    
    workflow.version = version
    wf_version = workflow.create_version(
        created_by=data.get('created_by', 'system'),
        description=description
    )
    
    storage.save_workflow(workflow)
    storage.save_workflow_version(wf_version)
    
    return jsonify({
        "success": True,
        "version": wf_version.to_dict()
    })


@app.route('/api/workflows/<workflow_id>/versions/<version>/rollback', methods=['POST'])
def rollback_workflow_version(workflow_id, version):
    """回滚到指定版本"""
    wf_version = storage.load_workflow_version(workflow_id, version)
    if not wf_version:
        return jsonify({"error": "版本不存在"}), 404
    
    workflow = storage.load_workflow(workflow_id)
    if not workflow:
        return jsonify({"error": "工作流不存在"}), 404
    
    # 保存当前版本
    workflow.create_version(created_by="rollback")
    
    # 恢复到指定版本的定义
    from dag_engine import DAGParser
    restored = DAGParser.from_json(wf_version.definition)
    restored.id = workflow_id
    
    storage.save_workflow(restored)
    storage.save_workflow_version(wf_version)
    
    return jsonify({
        "success": True,
        "workflow": restored.to_dict()
    })


# ---------- 执行记录 API ----------

@app.route('/api/records', methods=['GET'])
def list_execution_records():
    """列出执行记录"""
    workflow_id = request.args.get('workflow_id')
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    records = storage.list_execution_records(workflow_id, limit, offset)
    return jsonify({"records": records})


@app.route('/api/records/<run_id>', methods=['GET'])
def get_execution_record(run_id):
    """获取执行记录详情"""
    record = storage.load_execution_record(run_id)
    if not record:
        return jsonify({"error": "记录不存在"}), 404
    return jsonify(record.to_dict())


@app.route('/api/records/<run_id>', methods=['DELETE'])
def delete_execution_record(run_id):
    """删除执行记录"""
    if storage.delete_execution_record(run_id):
        return jsonify({"success": True})
    return jsonify({"error": "记录不存在"}), 404


@app.route('/api/workflows/<workflow_id>/statistics', methods=['GET'])
def get_workflow_statistics(workflow_id):
    """获取工作流统计信息"""
    stats = storage.get_workflow_statistics(workflow_id)
    return jsonify(stats)


# ---------- DAG分析 API ----------

@app.route('/api/workflows/<workflow_id>/analyze', methods=['POST'])
def analyze_workflow(workflow_id):
    """分析工作流DAG"""
    workflow = storage.load_workflow(workflow_id)
    if not workflow:
        return jsonify({"error": "工作流不存在"}), 404
    
    from dag_engine import DAGScheduler
    
    try:
        scheduler = DAGScheduler(workflow)
        
        # 并行度分析
        parallelism = scheduler.calculate_parallelism()
        
        # 执行时间估算（假设每个任务1秒）
        task_durations = {t.id: 1.0 for t in workflow.tasks}
        time_estimate = scheduler.estimate_execution_time(task_durations)
        
        # 关键路径
        critical_path, path_length = scheduler.calculate_critical_path()
        
        return jsonify({
            "parallelism": parallelism,
            "time_estimate": time_estimate,
            "critical_path": {
                "path": critical_path,
                "length": path_length
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/api/workflows/<workflow_id>/export', methods=['GET'])
def export_workflow(workflow_id):
    """导出工作流定义"""
    workflow = storage.load_workflow(workflow_id)
    if not workflow:
        return jsonify({"error": "工作流不存在"}), 404
    
    return jsonify(workflow.to_dict())


@app.route('/api/workflows/import', methods=['POST'])
def import_workflow():
    """导入工作流定义"""
    data = request.get_json()
    if not data:
        return jsonify({"error": "无效的JSON数据"}), 400
    
    from dag_engine import DAGParser
    
    try:
        # 生成新的ID避免冲突
        workflow = DAGParser.from_json(data)
        workflow.id = generate_id()
        workflow.created_at = datetime.now()
        
        storage.save_workflow(workflow)
        
        return jsonify({
            "success": True,
            "workflow": workflow.to_dict()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# ---------- 系统 API ----------

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """获取系统统计"""
    workflows = storage.list_workflows()
    runs = storage.list_workflow_runs()
    
    # 计算更多统计
    total_tasks = sum(wf.get("task_count", 0) for wf in workflows)
    active_runs = runner.scheduler.get_active_runs()
    
    return jsonify({
        "workflows": len(workflows),
        "total_tasks": total_tasks,
        "runs": len(runs),
        "active_runs": len(active_runs),
        "websocket_connections": ws_manager.get_connection_count(),
        "subscription_stats": ws_manager.get_subscription_stats(),
        "system": {
            "uptime": "running",
            "version": "2.0.0"
        }
    })


@app.route('/api/health', methods=['GET'])
def health_check():
    """健康检查"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0.0",
        "components": {
            "flask": "running",
            "websocket": "running",
            "scheduler": "running"
        }
    })


# ==================== WebSocket服务器 ====================

WS_PORT = 8765


async def websocket_handler(websocket, path):
    """WebSocket连接处理"""
    connection_id = generate_id()
    ws_manager.register_connection(connection_id, websocket)
    
    try:
        # 发送欢迎消息
        await websocket.send(json.dumps({
            "type": "connected",
            "connection_id": connection_id,
            "message": "已连接到DAG调度器"
        }))
        
        # 处理消息
        async for message in websocket:
            response = await ws_manager.handle_message(connection_id, message)
            if response:
                await websocket.send(json.dumps(response, ensure_ascii=False))
                
    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        logger.error(f"WebSocket错误: {e}")
    finally:
        ws_manager.unregister_connection(connection_id)


def start_websocket_server():
    """启动WebSocket服务器"""
    import threading
    
    def run_server():
        import websockets.sync.server as sync_server
        
        def handle_ws(websocket):
            connection_id = generate_id()
            ws_manager.register_connection(connection_id, websocket)
            
            try:
                # 发送欢迎消息
                websocket.send(json.dumps({
                    "type": "connected",
                    "connection_id": connection_id,
                    "message": "已连接到DAG调度器"
                }))
                
                # 处理消息
                for message in websocket:
                    loop = asyncio.new_event_loop()
                    response = loop.run_until_complete(
                        ws_manager.handle_message(connection_id, message)
                    )
                    loop.close()
                    if response:
                        websocket.send(json.dumps(response, ensure_ascii=False))
                        
            except Exception as e:
                logger.error(f"WebSocket错误: {e}")
            finally:
                ws_manager.unregister_connection(connection_id)
        
        server = sync_server.serve(handle_ws, "0.0.0.0", WS_PORT)
        logger.info(f"WebSocket服务器启动: ws://0.0.0.0:{WS_PORT}")
        server.serve_forever()
    
    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
    return thread


# ==================== 启动 ====================

if __name__ == '__main__':
    # 确保目录存在
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "workflows").mkdir(exist_ok=True)
    (DATA_DIR / "states").mkdir(exist_ok=True)
    (DATA_DIR / "logs").mkdir(exist_ok=True)
    
    # 启动WebSocket服务器
    start_websocket_server()
    
    # 启动Flask
    print(f"\n{'='*50}")
    print(f"DAG工作流调度引擎")
    print(f"{'='*50}")
    print(f"前端地址: http://localhost:5000")
    print(f"WebSocket: ws://localhost:{WS_PORT}")
    print(f"API文档: http://localhost:5000/api/stats")
    print(f"{'='*50}\n")
    
    app.run(host='0.0.0.0', port=5000, debug=False)
