"""JSON文件存储 - 支持原子性写入"""
import json
import os
import tempfile
import shutil
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path
import fcntl

from models import (
    WorkflowDefinition, WorkflowRun, WorkflowStatus,
    TaskInstance, TaskStatus, generate_id, ExecutionRecord,
    WorkflowVersion
)


class AtomicJSONStorage:
    """原子性JSON文件存储"""
    
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.workflows_dir = self.data_dir / "workflows"
        self.states_dir = self.data_dir / "states"
        self.logs_dir = self.data_dir / "logs"
        
        # 创建目录
        for d in [self.workflows_dir, self.states_dir, self.logs_dir]:
            d.mkdir(parents=True, exist_ok=True)
    
    def _atomic_write(self, filepath: Path, data: Dict[str, Any]) -> None:
        """原子性写入JSON文件
        
        使用临时文件+重命名确保写入原子性
        使用文件锁防止并发写入冲突
        """
        lock_file = filepath.with_suffix('.lock')
        
        # 获取文件锁
        with open(lock_file, 'w') as lock_fd:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            try:
                # 写入临时文件
                fd, tmp_path = tempfile.mkstemp(
                    dir=filepath.parent,
                    suffix='.tmp'
                )
                try:
                    with os.fdopen(fd, 'w') as tmp_file:
                        json.dump(data, tmp_file, indent=2, ensure_ascii=False)
                    # 原子性重命名
                    shutil.move(tmp_path, str(filepath))
                except Exception:
                    # 清理临时文件
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                    raise
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
    
    def _atomic_read(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """原子性读取JSON文件"""
        if not filepath.exists():
            return None
        
        lock_file = filepath.with_suffix('.lock')
        
        # 创建锁文件
        lock_file.touch(exist_ok=True)
        
        with open(lock_file, 'w') as lock_fd:
            fcntl.flock(lock_fd, fcntl.LOCK_SH)
            try:
                with open(filepath, 'r') as f:
                    return json.load(f)
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
    
    # ==================== 工作流定义 ====================
    
    def save_workflow(self, workflow: WorkflowDefinition) -> None:
        """保存工作流定义"""
        workflow.updated_at = datetime.now()
        if not workflow.created_at:
            workflow.created_at = datetime.now()
        
        filepath = self.workflows_dir / f"{workflow.id}.json"
        self._atomic_write(filepath, workflow.to_dict())
    
    def load_workflow(self, workflow_id: str) -> Optional[WorkflowDefinition]:
        """加载工作流定义"""
        filepath = self.workflows_dir / f"{workflow_id}.json"
        data = self._atomic_read(filepath)
        if data:
            return WorkflowDefinition.from_dict(data)
        return None
    
    def list_workflows(self) -> List[Dict[str, Any]]:
        """列出所有工作流"""
        workflows = []
        for filepath in self.workflows_dir.glob("*.json"):
            data = self._atomic_read(filepath)
            if data:
                workflows.append({
                    "id": data["id"],
                    "name": data["name"],
                    "task_count": len(data.get("tasks", [])),
                    "schedule": data.get("schedule"),
                    "created_at": data.get("created_at"),
                    "updated_at": data.get("updated_at")
                })
        return workflows
    
    def delete_workflow(self, workflow_id: str) -> bool:
        """删除工作流"""
        filepath = self.workflows_dir / f"{workflow_id}.json"
        if filepath.exists():
            filepath.unlink()
            return True
        return False
    
    # ==================== 工作流运行状态 ====================
    
    def save_workflow_run(self, run: WorkflowRun) -> None:
        """保存工作流运行状态"""
        filepath = self.states_dir / f"{run.run_id}.json"
        self._atomic_write(filepath, run.to_dict())
    
    def load_workflow_run(self, run_id: str) -> Optional[WorkflowRun]:
        """加载工作流运行状态"""
        filepath = self.states_dir / f"{run_id}.json"
        data = self._atomic_read(filepath)
        if data:
            return WorkflowRun.from_dict(data)
        return None
    
    def list_workflow_runs(
        self, workflow_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """列出工作流运行记录"""
        runs = []
        for filepath in self.states_dir.glob("*.json"):
            data = self._atomic_read(filepath)
            if data:
                if workflow_id and data.get("workflow_id") != workflow_id:
                    continue
                runs.append({
                    "run_id": data["run_id"],
                    "workflow_id": data["workflow_id"],
                    "status": data["status"],
                    "start_time": data.get("start_time"),
                    "end_time": data.get("end_time"),
                    "task_count": len(data.get("task_instances", {}))
                })
        return sorted(runs, key=lambda x: x.get("start_time") or "", reverse=True)
    
    def delete_workflow_run(self, run_id: str) -> bool:
        """删除工作流运行记录"""
        filepath = self.states_dir / f"{run_id}.json"
        if filepath.exists():
            filepath.unlink()
            return True
        return False
    
    # ==================== 任务日志 ====================
    
    def append_task_log(
        self, 
        run_id: str, 
        task_id: str, 
        log_line: str
    ) -> None:
        """追加任务日志"""
        log_dir = self.logs_dir / run_id
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"{task_id}.log"
        lock_file = log_file.with_suffix('.log.lock')
        
        lock_file.touch(exist_ok=True)
        with open(lock_file, 'w') as lock_fd:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            try:
                with open(log_file, 'a') as f:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    f.write(f"[{timestamp}] {log_line}\n")
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
    
    def get_task_logs(self, run_id: str, task_id: str) -> List[str]:
        """获取任务日志"""
        log_file = self.logs_dir / run_id / f"{task_id}.log"
        if not log_file.exists():
            return []
        
        lock_file = log_file.with_suffix('.log.lock')
        lock_file.touch(exist_ok=True)
        
        with open(lock_file, 'w') as lock_fd:
            fcntl.flock(lock_fd, fcntl.LOCK_SH)
            try:
                with open(log_file, 'r') as f:
                    return f.readlines()
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
    
    def get_workflow_logs(self, run_id: str) -> Dict[str, List[str]]:
        """获取工作流所有任务日志"""
        log_dir = self.logs_dir / run_id
        if not log_dir.exists():
            return {}
        
        logs = {}
        for log_file in log_dir.glob("*.log"):
            task_id = log_file.stem
            logs[task_id] = self.get_task_logs(run_id, task_id)
        
        return logs
    
    # ==================== 执行记录 ====================
    
    def save_execution_record(self, record: ExecutionRecord) -> None:
        """保存执行记录"""
        records_dir = self.data_dir / "records"
        records_dir.mkdir(exist_ok=True)
        
        filepath = records_dir / f"{record.run_id}.json"
        self._atomic_write(filepath, record.to_dict())
    
    def load_execution_record(self, run_id: str) -> Optional[ExecutionRecord]:
        """加载执行记录"""
        records_dir = self.data_dir / "records"
        filepath = records_dir / f"{run_id}.json"
        data = self._atomic_read(filepath)
        if data:
            return ExecutionRecord.from_dict(data)
        return None
    
    def list_execution_records(
        self, 
        workflow_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """列出执行记录"""
        records_dir = self.data_dir / "records"
        if not records_dir.exists():
            return []
        
        records = []
        for filepath in records_dir.glob("*.json"):
            data = self._atomic_read(filepath)
            if data:
                if workflow_id and data.get("workflow_id") != workflow_id:
                    continue
                records.append({
                    "run_id": data["run_id"],
                    "workflow_id": data["workflow_id"],
                    "workflow_name": data.get("workflow_name", ""),
                    "status": data["status"],
                    "start_time": data.get("start_time"),
                    "duration": data.get("duration", 0),
                    "total_tasks": data.get("total_tasks", 0),
                    "success_tasks": data.get("success_tasks", 0),
                    "failed_tasks": data.get("failed_tasks", 0)
                })
        
        # 按时间排序
        records.sort(key=lambda x: x.get("start_time", ""), reverse=True)
        
        return records[offset:offset + limit]
    
    def get_workflow_statistics(self, workflow_id: str) -> Dict[str, Any]:
        """获取工作流统计信息"""
        records = self.list_execution_records(workflow_id)
        
        if not records:
            return {
                "total_runs": 0,
                "success_rate": 0,
                "avg_duration": 0,
                "total_tasks": 0
            }
        
        total_runs = len(records)
        success_runs = sum(1 for r in records if r["status"] == "success")
        total_duration = sum(r.get("duration", 0) for r in records)
        total_tasks = sum(r.get("total_tasks", 0) for r in records)
        
        return {
            "total_runs": total_runs,
            "success_rate": success_runs / total_runs * 100 if total_runs > 0 else 0,
            "avg_duration": total_duration / total_runs if total_runs > 0 else 0,
            "total_tasks": total_tasks,
            "last_run": records[0] if records else None,
            "recent_runs": records[:10]
        }
    
    def delete_execution_record(self, run_id: str) -> bool:
        """删除执行记录"""
        records_dir = self.data_dir / "records"
        filepath = records_dir / f"{run_id}.json"
        if filepath.exists():
            filepath.unlink()
            return True
        return False
    
    # ==================== 版本管理 ====================
    
    def save_workflow_version(self, version: WorkflowVersion) -> None:
        """保存工作流版本"""
        versions_dir = self.data_dir / "versions"
        versions_dir.mkdir(exist_ok=True)
        
        workflow_dir = versions_dir / version.workflow_id
        workflow_dir.mkdir(exist_ok=True)
        
        filepath = workflow_dir / f"v{version.version}.json"
        self._atomic_write(filepath, version.to_dict())
    
    def load_workflow_version(
        self, 
        workflow_id: str, 
        version: str
    ) -> Optional[WorkflowVersion]:
        """加载工作流版本"""
        versions_dir = self.data_dir / "versions"
        filepath = versions_dir / workflow_id / f"v{version}.json"
        data = self._atomic_read(filepath)
        if data:
            return WorkflowVersion.from_dict(data)
        return None
    
    def list_workflow_versions(self, workflow_id: str) -> List[Dict[str, Any]]:
        """列出工作流版本"""
        versions_dir = self.data_dir / "versions"
        workflow_dir = versions_dir / workflow_id
        
        if not workflow_dir.exists():
            return []
        
        versions = []
        for filepath in workflow_dir.glob("v*.json"):
            data = self._atomic_read(filepath)
            if data:
                versions.append({
                    "version": data["version"],
                    "created_at": data.get("created_at"),
                    "created_by": data.get("created_by", ""),
                    "description": data.get("description", ""),
                    "hash": data.get("hash", "")
                })
        
        return sorted(versions, key=lambda x: x["version"], reverse=True)
    
    def delete_workflow_version(
        self, 
        workflow_id: str, 
        version: str
    ) -> bool:
        """删除工作流版本"""
        versions_dir = self.data_dir / "versions"
        filepath = versions_dir / workflow_id / f"v{version}.json"
        if filepath.exists():
            filepath.unlink()
            return True
        return False
