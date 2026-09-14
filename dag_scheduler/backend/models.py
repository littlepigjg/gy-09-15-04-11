"""高级数据模型 - 版本控制、参数系统、条件分支"""
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Any, Optional, Union, Callable
from datetime import datetime
import uuid
import copy
import hashlib


class TaskType(Enum):
    """任务类型"""
    SHELL = "shell"
    PYTHON = "python"
    HTTP = "http"
    SUBWORKFLOW = "subworkflow"  # 子工作流
    CONDITION = "condition"      # 条件分支
    PARALLEL = "parallel"        # 并行网关
    MERGE = "merge"              # 合并网关


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"
    WAITING = "waiting"          # 等待依赖
    READY = "ready"              # 就绪，等待调度
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    TIMEOUT = "timeout"
    SKIPPED = "skipped"          # 条件跳过
    CANCELLED = "cancelled"
    BLOCKED = "blocked"          # 被锁阻塞


class WorkflowStatus(Enum):
    """工作流状态枚举"""
    IDLE = "idle"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PAUSED = "paused"            # 暂停
    CANCELLED = "cancelled"


class TriggerType(Enum):
    """触发类型"""
    MANUAL = "manual"
    SCHEDULE = "schedule"
    EVENT = "event"
    API = "api"
    WEBHOOK = "webhook"


class ParamType(Enum):
    """参数类型"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    JSON = "json"
    FILE = "file"
    SECRET = "secret"            # 敏感参数


@dataclass
class Parameter:
    """参数定义"""
    name: str
    type: ParamType = ParamType.STRING
    default: Any = None
    required: bool = False
    description: str = ""
    sensitive: bool = False      # 是否敏感信息
    options: List[Any] = field(default_factory=list)  # 枚举选项
    
    def validate(self, value: Any) -> bool:
        """验证参数值"""
        if self.required and value is None:
            return False
        
        if value is None:
            return True
        
        if self.type == ParamType.INTEGER:
            try:
                int(value)
                return True
            except (ValueError, TypeError):
                return False
        elif self.type == ParamType.FLOAT:
            try:
                float(value)
                return True
            except (ValueError, TypeError):
                return False
        elif self.type == ParamType.BOOLEAN:
            return isinstance(value, bool) or str(value).lower() in ('true', 'false')
        elif self.type == ParamType.JSON:
            try:
                import json
                if isinstance(value, str):
                    json.loads(value)
                return True
            except:
                return False
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type.value,
            "default": self.default,
            "required": self.required,
            "description": self.description,
            "sensitive": self.sensitive,
            "options": self.options
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Parameter':
        return cls(
            name=data["name"],
            type=ParamType(data.get("type", "string")),
            default=data.get("default"),
            required=data.get("required", False),
            description=data.get("description", ""),
            sensitive=data.get("sensitive", False),
            options=data.get("options", [])
        )


@dataclass
class ResourceLimit:
    """资源限制"""
    cpu_cores: float = 1.0           # CPU核数
    memory_mb: int = 512             # 内存MB
    disk_mb: int = 1024              # 磁盘MB
    timeout: int = 3600              # 超时时间（秒）
    max_retries: int = 3
    retry_delay: int = 5
    priority: int = 0                # 优先级 0-100
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "cpu_cores": self.cpu_cores,
            "memory_mb": self.memory_mb,
            "disk_mb": self.disk_mb,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "priority": self.priority
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ResourceLimit':
        if not data:
            return cls()
        return cls(
            cpu_cores=data.get("cpu_cores", 1.0),
            memory_mb=data.get("memory_mb", 512),
            disk_mb=data.get("disk_mb", 1024),
            timeout=data.get("timeout", 3600),
            max_retries=data.get("max_retries", 3),
            retry_delay=data.get("retry_delay", 5),
            priority=data.get("priority", 0)
        )


@dataclass
class TaskLock:
    """任务锁 - 防止并发冲突"""
    lock_key: str               # 锁键
    timeout: int = 300          # 锁超时（秒）
    retry_count: int = 3        # 获取锁重试次数
    retry_delay: int = 5        # 重试延迟（秒）
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "lock_key": self.lock_key,
            "timeout": self.timeout,
            "retry_count": self.retry_count,
            "retry_delay": self.retry_delay
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskLock':
        if not data:
            return None
        return cls(
            lock_key=data["lock_key"],
            timeout=data.get("timeout", 300),
            retry_count=data.get("retry_count", 3),
            retry_delay=data.get("retry_delay", 5)
        )


@dataclass
class Condition:
    """条件表达式"""
    expression: str             # 条件表达式，如 "status == 'success'"
    true_task: Optional[str] = None   # 条件为真时执行的任务
    false_task: Optional[str] = None  # 条件为假时执行的任务
    
    def evaluate(self, context: Dict[str, Any]) -> bool:
        """评估条件表达式"""
        try:
            # 安全的表达式评估
            import ast
            import operator
            
            # 支持的操作符
            ops = {
                ast.Eq: operator.eq,
                ast.NotEq: operator.ne,
                ast.Lt: operator.lt,
                ast.LtE: operator.le,
                ast.Gt: operator.gt,
                ast.GtE: operator.ge,
                ast.In: lambda a, b: a in b,
                ast.NotIn: lambda a, b: a not in b,
            }
            
            tree = ast.parse(self.expression, mode='eval')
            return self._eval_node(tree.body, context, ops)
            
        except Exception as e:
            print(f"条件评估失败: {e}")
            return False
    
    def _eval_node(self, node, context, ops):
        """递归评估AST节点"""
        import ast
        
        if isinstance(node, ast.Compare):
            left = self._eval_node(node.left, context, ops)
            for op, comparator in zip(node.ops, node.comparators):
                right = self._eval_node(comparator, context, ops)
                if op not in ops:
                    raise ValueError(f"不支持的操作符: {op}")
                if not ops[op](left, right):
                    return False
            return True
        elif isinstance(node, ast.Name):
            return context.get(node.id)
        elif isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.Str):
            return node.s
        elif isinstance(node, ast.Num):
            return node.n
        elif isinstance(node, ast.BoolOp):
            values = [self._eval_node(v, context, ops) for v in node.values]
            if isinstance(node, ast.And):
                return all(values)
            else:
                return any(values)
        elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return not self._eval_node(node.operand, context, ops)
        
        raise ValueError(f"不支持的AST节点类型: {type(node)}")
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "expression": self.expression,
            "true_task": self.true_task,
            "false_task": self.false_task
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Condition':
        if not data:
            return None
        return cls(
            expression=data["expression"],
            true_task=data.get("true_task"),
            false_task=data.get("false_task")
        )


@dataclass
class TaskDefinition:
    """增强版任务定义"""
    id: str
    name: str
    type: TaskType = TaskType.SHELL
    command: str = ""
    dependencies: List[str] = field(default_factory=list)
    
    # 资源限制
    resources: ResourceLimit = field(default_factory=ResourceLimit)
    
    # 参数和变量
    parameters: List[Parameter] = field(default_factory=list)
    input_mapping: Dict[str, str] = field(default_factory=dict)    # 输入映射
    output_mapping: Dict[str, str] = field(default_factory=dict)   # 输出映射
    
    # 条件和分支
    condition: Optional[Condition] = None
    
    # 锁配置
    lock: Optional[TaskLock] = None
    
    # 子工作流
    subworkflow_id: Optional[str] = None
    
    # 可视化
    position: Dict[str, int] = field(default_factory=lambda: {"x": 0, "y": 0})
    size: Dict[str, int] = field(default_factory=lambda: {"width": 160, "height": 60})
    
    # 元数据
    description: str = ""
    tags: List[str] = field(default_factory=list)
    enabled: bool = True
    
    def calculate_hash(self) -> str:
        """计算任务定义哈希"""
        content = f"{self.id}:{self.name}:{self.type.value}:{self.command}"
        return hashlib.md5(content.encode()).hexdigest()[:8]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type.value,
            "command": self.command,
            "dependencies": self.dependencies,
            "resources": self.resources.to_dict(),
            "parameters": [p.to_dict() for p in self.parameters],
            "input_mapping": self.input_mapping,
            "output_mapping": self.output_mapping,
            "condition": self.condition.to_dict() if self.condition else None,
            "lock": self.lock.to_dict() if self.lock else None,
            "subworkflow_id": self.subworkflow_id,
            "position": self.position,
            "size": self.size,
            "description": self.description,
            "tags": self.tags,
            "enabled": self.enabled
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskDefinition':
        resources = ResourceLimit.from_dict(data.get("resources"))
        parameters = [Parameter.from_dict(p) for p in data.get("parameters", [])]
        condition = Condition.from_dict(data.get("condition"))
        lock = TaskLock.from_dict(data.get("lock"))
        
        return cls(
            id=data["id"],
            name=data["name"],
            type=TaskType(data.get("type", "shell")),
            command=data.get("command", ""),
            dependencies=data.get("dependencies", []),
            resources=resources,
            parameters=parameters,
            input_mapping=data.get("input_mapping", {}),
            output_mapping=data.get("output_mapping", {}),
            condition=condition,
            lock=lock,
            subworkflow_id=data.get("subworkflow_id"),
            position=data.get("position", {"x": 0, "y": 0}),
            size=data.get("size", {"width": 160, "height": 60}),
            description=data.get("description", ""),
            tags=data.get("tags", []),
            enabled=data.get("enabled", True)
        )


@dataclass
class TaskInstance:
    """任务实例（运行时状态）"""
    task_id: str
    workflow_id: str
    run_id: str
    status: TaskStatus = TaskStatus.PENDING
    
    # 时间信息
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    scheduled_time: Optional[datetime] = None
    
    # 执行信息
    retry_count: int = 0
    error_message: Optional[str] = None
    exit_code: Optional[int] = None
    process_id: Optional[int] = None
    
    # 日志和输出
    logs: List[Dict[str, Any]] = field(default_factory=list)
    stdout: str = ""
    stderr: str = ""
    
    # 输入输出
    inputs: Dict[str, Any] = field(default_factory=dict)
    outputs: Dict[str, Any] = field(default_factory=dict)
    
    # 锁信息
    lock_acquired: bool = False
    lock_key: Optional[str] = None
    
    # 性能指标
    cpu_usage: float = 0.0
    memory_usage: int = 0
    disk_usage: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "workflow_id": self.workflow_id,
            "run_id": self.run_id,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "scheduled_time": self.scheduled_time.isoformat() if self.scheduled_time else None,
            "retry_count": self.retry_count,
            "error_message": self.error_message,
            "exit_code": self.exit_code,
            "process_id": self.process_id,
            "logs": self.logs[-100:],  # 只保留最近100条
            "stdout": self.stdout[-10000:],  # 限制输出大小
            "stderr": self.stderr[-10000:],
            "inputs": self.inputs,
            "outputs": self.outputs,
            "lock_acquired": self.lock_acquired,
            "lock_key": self.lock_key,
            "cpu_usage": self.cpu_usage,
            "memory_usage": self.memory_usage,
            "disk_usage": self.disk_usage
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskInstance':
        instance = cls(
            task_id=data["task_id"],
            workflow_id=data["workflow_id"],
            run_id=data.get("run_id", ""),
            status=TaskStatus(data["status"]),
            retry_count=data.get("retry_count", 0),
            error_message=data.get("error_message"),
            exit_code=data.get("exit_code"),
            process_id=data.get("process_id"),
            logs=data.get("logs", []),
            stdout=data.get("stdout", ""),
            stderr=data.get("stderr", ""),
            inputs=data.get("inputs", {}),
            outputs=data.get("outputs", {}),
            lock_acquired=data.get("lock_acquired", False),
            lock_key=data.get("lock_key"),
            cpu_usage=data.get("cpu_usage", 0.0),
            memory_usage=data.get("memory_usage", 0),
            disk_usage=data.get("disk_usage", 0)
        )
        if data.get("start_time"):
            instance.start_time = datetime.fromisoformat(data["start_time"])
        if data.get("end_time"):
            instance.end_time = datetime.fromisoformat(data["end_time"])
        if data.get("scheduled_time"):
            instance.scheduled_time = datetime.fromisoformat(data["scheduled_time"])
        return instance
    
    @property
    def duration(self) -> Optional[float]:
        """计算执行时长（秒）"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


@dataclass
class WorkflowVersion:
    """工作流版本"""
    version: str
    workflow_id: str
    definition: Dict[str, Any]
    created_at: datetime = field(default_factory=datetime.now)
    created_by: str = "system"
    description: str = ""
    hash: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "workflow_id": self.workflow_id,
            "definition": self.definition,
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "description": self.description,
            "hash": self.hash
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowVersion':
        version = cls(
            version=data["version"],
            workflow_id=data["workflow_id"],
            definition=data["definition"],
            description=data.get("description", ""),
            hash=data.get("hash", "")
        )
        if data.get("created_at"):
            version.created_at = datetime.fromisoformat(data["created_at"])
        if data.get("created_by"):
            version.created_by = data["created_by"]
        return version


@dataclass
class WorkflowDefinition:
    """增强版工作流定义"""
    id: str
    name: str
    version: str = "1.0.0"
    tasks: List[TaskDefinition] = field(default_factory=list)
    
    # 参数定义
    parameters: List[Parameter] = field(default_factory=list)
    
    # 全局配置
    max_concurrency: int = 10
    max_task_concurrency: int = 5
    schedule: Optional[str] = None
    trigger_type: TriggerType = TriggerType.MANUAL
    
    # 元数据
    description: str = ""
    tags: List[str] = field(default_factory=list)
    category: str = "default"
    owner: str = ""
    
    # 超时和重试
    workflow_timeout: int = 86400  # 工作流超时（秒）
    
    # 时间戳
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # 版本历史
    version_history: List[WorkflowVersion] = field(default_factory=list)
    
    def calculate_hash(self) -> str:
        """计算工作流定义哈希"""
        content = f"{self.id}:{self.name}:{self.version}"
        for task in sorted(self.tasks, key=lambda t: t.id):
            content += f":{task.calculate_hash()}"
        return hashlib.md5(content.encode()).hexdigest()[:12]
    
    def create_version(self, created_by: str = "system", description: str = "") -> WorkflowVersion:
        """创建版本快照"""
        version = WorkflowVersion(
            version=self.version,
            workflow_id=self.id,
            definition=self.to_dict(),
            created_by=created_by,
            description=description,
            hash=self.calculate_hash()
        )
        self.version_history.append(version)
        return version
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "version": self.version,
            "tasks": [t.to_dict() for t in self.tasks],
            "parameters": [p.to_dict() for p in self.parameters],
            "max_concurrency": self.max_concurrency,
            "max_task_concurrency": self.max_task_concurrency,
            "schedule": self.schedule,
            "trigger_type": self.trigger_type.value,
            "description": self.description,
            "tags": self.tags,
            "category": self.category,
            "owner": self.owner,
            "workflow_timeout": self.workflow_timeout,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "version_history": [v.to_dict() for v in self.version_history[-10:]]  # 只保留最近10个版本
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowDefinition':
        tasks = [TaskDefinition.from_dict(t) for t in data.get("tasks", [])]
        parameters = [Parameter.from_dict(p) for p in data.get("parameters", [])]
        version_history = [WorkflowVersion.from_dict(v) for v in data.get("version_history", [])]
        
        workflow = cls(
            id=data["id"],
            name=data["name"],
            version=data.get("version", "1.0.0"),
            tasks=tasks,
            parameters=parameters,
            max_concurrency=data.get("max_concurrency", 10),
            max_task_concurrency=data.get("max_task_concurrency", 5),
            schedule=data.get("schedule"),
            trigger_type=TriggerType(data.get("trigger_type", "manual")),
            description=data.get("description", ""),
            tags=data.get("tags", []),
            category=data.get("category", "default"),
            owner=data.get("owner", ""),
            workflow_timeout=data.get("workflow_timeout", 86400),
            version_history=version_history
        )
        
        if data.get("created_at"):
            workflow.created_at = datetime.fromisoformat(data["created_at"])
        if data.get("updated_at"):
            workflow.updated_at = datetime.fromisoformat(data["updated_at"])
        
        return workflow


@dataclass
class WorkflowRun:
    """工作流运行实例"""
    run_id: str
    workflow_id: str
    workflow_version: str = ""
    status: WorkflowStatus = WorkflowStatus.IDLE
    
    # 时间信息
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    # 执行上下文
    parameters: Dict[str, Any] = field(default_factory=dict)
    variables: Dict[str, Any] = field(default_factory=dict)
    task_instances: Dict[str, TaskInstance] = field(default_factory=dict)
    
    # 执行计划
    execution_plan: List[List[str]] = field(default_factory=list)  # 按层级分组的任务ID
    
    # 触发信息
    trigger_type: TriggerType = TriggerType.MANUAL
    triggered_by: str = ""
    
    # 错误信息
    error_message: Optional[str] = None
    
    # 性能统计
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    skipped_tasks: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workflow_id": self.workflow_id,
            "workflow_version": self.workflow_version,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "parameters": self.parameters,
            "variables": self.variables,
            "task_instances": {k: v.to_dict() for k, v in self.task_instances.items()},
            "execution_plan": self.execution_plan,
            "trigger_type": self.trigger_type.value,
            "triggered_by": self.triggered_by,
            "error_message": self.error_message,
            "total_tasks": self.total_tasks,
            "completed_tasks": self.completed_tasks,
            "failed_tasks": self.failed_tasks,
            "skipped_tasks": self.skipped_tasks
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowRun':
        run = cls(
            run_id=data["run_id"],
            workflow_id=data["workflow_id"],
            workflow_version=data.get("workflow_version", ""),
            status=WorkflowStatus(data["status"]),
            parameters=data.get("parameters", {}),
            variables=data.get("variables", {}),
            execution_plan=data.get("execution_plan", []),
            triggered_by=data.get("triggered_by", ""),
            error_message=data.get("error_message"),
            total_tasks=data.get("total_tasks", 0),
            completed_tasks=data.get("completed_tasks", 0),
            failed_tasks=data.get("failed_tasks", 0),
            skipped_tasks=data.get("skipped_tasks", 0)
        )
        if data.get("start_time"):
            run.start_time = datetime.fromisoformat(data["start_time"])
        if data.get("end_time"):
            run.end_time = datetime.fromisoformat(data["end_time"])
        if data.get("trigger_type"):
            run.trigger_type = TriggerType(data["trigger_type"])
        if data.get("task_instances"):
            run.task_instances = {
                k: TaskInstance.from_dict(v) 
                for k, v in data["task_instances"].items()
            }
        return run
    
    @property
    def duration(self) -> Optional[float]:
        """计算执行时长（秒）"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def progress(self) -> float:
        """计算执行进度百分比"""
        if self.total_tasks == 0:
            return 0.0
        return (self.completed_tasks + self.failed_tasks + self.skipped_tasks) / self.total_tasks * 100


@dataclass
class ExecutionRecord:
    """执行记录 - 用于历史分析"""
    run_id: str
    workflow_id: str
    workflow_name: str
    status: WorkflowStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: float = 0.0
    trigger_type: TriggerType = TriggerType.MANUAL
    triggered_by: str = ""
    
    # 统计信息
    total_tasks: int = 0
    success_tasks: int = 0
    failed_tasks: int = 0
    skipped_tasks: int = 0
    
    # 性能数据
    total_cpu_time: float = 0.0
    peak_memory: int = 0
    total_disk_io: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workflow_id": self.workflow_id,
            "workflow_name": self.workflow_name,
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration": self.duration,
            "trigger_type": self.trigger_type.value,
            "triggered_by": self.triggered_by,
            "total_tasks": self.total_tasks,
            "success_tasks": self.success_tasks,
            "failed_tasks": self.failed_tasks,
            "skipped_tasks": self.skipped_tasks,
            "total_cpu_time": self.total_cpu_time,
            "peak_memory": self.peak_memory,
            "total_disk_io": self.total_disk_io
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExecutionRecord':
        record = cls(
            run_id=data["run_id"],
            workflow_id=data["workflow_id"],
            workflow_name=data.get("workflow_name", ""),
            status=WorkflowStatus(data["status"]),
            duration=data.get("duration", 0.0),
            triggered_by=data.get("triggered_by", ""),
            total_tasks=data.get("total_tasks", 0),
            success_tasks=data.get("success_tasks", 0),
            failed_tasks=data.get("failed_tasks", 0),
            skipped_tasks=data.get("skipped_tasks", 0),
            total_cpu_time=data.get("total_cpu_time", 0.0),
            peak_memory=data.get("peak_memory", 0),
            total_disk_io=data.get("total_disk_io", 0)
        )
        if data.get("start_time"):
            record.start_time = datetime.fromisoformat(data["start_time"])
        if data.get("end_time"):
            record.end_time = datetime.fromisoformat(data["end_time"])
        if data.get("trigger_type"):
            record.trigger_type = TriggerType(data["trigger_type"])
        return record


def generate_id() -> str:
    """生成唯一ID"""
    return str(uuid.uuid4())[:8]
