"""增强版调度器 - 锁、资源限制、条件分支、子工作流"""
import asyncio
import subprocess
import signal
import logging
import threading
import time
import re
from datetime import datetime
from typing import Dict, Set, Optional, Callable, Any, List
from collections import defaultdict
import psutil

from models import (
    WorkflowDefinition, WorkflowRun, WorkflowStatus,
    TaskDefinition, TaskInstance, TaskStatus, TaskType,
    ResourceLimit, TaskLock, Condition, generate_id,
    ExecutionRecord, TriggerType
)
from dag_engine import DAGScheduler, VariableResolver, ExpressionEvaluator
from storage import AtomicJSONStorage

logger = logging.getLogger(__name__)


class DistributedLock:
    """分布式锁管理器"""
    
    def __init__(self):
        self._locks: Dict[str, Dict] = {}
        self._lock = threading.Lock()
    
    def acquire(self, lock_key: str, owner: str, timeout: int = 300) -> bool:
        """获取锁"""
        with self._lock:
            now = datetime.now()
            
            # 检查锁是否存在
            if lock_key in self._locks:
                lock_info = self._locks[lock_key]
                
                # 检查是否过期
                lock_time = lock_info['acquired_at']
                if (now - lock_time).total_seconds() > lock_info['timeout']:
                    # 锁已过期，强制释放
                    del self._locks[lock_key]
                elif lock_info['owner'] != owner:
                    # 锁被其他人持有
                    return False
                else:
                    # 同一个所有者，重入
                    lock_info['ref_count'] += 1
                    return True
            
            # 获取新锁
            self._locks[lock_key] = {
                'owner': owner,
                'acquired_at': now,
                'timeout': timeout,
                'ref_count': 1
            }
            return True
    
    def release(self, lock_key: str, owner: str) -> bool:
        """释放锁"""
        with self._lock:
            if lock_key not in self._locks:
                return False
            
            lock_info = self._locks[lock_key]
            
            if lock_info['owner'] != owner:
                return False
            
            lock_info['ref_count'] -= 1
            
            if lock_info['ref_count'] <= 0:
                del self._locks[lock_key]
            
            return True
    
    def is_locked(self, lock_key: str) -> bool:
        """检查锁是否存在"""
        with self._lock:
            if lock_key not in self._locks:
                return False
            
            lock_info = self._locks[lock_key]
            
            # 检查是否过期
            if (datetime.now() - lock_info['acquired_at']).total_seconds() > lock_info['timeout']:
                del self._locks[lock_key]
                return False
            
            return True
    
    def get_lock_info(self, lock_key: str) -> Optional[Dict]:
        """获取锁信息"""
        with self._lock:
            return self._locks.get(lock_key)


class ResourceMonitor:
    """资源监控器"""
    
    def __init__(self, limits: ResourceLimit):
        self.limits = limits
        self._start_time = None
        self._process = None
    
    def start(self, pid: int = None):
        """开始监控"""
        self._start_time = datetime.now()
        if pid:
            try:
                self._process = psutil.Process(pid)
            except psutil.NoSuchProcess:
                pass
    
    def get_usage(self) -> Dict[str, Any]:
        """获取当前资源使用"""
        usage = {
            'cpu_percent': 0.0,
            'memory_mb': 0,
            'disk_mb': 0,
            'elapsed_seconds': 0
        }
        
        if self._start_time:
            usage['elapsed_seconds'] = (datetime.now() - self._start_time).total_seconds()
        
        if self._process:
            try:
                usage['cpu_percent'] = self._process.cpu_percent()
                mem_info = self._process.memory_info()
                usage['memory_mb'] = mem_info.rss / 1024 / 1024
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        return usage
    
    def check_limits(self) -> tuple[bool, str]:
        """检查是否超出资源限制"""
        usage = self.get_usage()
        
        if usage['elapsed_seconds'] > self.limits.timeout:
            return False, f"任务执行超时: {usage['elapsed_seconds']:.1f}s > {self.limits.timeout}s"
        
        if usage['memory_mb'] > self.limits.memory_mb:
            return False, f"内存超限: {usage['memory_mb']:.1f}MB > {self.limits.memory_mb}MB"
        
        return True, ""


class TaskScheduler:
    """增强版任务调度器"""
    
    def __init__(self, storage: AtomicJSONStorage, max_workers: int = 10):
        self.storage = storage
        self.max_workers = max_workers
        
        # 并发控制
        self._semaphore = asyncio.Semaphore(max_workers)
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._running_processes: Dict[str, subprocess.Popen] = {}
        
        # 锁管理
        self._lock_manager = DistributedLock()
        
        # 资源监控
        self._resource_monitors: Dict[str, ResourceMonitor] = {}
        
        # 活跃的工作流运行
        self._active_runs: Dict[str, WorkflowRun] = {}
        self._dag_schedulers: Dict[str, DAGScheduler] = {}
        
        # 状态回调
        self._status_callbacks: list[Callable] = []
        
        # 执行线程池
        self._executor = None  # 延迟初始化
        
        # 运行锁
        self._run_locks: Dict[str, asyncio.Lock] = {}
    
    def _get_executor(self):
        """延迟获取执行器"""
        if self._executor is None:
            from concurrent.futures import ThreadPoolExecutor
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        return self._executor
    
    def on_status_change(self, callback: Callable) -> None:
        """注册状态变化回调"""
        self._status_callbacks.append(callback)
    
    async def _notify_status_change(
        self, 
        event_type: str, 
        run_id: str, 
        data: Dict[str, Any]
    ) -> None:
        """通知状态变化"""
        for callback in self._status_callbacks:
            try:
                await callback(event_type, run_id, data)
            except Exception as e:
                logger.error(f"状态回调执行失败: {e}")
    
    def start_workflow_sync(
        self, 
        workflow_id: str, 
        run_id: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        triggered_by: str = "manual"
    ) -> WorkflowRun:
        """同步启动工作流执行"""
        # 加载工作流定义
        workflow = self.storage.load_workflow(workflow_id)
        if not workflow:
            raise ValueError(f"工作流 {workflow_id} 不存在")
        
        # 验证参数
        if parameters:
            for param_def in workflow.parameters:
                if param_def.required and param_def.name not in parameters:
                    raise ValueError(f"缺少必要参数: {param_def.name}")
                if param_def.name in parameters:
                    if not param_def.validate(parameters[param_def.name]):
                        raise ValueError(f"参数验证失败: {param_def.name}")
        
        # 创建运行实例
        if not run_id:
            run_id = generate_id()
        
        run = WorkflowRun(
            run_id=run_id,
            workflow_id=workflow_id,
            workflow_version=workflow.version,
            status=WorkflowStatus.RUNNING,
            start_time=datetime.now(),
            parameters=parameters or {},
            trigger_type=TriggerType.MANUAL,
            triggered_by=triggered_by
        )
        
        # 初始化任务实例
        for task in workflow.tasks:
            if task.enabled:
                run.task_instances[task.id] = TaskInstance(
                    task_id=task.id,
                    workflow_id=workflow_id,
                    run_id=run_id
                )
        
        run.total_tasks = len(run.task_instances)
        
        # 保存初始状态
        self.storage.save_workflow_run(run)
        
        # 创建DAG调度器
        dag_scheduler = DAGScheduler(workflow)
        
        # 保存活跃运行
        self._active_runs[run_id] = run
        self._dag_schedulers[run_id] = dag_scheduler
        
        # 在独立线程中启动异步执行
        def run_async():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                # 通知工作流开始
                loop.run_until_complete(
                    self._notify_status_change("workflow_start", run_id, run.to_dict())
                )
                # 执行工作流
                loop.run_until_complete(self._execute_workflow(run_id))
            finally:
                loop.close()
        
        thread = threading.Thread(target=run_async, daemon=True)
        thread.start()
        
        return run
    
    async def _execute_workflow(self, run_id: str) -> None:
        """执行工作流调度"""
        run = self._active_runs.get(run_id)
        if not run:
            return
        
        workflow = self.storage.load_workflow(run.workflow_id)
        dag_scheduler = self._dag_schedulers.get(run_id)
        
        if not workflow or not dag_scheduler:
            return
        
        try:
            # 获取执行计划
            execution_levels = dag_scheduler.get_execution_levels()
            run.execution_plan = [[t.id for t in level] for level in execution_levels]
            
            completed_tasks: Set[str] = set()
            running_tasks: Set[str] = set()
            failed_tasks: Set[str] = set()
            skipped_tasks: Set[str] = set()
            
            # 构建执行上下文
            context = {
                'variables': run.parameters.copy(),
                'task_instances': {}
            }
            
            # 按层级执行
            for level_idx, level in enumerate(execution_levels):
                # 获取当前可执行的任务
                ready_tasks = dag_scheduler.get_ready_tasks(
                    completed_tasks, running_tasks, failed_tasks, skipped_tasks, context
                )
                
                # 过滤出当前层级的任务
                level_tasks = [
                    t for t in ready_tasks 
                    if t.id in [lt.id for lt in level]
                    and t.id not in running_tasks
                    and t.id not in failed_tasks
                ]
                
                if not level_tasks:
                    continue
                
                # 按优先级排序
                level_tasks.sort(key=lambda t: t.resources.priority, reverse=True)
                
                # 为每个任务创建执行协程
                tasks_coroutines = []
                for task in level_tasks:
                    # 检查锁
                    if task.lock:
                        if self._lock_manager.is_locked(task.lock.lock_key):
                            # 等待锁
                            logger.info(f"任务 {task.id} 等待锁 {task.lock.lock_key}")
                            # 可以选择等待或跳过
                            continue
                    
                    running_tasks.add(task.id)
                    run.task_instances[task.id].status = TaskStatus.RUNNING
                    
                    coro = self._execute_task(
                        run_id, task, context,
                        completed_tasks, running_tasks, failed_tasks, skipped_tasks
                    )
                    tasks_coroutines.append(coro)
                
                # 并行执行当前层级的任务
                if tasks_coroutines:
                    await asyncio.gather(*tasks_coroutines, return_exceptions=True)
            
            # 检查最终状态
            if failed_tasks:
                run.status = WorkflowStatus.FAILED
                run.error_message = f"任务失败: {', '.join(failed_tasks)}"
            else:
                run.status = WorkflowStatus.SUCCESS
            
            run.end_time = datetime.now()
            run.completed_tasks = len(completed_tasks)
            run.failed_tasks = len(failed_tasks)
            run.skipped_tasks = len(skipped_tasks)
            
            # 保存最终状态
            self.storage.save_workflow_run(run)
            
            # 创建执行记录
            self._create_execution_record(run, workflow)
            
            # 通知工作流完成
            await self._notify_status_change(
                "workflow_complete", run_id, run.to_dict()
            )
            
        except Exception as e:
            logger.error(f"工作流执行失败: {e}")
            run.status = WorkflowStatus.FAILED
            run.end_time = datetime.now()
            run.error_message = str(e)
            self.storage.save_workflow_run(run)
            
            await self._notify_status_change(
                "workflow_error", run_id, {
                    "error": str(e),
                    "run": run.to_dict()
                }
            )
        finally:
            # 清理
            self._active_runs.pop(run_id, None)
            self._dag_schedulers.pop(run_id, None)
            self._run_locks.pop(run_id, None)
    
    def _create_execution_record(self, run: WorkflowRun, workflow: WorkflowDefinition) -> None:
        """创建执行记录"""
        record = ExecutionRecord(
            run_id=run.run_id,
            workflow_id=run.workflow_id,
            workflow_name=workflow.name,
            status=run.status,
            start_time=run.start_time,
            end_time=run.end_time,
            duration=run.duration or 0.0,
            trigger_type=run.trigger_type,
            triggered_by=run.triggered_by,
            total_tasks=run.total_tasks,
            success_tasks=run.completed_tasks,
            failed_tasks=run.failed_tasks,
            skipped_tasks=run.skipped_tasks
        )
        
        # 计算性能数据
        for instance in run.task_instances.values():
            record.total_cpu_time += instance.cpu_usage
            record.peak_memory = max(record.peak_memory, instance.memory_usage)
        
        self.storage.save_execution_record(record)
    
    async def _execute_task(
        self,
        run_id: str,
        task: TaskDefinition,
        context: Dict[str, Any],
        completed_tasks: Set[str],
        running_tasks: Set[str],
        failed_tasks: Set[str],
        skipped_tasks: Set[str]
    ) -> None:
        """执行单个任务"""
        run = self._active_runs.get(run_id)
        if not run:
            return
        
        instance = run.task_instances.get(task.id)
        if not instance:
            return
        
        # 解析命令模板
        command = VariableResolver.resolve_command(task.command, context)
        
        # 设置输入映射
        for param_name, var_path in task.input_mapping.items():
            instance.inputs[param_name] = context.get(var_path)
        
        # 获取信号量控制并发
        async with self._semaphore:
            max_retries = task.resources.max_retries
            
            for attempt in range(max_retries + 1):
                try:
                    # 更新重试次数
                    instance.retry_count = attempt
                    instance.start_time = datetime.now()
                    
                    # 获取锁
                    if task.lock:
                        lock_acquired = self._lock_manager.acquire(
                            task.lock.lock_key,
                            f"{run_id}:{task.id}",
                            task.lock.timeout
                        )
                        if not lock_acquired:
                            # 等待并重试获取锁
                            for retry in range(task.lock.retry_count):
                                await asyncio.sleep(task.lock.retry_delay)
                                lock_acquired = self._lock_manager.acquire(
                                    task.lock.lock_key,
                                    f"{run_id}:{task.id}",
                                    task.lock.timeout
                                )
                                if lock_acquired:
                                    break
                        
                        if not lock_acquired:
                            raise Exception(f"无法获取锁: {task.lock.lock_key}")
                        
                        instance.lock_acquired = True
                        instance.lock_key = task.lock.lock_key
                    
                    # 通知任务开始
                    await self._notify_status_change(
                        "task_start", run_id, {
                            "task_id": task.id,
                            "attempt": attempt + 1,
                            "instance": instance.to_dict()
                        }
                    )
                    
                    # 根据任务类型执行
                    if task.type == TaskType.SUBWORKFLOW:
                        await self._execute_subworkflow(run_id, task, instance, context)
                    else:
                        await self._run_task_command(run_id, task, instance, command)
                    
                    # 提取输出
                    if task.output_mapping and instance.stdout:
                        instance.outputs = VariableResolver.extract_outputs(
                            task.output_mapping, instance.stdout
                        )
                    
                    # 更新上下文
                    context['task_instances'][task.id] = instance.to_dict()
                    context.update(instance.outputs)
                    
                    # 任务成功
                    instance.status = TaskStatus.SUCCESS
                    instance.end_time = datetime.now()
                    
                    completed_tasks.add(task.id)
                    running_tasks.discard(task.id)
                    
                    # 保存状态
                    self.storage.save_workflow_run(run)
                    
                    # 通知任务完成
                    await self._notify_status_change(
                        "task_complete", run_id, {
                            "task_id": task.id,
                            "instance": instance.to_dict()
                        }
                    )
                    
                    return  # 成功，退出重试循环
                    
                except asyncio.TimeoutError:
                    # 超时处理
                    instance.status = TaskStatus.TIMEOUT
                    instance.error_message = f"任务执行超时（{task.resources.timeout}秒）"
                    instance.end_time = datetime.now()
                    
                    # 尝试终止进程
                    self._terminate_task_process(task.id)
                    
                    await self._notify_status_change(
                        "task_timeout", run_id, {
                            "task_id": task.id,
                            "timeout": task.resources.timeout,
                            "instance": instance.to_dict()
                        }
                    )
                    
                    if attempt < max_retries:
                        # 重试
                        instance.status = TaskStatus.RETRYING
                        await self._notify_status_change(
                            "task_retry", run_id, {
                                "task_id": task.id,
                                "attempt": attempt + 1,
                                "max_retries": max_retries
                            }
                        )
                        await asyncio.sleep(task.resources.retry_delay)
                    else:
                        failed_tasks.add(task.id)
                        running_tasks.discard(task.id)
                    
                except Exception as e:
                    # 其他错误
                    instance.status = TaskStatus.FAILED
                    instance.error_message = str(e)
                    instance.end_time = datetime.now()
                    
                    await self._notify_status_change(
                        "task_error", run_id, {
                            "task_id": task.id,
                            "error": str(e),
                            "instance": instance.to_dict()
                        }
                    )
                    
                    if attempt < max_retries:
                        # 重试
                        instance.status = TaskStatus.RETRYING
                        await self._notify_status_change(
                            "task_retry", run_id, {
                                "task_id": task.id,
                                "attempt": attempt + 1,
                                "max_retries": max_retries
                            }
                        )
                        await asyncio.sleep(task.resources.retry_delay)
                    else:
                        failed_tasks.add(task.id)
                        running_tasks.discard(task.id)
                
                finally:
                    # 释放锁
                    if task.lock and instance.lock_acquired:
                        self._lock_manager.release(task.lock.lock_key, f"{run_id}:{task.id}")
                        instance.lock_acquired = False
            
            # 保存最终状态
            self.storage.save_workflow_run(run)
    
    async def _run_task_command(
        self, 
        run_id: str, 
        task: TaskDefinition, 
        instance: TaskInstance,
        command: str
    ) -> None:
        """执行任务命令"""
        loop = asyncio.get_event_loop()
        
        # 创建资源监控器
        monitor = ResourceMonitor(task.resources)
        self._resource_monitors[f"{run_id}:{task.id}"] = monitor
        
        def _execute():
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_IGN)
            )
            
            instance.process_id = process.pid
            monitor.start(process.pid)
            
            # 存储进程引用以便终止
            self._running_processes[task.id] = process
            
            # 实时读取输出
            for line in process.stdout:
                line = line.rstrip('\n')
                instance.stdout += line + '\n'
                instance.logs.append({
                    'timestamp': datetime.now().isoformat(),
                    'level': 'info',
                    'message': line
                })
                
                # 写入日志文件
                self.storage.append_task_log(run_id, task.id, line)
                
                # 异步通知日志
                for callback in self._status_callbacks:
                    try:
                        asyncio.ensure_future(callback(
                            "task_log", run_id, {
                                "task_id": task.id,
                                "log": line
                            }
                        ))
                    except Exception:
                        pass
                
                # 检查资源限制
                within_limits, limit_msg = monitor.check_limits()
                if not within_limits:
                    process.terminate()
                    raise Exception(limit_msg)
            
            # 等待进程完成
            return_code = process.wait()
            
            # 移除进程引用
            self._running_processes.pop(task.id, None)
            
            # 更新资源使用
            usage = monitor.get_usage()
            instance.cpu_usage = usage['cpu_percent']
            instance.memory_usage = usage['memory_mb']
            
            instance.exit_code = return_code
            
            if return_code != 0:
                raise Exception(f"命令执行失败，返回码: {return_code}, 输出: {instance.stdout[-1000:]}")
        
        # 使用超时执行
        try:
            await asyncio.wait_for(
                loop.run_in_executor(self._get_executor(), _execute),
                timeout=task.resources.timeout
            )
        except asyncio.TimeoutError:
            self._terminate_task_process(task.id)
            raise
    
    async def _execute_subworkflow(
        self,
        run_id: str,
        task: TaskDefinition,
        instance: TaskInstance,
        context: Dict[str, Any]
    ) -> None:
        """执行子工作流"""
        if not task.subworkflow_id:
            raise Exception("子工作流任务缺少subworkflow_id")
        
        # 加载子工作流
        subworkflow = self.storage.load_workflow(task.subworkflow_id)
        if not subworkflow:
            raise Exception(f"子工作流不存在: {task.subworkflow_id}")
        
        # 创建子工作流运行
        sub_run = WorkflowRun(
            run_id=f"{run_id}_{task.id}",
            workflow_id=task.subworkflow_id,
            status=WorkflowStatus.RUNNING,
            start_time=datetime.now(),
            parameters=context.get('variables', {}),
            trigger_type=TriggerType.API,
            triggered_by=run_id
        )
        
        # 初始化子任务实例
        for sub_task in subworkflow.tasks:
            if sub_task.enabled:
                sub_run.task_instances[sub_task.id] = TaskInstance(
                    task_id=sub_task.id,
                    workflow_id=task.subworkflow_id,
                    run_id=sub_run.run_id
                )
        
        self.storage.save_workflow_run(sub_run)
        
        # 执行子工作流
        sub_scheduler = TaskScheduler(self.storage, self.max_workers)
        
        try:
            sub_run = await sub_scheduler._execute_workflow(sub_run.run_id)
            
            # 检查子工作流结果
            if sub_run and sub_run.status != WorkflowStatus.SUCCESS:
                raise Exception(f"子工作流执行失败: {sub_run.status}")
            
            instance.stdout = f"子工作流 {task.subworkflow_id} 执行完成"
            instance.status = TaskStatus.SUCCESS
            
        except Exception as e:
            instance.status = TaskStatus.FAILED
            instance.error_message = f"子工作流执行失败: {str(e)}"
            raise
    
    def _terminate_task_process(self, task_id: str) -> None:
        """终止任务进程"""
        process = self._running_processes.pop(task_id, None)
        if process:
            try:
                process.terminate()
                process.wait(timeout=5)
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass
    
    async def stop_workflow(self, run_id: str) -> bool:
        """停止工作流执行"""
        run = self._active_runs.get(run_id)
        if not run:
            return False
        
        # 终止所有运行中的任务
        for task_id, instance in run.task_instances.items():
            if instance.status == TaskStatus.RUNNING:
                self._terminate_task_process(task_id)
                instance.status = TaskStatus.CANCELLED
                instance.error_message = "工作流被手动停止"
                instance.end_time = datetime.now()
        
        # 更新状态
        run.status = WorkflowStatus.CANCELLED
        run.end_time = datetime.now()
        
        self.storage.save_workflow_run(run)
        
        # 通知
        await self._notify_status_change(
            "workflow_stop", run_id, run.to_dict()
        )
        
        # 清理
        self._active_runs.pop(run_id, None)
        self._dag_schedulers.pop(run_id, None)
        
        return True
    
    async def retry_task(
        self, 
        run_id: str, 
        task_id: str
    ) -> bool:
        """手动重试任务"""
        run = self._active_runs.get(run_id)
        if not run:
            run = self.storage.load_workflow_run(run_id)
            if not run:
                return False
        
        instance = run.task_instances.get(task_id)
        if not instance:
            return False
        
        if instance.status not in [TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.CANCELLED]:
            return False
        
        # 获取工作流定义
        workflow = self.storage.load_workflow(run.workflow_id)
        if not workflow:
            return False
        
        # 找到任务定义
        task_def = None
        for t in workflow.tasks:
            if t.id == task_id:
                task_def = t
                break
        
        if not task_def:
            return False
        
        # 重置任务状态
        instance.status = TaskStatus.PENDING
        instance.error_message = None
        instance.logs = []
        instance.stdout = ""
        instance.stderr = ""
        instance.retry_count = 0
        
        self.storage.save_workflow_run(run)
        
        # 启动任务执行
        completed_tasks = {
            tid for tid, inst in run.task_instances.items()
            if inst.status == TaskStatus.SUCCESS
        }
        running_tasks = {
            tid for tid, inst in run.task_instances.items()
            if inst.status == TaskStatus.RUNNING
        }
        failed_tasks = set()
        skipped_tasks = set()
        
        asyncio.create_task(
            self._execute_task(
                run_id, task_def,
                {'variables': run.parameters, 'task_instances': {}},
                completed_tasks, running_tasks, failed_tasks, skipped_tasks
            )
        )
        
        return True
    
    def get_active_runs(self) -> Dict[str, Dict]:
        """获取所有活跃运行"""
        return {
            run_id: run.to_dict() 
            for run_id, run in self._active_runs.items()
        }
    
    def is_task_running(self, task_id: str) -> bool:
        """检查任务是否正在运行"""
        return task_id in self._running_processes


class WorkflowRunner:
    """工作流运行管理器"""
    
    def __init__(self, storage: AtomicJSONStorage):
        self.storage = storage
        self.scheduler = TaskScheduler(storage)
        
        # 定时调度器
        self._scheduled_workflows: Dict[str, Dict] = {}
        self._scheduler_task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """启动运行管理器"""
        # 加载需要恢复的运行
        await self._recover_interrupted_runs()
        
        # 启动定时调度
        self._scheduler_task = asyncio.create_task(self._schedule_loop())
    
    async def _recover_interrupted_runs(self) -> None:
        """恢复中断的运行"""
        runs = self.storage.list_workflow_runs()
        
        for run_info in runs:
            if run_info["status"] == WorkflowStatus.RUNNING.value:
                run = self.storage.load_workflow_run(run_info["run_id"])
                if run:
                    logger.info(f"恢复中断的运行: {run.run_id}")
                    # 标记为失败
                    run.status = WorkflowStatus.FAILED
                    run.end_time = datetime.now()
                    run.error_message = "系统重启导致运行中断"
                    self.storage.save_workflow_run(run)
    
    async def _schedule_loop(self) -> None:
        """定时调度循环"""
        while True:
            try:
                # 检查所有工作流的定时配置
                workflows = self.storage.list_workflows()
                
                for wf_info in workflows:
                    workflow = self.storage.load_workflow(wf_info["id"])
                    if workflow and workflow.schedule:
                        # TODO: 实现cron表达式解析
                        pass
                
                await asyncio.sleep(60)  # 每分钟检查一次
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"调度循环错误: {e}")
                await asyncio.sleep(10)
    
    async def stop(self) -> None:
        """停止运行管理器"""
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        
        # 停止所有活跃运行
        for run_id in list(self.scheduler.get_active_runs().keys()):
            await self.scheduler.stop_workflow(run_id)
