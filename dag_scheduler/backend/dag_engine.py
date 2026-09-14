"""增强版DAG引擎 - 条件分支、子工作流、执行计划生成"""
from typing import List, Dict, Set, Optional, Tuple, Any
from collections import defaultdict, deque
import logging
import json
import re

from models import (
    TaskDefinition, WorkflowDefinition, TaskType, TaskStatus,
    Condition, TaskInstance, Parameter, ParamType
)

logger = logging.getLogger(__name__)


class DAGValidationError(Exception):
    """DAG验证错误"""
    pass


class DAGCycleError(DAGValidationError):
    """循环依赖错误"""
    pass


class TopologicalSortResult:
    """拓扑排序结果"""
    def __init__(self):
        self.sorted_tasks: List[str] = []
        self.levels: List[List[str]] = []  # 按层级分组
        self.has_cycle: bool = False
        self.cycle_nodes: List[str] = []
        self.in_degree: Dict[str, int] = {}
        self.out_degree: Dict[str, int] = {}


class ExpressionEvaluator:
    """安全的表达式评估器"""
    
    OPERATORS = {
        '==': lambda a, b: a == b,
        '!=': lambda a, b: a != b,
        '>': lambda a, b: a > b,
        '>=': lambda a, b: a >= b,
        '<': lambda a, b: a < b,
        '<=': lambda a, b: a <= b,
        'in': lambda a, b: a in b,
        'not in': lambda a, b: a not in b,
        'contains': lambda a, b: b in a,
        'startswith': lambda a, b: str(a).startswith(str(b)),
        'endswith': lambda a, b: str(a).endswith(str(b)),
        'matches': lambda a, b: bool(re.match(str(b), str(a))),
    }
    
    @classmethod
    def evaluate(cls, expression: str, context: Dict[str, Any]) -> bool:
        """评估表达式
        
        支持的表达式格式:
        - "status == 'success'"
        - "retry_count < 3 and exit_code == 0"
        - "output contains 'error'"
        - "not is_final"
        """
        try:
            # 替换变量
            processed = cls._substitute_variables(expression, context)
            
            # 分割逻辑操作符
            and_parts = re.split(r'\s+and\s+', processed)
            
            results = []
            for part in and_parts:
                part = part.strip()
                if part.startswith('not '):
                    result = not cls._evaluate_single(part[4:], context)
                    results.append(result)
                else:
                    result = cls._evaluate_single(part, context)
                    results.append(result)
            
            return all(results)
            
        except Exception as e:
            logger.error(f"表达式评估失败: {expression}, 错误: {e}")
            return False
    
    @classmethod
    def _substitute_variables(cls, expression: str, context: Dict[str, Any]) -> str:
        """替换表达式中的变量"""
        def replace_var(match):
            var_name = match.group(1)
            value = context.get(var_name)
            if value is None:
                return 'None'
            elif isinstance(value, bool):
                return str(value)
            elif isinstance(value, (int, float)):
                return str(value)
            elif isinstance(value, str):
                return f"'{value}'"
            else:
                return f"'{str(value)}'"
        
        return re.sub(r'\$\{(\w+)\}', replace_var, expression)
    
    @classmethod
    def _evaluate_single(cls, expression: str, context: Dict[str, Any]) -> bool:
        """评估单个表达式"""
        expression = expression.strip()
        
        # 处理布尔值
        if expression.lower() == 'true':
            return True
        if expression.lower() == 'false':
            return False
        
        # 处理变量（无操作符）
        if re.match(r'^\w+$', expression):
            return bool(context.get(expression, False))
        
        # 查找操作符
        for op in sorted(cls.OPERATORS.keys(), key=len, reverse=True):
            if f' {op} ' in expression:
                left, right = expression.split(f' {op} ', 1)
                left_val = cls._parse_value(left.strip(), context)
                right_val = cls._parse_value(right.strip(), context)
                return cls.OPERATORS[op](left_val, right_val)
        
        raise ValueError(f"无法解析表达式: {expression}")
    
    @classmethod
    def _parse_value(cls, value_str: str, context: Dict[str, Any]) -> Any:
        """解析值"""
        # 字符串
        if (value_str.startswith("'") and value_str.endswith("'")) or \
           (value_str.startswith('"') and value_str.endswith('"')):
            return value_str[1:-1]
        
        # 数字
        try:
            if '.' in value_str:
                return float(value_str)
            return int(value_str)
        except ValueError:
            pass
        
        # 布尔值
        if value_str.lower() == 'true':
            return True
        if value_str.lower() == 'false':
            return False
        
        # None
        if value_str.lower() == 'none':
            return None
        
        # 变量
        return context.get(value_str, value_str)


class DAGScheduler:
    """增强版DAG调度器"""
    
    def __init__(self, workflow: WorkflowDefinition):
        self.workflow = workflow
        self.task_map: Dict[str, TaskDefinition] = {
            t.id: t for t in workflow.tasks
        }
        self.adjacency: Dict[str, List[str]] = defaultdict(list)
        self.reverse_adjacency: Dict[str, List[str]] = defaultdict(list)
        self.in_degree: Dict[str, int] = {}
        self.out_degree: Dict[str, int] = {}
        
        self._build_graph()
    
    def _build_graph(self) -> None:
        """构建邻接表和度表"""
        # 初始化
        for task in self.workflow.tasks:
            self.in_degree[task.id] = 0
            self.out_degree[task.id] = 0
        
        # 构建图
        for task in self.workflow.tasks:
            for dep_id in task.dependencies:
                if dep_id not in self.task_map:
                    raise DAGValidationError(
                        f"任务 {task.id} 依赖的任务 {dep_id} 不存在"
                    )
                self.adjacency[dep_id].append(task.id)
                self.reverse_adjacency[task.id].append(dep_id)
                self.in_degree[task.id] += 1
                self.out_degree[dep_id] += 1
    
    def validate(self) -> List[str]:
        """验证DAG有效性"""
        errors = []
        
        # 检查循环依赖
        result = self.topological_sort()
        if result.has_cycle:
            errors.append(f"检测到循环依赖，涉及任务: {', '.join(result.cycle_nodes)}")
        
        # 检查依赖是否存在
        for task in self.workflow.tasks:
            for dep_id in task.dependencies:
                if dep_id not in self.task_map:
                    errors.append(f"任务 {task.id} 依赖的任务 {dep_id} 不存在")
        
        # 检查条件任务配置
        for task in self.workflow.tasks:
            if task.type == TaskType.CONDITION:
                if not task.condition:
                    errors.append(f"条件任务 {task.id} 缺少条件配置")
            
            if task.type == TaskType.SUBWORKFLOW:
                if not task.subworkflow_id:
                    errors.append(f"子工作流任务 {task.id} 缺少子工作流ID")
        
        return errors
    
    def topological_sort(self) -> TopologicalSortResult:
        """Kahn算法拓扑排序"""
        result = TopologicalSortResult()
        
        # 复制度表
        in_degree = dict(self.in_degree)
        
        # 找出所有入度为0的节点
        queue = deque([t for t, d in in_degree.items() if d == 0])
        visited_count = 0
        
        while queue:
            level_size = len(queue)
            current_level = []
            
            for _ in range(level_size):
                node = queue.popleft()
                current_level.append(node)
                result.sorted_tasks.append(node)
                visited_count += 1
                
                # 更新邻居节点入度
                for neighbor in self.adjacency[node]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
            
            if current_level:
                result.levels.append(current_level)
        
        # 检测循环
        if visited_count != len(self.workflow.tasks):
            result.has_cycle = True
            result.cycle_nodes = [
                t for t, d in in_degree.items() if d > 0
            ]
        
        result.in_degree = dict(self.in_degree)
        result.out_degree = dict(self.out_degree)
        
        return result
    
    def get_execution_levels(self) -> List[List[TaskDefinition]]:
        """获取按层级分组的执行计划"""
        sort_result = self.topological_sort()
        
        if sort_result.has_cycle:
            raise DAGValidationError("无法生成执行计划: 检测到循环依赖")
        
        levels = []
        for level_ids in sort_result.levels:
            level_tasks = []
            for tid in level_ids:
                if tid in self.task_map:
                    task = self.task_map[tid]
                    if task.enabled:  # 只包含启用的任务
                        level_tasks.append(task)
            if level_tasks:
                levels.append(level_tasks)
        
        return levels
    
    def get_ready_tasks(
        self, 
        completed_tasks: Set[str], 
        running_tasks: Set[str],
        failed_tasks: Set[str],
        skipped_tasks: Set[str],
        context: Optional[Dict[str, Any]] = None
    ) -> List[TaskDefinition]:
        """获取当前可执行的任务"""
        ready = []
        
        for task in self.workflow.tasks:
            if task.id in completed_tasks or task.id in running_tasks:
                continue
            if task.id in failed_tasks or task.id in skipped_tasks:
                continue
            if not task.enabled:
                continue
            
            # 检查所有依赖是否已完成
            all_deps_met = True
            for dep_id in task.dependencies:
                if dep_id not in completed_tasks and dep_id not in skipped_tasks:
                    all_deps_met = False
                    break
            
            if not all_deps_met:
                continue
            
            # 检查条件任务
            if task.condition and context:
                # 获取前驱任务的输出
                condition_context = self._build_condition_context(task.id, context)
                should_execute = task.condition.evaluate(condition_context)
                
                if not should_execute:
                    continue
            
            ready.append(task)
        
        return ready
    
    def _build_condition_context(self, task_id: str, run_context: Dict[str, Any]) -> Dict[str, Any]:
        """构建条件评估上下文"""
        context = {}
        
        # 添加运行变量
        context.update(run_context.get('variables', {}))
        
        # 添加前驱任务的状态和输出
        for dep_id in self.reverse_adjacency[task_id]:
            task_instance = run_context.get('task_instances', {}).get(dep_id)
            if task_instance:
                context[f'{dep_id}_status'] = task_instance.get('status', 'unknown')
                context[f'{dep_id}_output'] = task_instance.get('outputs', {})
        
        return context
    
    def get_task_depth(self, task_id: str) -> int:
        """获取任务在DAG中的深度"""
        if task_id not in self.task_map:
            return 0
        
        depth_cache = {}
        
        def _calc_depth(tid: str) -> int:
            if tid in depth_cache:
                return depth_cache[tid]
            
            deps = self.reverse_adjacency[tid]
            if not deps:
                depth_cache[tid] = 0
                return 0
            
            max_dep_depth = max(_calc_depth(d) for d in deps)
            depth_cache[tid] = max_dep_depth + 1
            return max_dep_depth + 1
        
        return _calc_depth(task_id)
    
    def get_task_dependencies(self, task_id: str, include_self: bool = False) -> Set[str]:
        """获取任务的所有间接依赖"""
        deps = set()
        queue = deque([task_id])
        
        while queue:
            current = queue.popleft()
            for dep in self.reverse_adjacency[current]:
                if dep not in deps:
                    deps.add(dep)
                    queue.append(dep)
        
        if include_self:
            deps.add(task_id)
        
        return deps
    
    def get_task_dependents(self, task_id: str, include_self: bool = False) -> Set[str]:
        """获取所有依赖该任务的任务"""
        dependents = set()
        queue = deque([task_id])
        
        while queue:
            current = queue.popleft()
            for dep in self.adjacency[current]:
                if dep not in dependents:
                    dependents.add(dep)
                    queue.append(dep)
        
        if include_self:
            dependents.add(task_id)
        
        return dependents
    
    def calculate_critical_path(self) -> Tuple[List[str], int]:
        """计算关键路径（最长路径）"""
        sort_result = self.topological_sort()
        
        if sort_result.has_cycle:
            return [], 0
        
        # 计算每个节点到起点的最长距离
        longest_path = {tid: 0 for tid in sort_result.sorted_tasks}
        predecessor = {tid: None for tid in sort_result.sorted_tasks}
        
        for task_id in sort_result.sorted_tasks:
            for neighbor in self.adjacency[task_id]:
                if longest_path[task_id] + 1 > longest_path[neighbor]:
                    longest_path[neighbor] = longest_path[task_id] + 1
                    predecessor[neighbor] = task_id
        
        # 找出最长路径
        end_node = max(longest_path, key=longest_path.get)
        max_length = longest_path[end_node]
        
        # 回溯路径
        path = []
        current = end_node
        while current is not None:
            path.append(current)
            current = predecessor[current]
        path.reverse()
        
        return path, max_length
    
    def calculate_parallelism(self) -> Dict[str, Any]:
        """计算并行度分析"""
        sort_result = self.topological_sort()
        
        max_parallel = max(len(level) for level in sort_result.levels) if sort_result.levels else 0
        avg_parallel = sum(len(level) for level in sort_result.levels) / len(sort_result.levels) if sort_result.levels else 0
        
        # 计算每个任务的并行组
        parallel_groups = {}
        for i, level in enumerate(sort_result.levels):
            for task_id in level:
                parallel_groups[task_id] = i
        
        return {
            "max_parallelism": max_parallel,
            "average_parallelism": avg_parallel,
            "total_levels": len(sort_result.levels),
            "parallel_groups": parallel_groups,
            "critical_path": self.calculate_critical_path()[0]
        }
    
    def estimate_execution_time(self, task_durations: Dict[str, float]) -> Dict[str, Any]:
        """估算执行时间"""
        sort_result = self.topological_sort()
        
        # 计算每个任务的最早完成时间
        earliest_finish = {}
        for task_id in sort_result.sorted_tasks:
            duration = task_durations.get(task_id, 1.0)
            if not self.reverse_adjacency[task_id]:
                earliest_finish[task_id] = duration
            else:
                max_dep_finish = max(
                    earliest_finish.get(dep, 0) 
                    for dep in self.reverse_adjacency[task_id]
                )
                earliest_finish[task_id] = max_dep_finish + duration
        
        # 总时间（串行）
        total_serial = sum(task_durations.get(tid, 1.0) for tid in sort_result.sorted_tasks)
        
        # 关键路径时间（并行）
        critical_path_time = max(earliest_finish.values()) if earliest_finish else 0
        
        # 加速比
        speedup = total_serial / critical_path_time if critical_path_time > 0 else 1
        
        return {
            "total_serial_time": total_serial,
            "critical_path_time": critical_path_time,
            "speedup_ratio": speedup,
            "task_finish_times": earliest_finish
        }


class DAGParser:
    """增强版DAG解析器"""
    
    @staticmethod
    def from_json(data: Dict) -> WorkflowDefinition:
        """从JSON数据解析工作流定义"""
        tasks = []
        for task_data in data.get("tasks", []):
            task = TaskDefinition.from_dict(task_data)
            tasks.append(task)
        
        parameters = [Parameter.from_dict(p) for p in data.get("parameters", [])]
        
        workflow = WorkflowDefinition(
            id=data["id"],
            name=data["name"],
            version=data.get("version", "1.0.0"),
            tasks=tasks,
            parameters=parameters,
            max_concurrency=data.get("max_concurrency", 10),
            max_task_concurrency=data.get("max_task_concurrency", 5),
            schedule=data.get("schedule"),
            description=data.get("description", ""),
            tags=data.get("tags", []),
            category=data.get("category", "default"),
            owner=data.get("owner", ""),
            workflow_timeout=data.get("workflow_timeout", 86400)
        )
        
        # 验证DAG
        scheduler = DAGScheduler(workflow)
        errors = scheduler.validate()
        if errors:
            raise DAGValidationError(f"工作流验证失败: {'; '.join(errors)}")
        
        return workflow
    
    @staticmethod
    def to_json(workflow: WorkflowDefinition) -> Dict:
        """将工作流定义转换为JSON"""
        return workflow.to_dict()
    
    @staticmethod
    def validate_expression(expression: str) -> Tuple[bool, str]:
        """验证条件表达式"""
        try:
            # 简单的语法检查
            if not expression.strip():
                return False, "表达式不能为空"
            
            # 检查基本语法
            if '==' in expression or '!=' in expression or '>' in expression or '<' in expression:
                return True, "有效"
            
            if expression.lower() in ('true', 'false'):
                return True, "有效"
            
            if re.match(r'^\w+$', expression):
                return True, "变量引用"
            
            return True, "有效"
            
        except Exception as e:
            return False, str(e)


class VariableResolver:
    """变量解析器 - 处理任务间的数据传递"""
    
    @staticmethod
    def resolve_template(template: str, context: Dict[str, Any]) -> str:
        """解析模板字符串中的变量
        
        支持格式:
        - ${var_name} - 变量
        - ${task_id.output} - 任务输出
        - ${task_id.status} - 任务状态
        - ${param_name} - 参数
        """
        def replace_var(match):
            var_path = match.group(1)
            parts = var_path.split('.')
            
            if len(parts) == 1:
                # 简单变量
                value = context.get(parts[0])
            elif len(parts) == 2:
                # 任务输出或状态
                task_id, field = parts
                task_data = context.get('task_instances', {}).get(task_id, {})
                value = task_data.get(field) if isinstance(task_data, dict) else None
            else:
                value = None
            
            if value is None:
                return '${' + var_path + '}'  # 保留未解析的变量
            return str(value)
        
        return re.sub(r'\$\{([^}]+)\}', replace_var, template)
    
    @staticmethod
    def resolve_command(command: str, context: Dict[str, Any]) -> str:
        """解析命令模板"""
        return VariableResolver.resolve_template(command, context)
    
    @staticmethod
    def extract_outputs(output_mapping: Dict[str, str], raw_output: str) -> Dict[str, Any]:
        """从原始输出中提取结构化数据"""
        outputs = {}
        
        for key, pattern in output_mapping.items():
            try:
                # 简单的正则提取
                match = re.search(pattern, raw_output)
                if match:
                    outputs[key] = match.group(1) if match.lastindex else match.group(0)
            except Exception as e:
                logger.warning(f"输出提取失败: {key}, {e}")
        
        return outputs
