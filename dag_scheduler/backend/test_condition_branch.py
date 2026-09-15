"""条件分支端到端验证：真/假两个走向各运行一次"""
import sys
import os
import tempfile
import time
import asyncio

sys.path.insert(0, os.path.dirname(__file__))

# 本地验证环境缺少 psutil 时注入最小桩
try:
    import psutil  # noqa: F401
except ImportError:
    import types
    _stub = types.ModuleType("psutil")

    class _NoSuchProcess(Exception):
        pass

    class _AccessDenied(Exception):
        pass

    class _Process:
        def __init__(self, pid):
            self.pid = pid

        def cpu_percent(self):
            return 0.0

        def memory_info(self):
            class _Mem:
                rss = 0
            return _Mem()

    _stub.NoSuchProcess = _NoSuchProcess
    _stub.AccessDenied = _AccessDenied
    _stub.Process = _Process
    sys.modules["psutil"] = _stub

from models import WorkflowDefinition, TaskDefinition, TaskType, Condition, ResourceLimit
from storage import AtomicJSONStorage
from scheduler import TaskScheduler
from dag_engine import DAGParser


def build_workflow():
    tasks = [
        TaskDefinition(
            id="start", name="准备", type=TaskType.SHELL,
            command="echo start",
            position={"x": 80, "y": 200},
            resources=ResourceLimit(timeout=30, max_retries=0),
        ),
        TaskDefinition(
            id="gate", name="是否生产环境", type=TaskType.CONDITION,
            dependencies=["start"],
            condition=Condition(
                expression="env == 'prod'",
                true_task="prod_task",
                false_task="test_task",
            ),
            position={"x": 340, "y": 200},
            resources=ResourceLimit(timeout=30, max_retries=0),
        ),
        TaskDefinition(
            id="prod_task", name="生产部署", type=TaskType.SHELL,
            dependencies=["gate"], command="echo deploying-prod",
            position={"x": 620, "y": 80},
            resources=ResourceLimit(timeout=30, max_retries=0),
        ),
        TaskDefinition(
            id="test_task", name="测试部署", type=TaskType.SHELL,
            dependencies=["gate"], command="echo deploying-test",
            position={"x": 620, "y": 320},
            resources=ResourceLimit(timeout=30, max_retries=0),
        ),
        TaskDefinition(
            id="notify", name="通知", type=TaskType.SHELL,
            dependencies=["prod_task", "test_task"], command="echo done",
            position={"x": 900, "y": 200},
            resources=ResourceLimit(timeout=30, max_retries=0),
        ),
    ]
    wf = WorkflowDefinition(id="cond-demo", name="条件分支演示", tasks=tasks)

    # 先过校验
    errors = DAGParser.from_json(wf.to_dict())
    return wf


def run_once(storage, wf, env):
    scheduler = TaskScheduler(storage, max_workers=4)
    events = []
    async def cb(event, run_id, data):
        events.append((event, data.get("task_id"), data.get("reason", "")))
    scheduler.on_status_change(cb)

    run = scheduler.start_workflow_sync(wf.id, parameters={"env": env})
    run_id = run.run_id

    # 等待后台线程执行完
    for _ in range(100):
        time.sleep(0.1)
        loaded = storage.load_workflow_run(run_id)
        if loaded.status.value in ("success", "failed"):
            run = loaded
            break

    print(f"\n===== env={env} -> 工作流状态: {run.status.value} =====")
    for tid, inst in run.task_instances.items():
        extra = f" branch={inst.branch_result}" if inst.branch_result else ""
        extra += f" reason={inst.skip_reason}" if inst.skip_reason else ""
        print(f"  {tid:12s} {inst.status.value:8s}{extra}")
    print(f"  统计: success={run.completed_tasks} failed={run.failed_tasks} skipped={run.skipped_tasks}")
    skipped_events = [e for e in events if e[0] == "task_skipped"]
    print(f"  task_skipped 事件: {skipped_events}")
    return run


def main():
    tmp = tempfile.mkdtemp()
    storage = AtomicJSONStorage(tmp)
    wf = build_workflow()
    storage.save_workflow(wf)

    ok = True

    # 1. env=prod：prod_task 执行，test_task 跳过，notify 仍执行
    r = run_once(storage, wf, "prod")
    st = {t: i.status.value for t, i in r.task_instances.items()}
    ok &= st["prod_task"] == "success"
    ok &= st["test_task"] == "skipped"
    ok &= st["notify"] == "success"
    ok &= st["gate"] == "success"
    ok &= r.status.value == "success"
    prod_inst = r.task_instances["test_task"]
    ok &= prod_inst.skip_reason is not None

    # 2. env=test：test_task 执行，prod_task 跳过
    r = run_once(storage, wf, "test")
    st = {t: i.status.value for t, i in r.task_instances.items()}
    ok &= st["test_task"] == "success"
    ok &= st["prod_task"] == "skipped"
    ok &= st["notify"] == "success"
    ok &= r.status.value == "success"

    print("\n结果:", "全部通过 ✅" if ok else "存在失败 ❌")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
