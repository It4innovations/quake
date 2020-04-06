
from .base.plan import Plan
from .wrapper import FunctionWrapper, ArgConfig

import pickle


global_plan = Plan()
global_client = None


def mpi_task(*, n_processes, n_outputs=1):
    def _builder(fn):
        return FunctionWrapper(fn, n_processes, n_outputs, global_plan)
    return _builder


def arg(name, layout="all_to_all"):
    def _builder(fn):
        if isinstance(fn, FunctionWrapper):
            configs = fn.arg_configs
        elif hasattr(fn, "_quake_args"):
            configs = fn._quake_args
        else:
            configs = {}
            fn._quake_args = configs
        configs[name] = ArgConfig(layout)
        return fn
    return _builder



def wait(task):
    _flush_global_plan()
    global_client.wait(task)


def wait_all(tasks):
    _flush_global_plan()
    global_client.wait_all(tasks)


def gather(task, output_id=None, collapse_single_output=True):
    _flush_global_plan()
    if output_id is None and task.n_outputs == 1 and collapse_single_output:
        output_id = 0
    result = global_client.gather(task, output_id)
    if output_id is not None:
        return [pickle.loads(r) for r in result]
    else:
        return [[pickle.loads(c) for c in r]for r in result]


def remove(task):
    global_client.remove(task)


def reset_global_plan():
    global_plan.take_tasks()


def set_global_client(client):
    global global_client
    global_client = client


def _flush_global_plan():
    tasks = global_plan.take_tasks()
    if tasks:
        global_client.submit(tasks)