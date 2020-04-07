import pickle

from .base.plan import Plan
from .wrapper import FunctionWrapper, ArgConfig, ResultProxy

global_plan = Plan()
global_client = None

# ===== DECORATORS =========================


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


# ===== PUBLIC FUNCTIONS =====================


def wait(result):
    _flush_global_plan()
    global_client.wait(_get_task(result))


def wait_all(results):
    _flush_global_plan()
    global_client.wait_all([_get_task(result) for result in results])


def gather(result, output_id=None, collapse_single_output=True):
    _flush_global_plan()
    task = _get_task(result)
    if output_id is None and task.n_outputs == 1 and collapse_single_output:
        output_id = 0
    result = global_client.gather(task, output_id)
    if output_id is not None:
        return [pickle.loads(r) for r in result]
    else:
        return [[pickle.loads(c) for c in r] for r in result]


def remove(task):
    global_client.remove(task)


# ===== MISC PUBLIC FUNCTIONS ==============


def reset_global_plan():
    global_plan.take_tasks()


def set_global_client(client):
    global global_client
    global_client = client


# ===== Internals ==============


def _flush_global_plan():
    tasks = global_plan.take_tasks()
    if tasks:
        global_client.submit(tasks)


def _get_task(obj):
    if isinstance(obj, ResultProxy):
        return obj.task
    else:
        raise Exception("ResultProxy expected, got '{}'".format(repr(obj)))