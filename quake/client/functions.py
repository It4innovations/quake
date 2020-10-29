import pickle
import os

from .base.plan import Plan
from . import Client
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
    client = ensure_global_client()
    _flush_global_plan(client)
    client.wait(_get_task(result))


def wait_all(results):
    client = ensure_global_client()
    _flush_global_plan(client)
    client.wait_all([_get_task(result) for result in results])


def gather(result, output_id=None, collapse_single_output=True):
    client = ensure_global_client()
    task = _get_task(result)
    if not task.keep:
        if not task.is_new():
            raise Exception("Task was already submitted, but without 'keep' flag")
        task.keep = True
        temp_keep = True
    else:
        temp_keep = False
    _flush_global_plan(client)

    if output_id is None and task.n_outputs == 1 and collapse_single_output:
        output_id = 0
    result = client.gather(task, output_id)

    if temp_keep:
        client.remove(task)

    if output_id is not None:
        return [pickle.loads(r) for r in result]
    else:
        return [[pickle.loads(c) for c in r] for r in result]


def remove(result):
    task = _get_task(result)
    ensure_global_client().remove(task)


# ===== MISC PUBLIC FUNCTIONS ==============


def reset_global_plan():
    global_plan.take_tasks()


def set_global_client(client):
    global global_client
    global_client = client


def ensure_global_client():
    global global_client
    if global_client is None:
        server = os.environ.get("QUAKE_SERVER")
        if server is None:
            raise Exception(
                "No global server is defined."
                "Set variable QUAKE_SERVER or call quake.client.set_global_client()"
            )
        if ":" in server:
            hostname, port = server.rsplit(":", 1)
            try:
                port = int(port)
            except ValueError:
                raise Exception("Invalid format of QUAKE_SERVER variable")
            global_client = Client(hostname, port)
        else:
            global_client = Client(server)
    return global_client


# ===== Internals ==============


def _flush_global_plan(client):
    tasks = global_plan.take_tasks()
    if tasks:
        client.submit(tasks)


def _get_task(obj):
    if isinstance(obj, ResultProxy):
        return obj.task
    else:
        raise Exception("ResultProxy expected, got '{}'".format(repr(obj)))
