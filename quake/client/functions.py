
from .plan import Plan
from .wrapper import FunctionWrapper

global_plan = Plan()
global_client = None


def mpi_task(*, n_processes, n_outputs=1):
    def _builder(fn):
        return FunctionWrapper(fn, n_processes, n_outputs, global_plan)
    return _builder


def wait(task):
    _flush_global_plan()
    global_client.wait(task)


def wait_all(tasks):
    _flush_global_plan()
    global_client.wait_all(tasks)


def reset_global_plan():
    global_plan.take_tasks()


def set_global_client(client):
    global global_client
    global_client = client


def _flush_global_plan():
    tasks = global_plan.take_tasks()
    if tasks:
        global_client.submit(tasks)