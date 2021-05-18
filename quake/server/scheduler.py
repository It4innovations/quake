import logging

from .task import TaskState

logger = logging.getLogger(__file__)


def compute_b_levels(tasks):
    stack = []
    to_compute = {}
    for task in tasks.values():
        if task.state != TaskState.UNFINISHED:
            continue
        c = len(task.consumers)
        to_compute[task] = c
        if c == 0:
            stack.append(task)

    while stack:
        task = stack.pop()
        task.b_level = 1 + max((t.b_level for t in task.consumers), default=0)
        for t in task.deps:
            if task.state != TaskState.UNFINISHED:
                continue
            to_compute[t] -= 1
            v = to_compute[t]
            if v <= 0:
                assert v == 0
                stack.append(t)
