

def compute_b_levels(tasks):
    stack = []
    to_compute = {}
    for task in tasks.values():
        c = len(task.consumers)
        to_compute[task] = c
        if c == 0:
            stack.append(task)

    stack = []
    while stack:
        task = stack.pop()
        task.b_level = 1 + max((t.b_level for t in task.consumers), default=0)
        for inp in task.inputs:
            t = inp.task
            to_compute[t] -= 1
            v = to_compute[t]
            if v <= 0:
                assert v == 0
                stack.append(t)