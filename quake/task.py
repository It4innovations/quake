
class TaskState:
    UNFINISHED = 1
    FINISHED = 2
    RELEASED = 3


class TaskInput:

    def __init__(self, task: "Task", output_id: int, layout=None):
        assert 0 <= output_id < task.n_outputs
        self.task = task
        self.output_id = output_id
        self.layout = layout


class Task:

    def __init__(self, n_outputs, n_workers, args, inputs=(), keep=False):
        self.task_id = None
        self.inputs = inputs
        self.n_outputs = n_outputs
        self.n_workers = n_workers
        self.args = tuple(args)

        self.state = TaskState.UNFINISHED
        self.keep = keep

        self.deps = frozenset(inp.task for inp in inputs)
        self.unfinished_deps = None
        self.consumers = set()
        self.events = None
        self.callbacks = None

    def add_callbacks(self, callback):
        callbacks = self.callbacks
        if callbacks is None:
            self.callbacks = [callback]
        else:
            callbacks.append(callback)

    @property
    def is_ready(self):
        return self.unfinished_deps == 0

    def __repr__(self):
        return "<Task id={}>".format(self.task_id)