class TaskState:
    UNFINISHED = 1
    FINISHED = 2
    RELEASED = 3
    ERROR = 4


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
        self.events = None
        self.error = None

    def recursive_consumers(self):
        tasks = set()
        stack = [self]
        while stack:
            task = stack.pop()
            for t in task.consumers:
                if t not in tasks:
                    stack.append(t)
                    tasks.append(t)
        return tasks

    def add_event(self, event):
        events = self.events
        if events is None:
            self.events = [event]
        else:
            events.append(events)

    def set_error(self, error):
        self.state = TaskState.ERROR
        self.error = error
        self.fire_events()

    def fire_events(self):
        if self.events:
            for event in self.events:
                event.set()
            self.events = None

    def is_ready(self):
        return self.unfinished_deps == 0

    def __repr__(self):
        return "<Task id={}>".format(self.task_id)
