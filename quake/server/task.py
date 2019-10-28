class TaskState:
    UNFINISHED = 1
    FINISHED = 2
    RELEASED = 3
    ERROR = 4


class Task:

    def __init__(self, task_id, n_outputs, n_workers, args, keep, config):
        assert isinstance(config, bytes) or config is None
        self.task_id = task_id
        self.inputs = []
        self.n_outputs = n_outputs
        self.n_workers = n_workers
        self.args = tuple(args)
        self.config = config

        self.state = TaskState.UNFINISHED
        self.keep = keep

        self.deps = None
        self.unfinished_deps = None
        self.consumers = set()
        self.events = None
        self.events = None
        self.error = None
        self.placement = None

    def make_data_name(self, output_id, part):
        return "data_{}_{}_{}".format(self.task_id, output_id, part)

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
        self._fire_events()

    def _fire_events(self):
        if self.events:
            for event in self.events:
                event.set()
            self.events = None

    def is_ready(self):
        return self.unfinished_deps == 0

    def set_finished(self, workers):
        assert self.state == TaskState.UNFINISHED
        assert len(workers) == self.n_workers
        self.state = TaskState.FINISHED
        self.placement = [{w} for w in workers]
        self._fire_events()

    def __repr__(self):
        return "<Task id={} w={}>".format(self.task_id, self.n_workers)
