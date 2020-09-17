from quake.common.utils import make_data_name


class TaskState:
    UNFINISHED = 1
    FINISHED = 2
    RELEASED = 3
    ERROR = 4


class Task:
    def __init__(self, task_id, n_outputs, n_workers, config, keep):
        self.task_id = task_id
        self.inputs = []
        self.n_outputs = n_outputs
        self.n_workers = n_workers
        self.config = config

        self.state = TaskState.UNFINISHED
        self.keep = keep

        self.deps = None
        self.unfinished_deps = None
        self.consumers = set()
        self.events = None
        self.error = None
        self.placement = (
            None  # placement[output_id][part_id] -> set of workers where is data placed
        )
        self.sizes = None  # sizes[output_id][part_id] -> sizes of data parts

        self.b_level = None

    def dump(self):
        print("Task {}".format(self.task_id))
        print("\tstate:", self.state)
        print("\tinputs:", self.inputs)
        print("\tn_outputs:", self.n_outputs)
        print("\tn_workers:", self.n_workers)
        print("\tconsumers:", self.consumers)
        print("\tb_level:", self.b_level)
        if self.error:
            print("\terror:", self.error)

    def make_data_name(self, output_id, part):
        return make_data_name(self.task_id, output_id, part)

    def recursive_consumers(self):
        tasks = set()
        stack = [self]
        while stack:
            task = stack.pop()
            for t in task.consumers:
                if t not in tasks:
                    stack.append(t)
                    tasks.add(t)
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

    def set_finished(self, workers, sizes):
        assert self.state == TaskState.UNFINISHED
        assert len(workers) == self.n_workers
        assert len(sizes) == self.n_outputs
        assert not sizes or len(sizes[0]) == self.n_workers

        self.state = TaskState.FINISHED
        self.placement = [[{w} for w in workers] for _ in range(self.n_outputs)]
        self.sizes = sizes
        self._fire_events()

    def _fake_finish(self, placement, sizes):
        self.placement = placement
        self.sizes = sizes
        self.state = TaskState.FINISHED

    def set_released(self):
        assert self.state == TaskState.FINISHED
        self.state = TaskState.RELEASED
        self.placement = None
        self.consumers = None
        self.sizes = None

    def __repr__(self):
        return "<Task id={}>".format(self.task_id)
