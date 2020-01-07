import enum

from ..common.taskinput import TaskInput
from ..common.layout import Layout


class TaskState(enum.Enum):
    NEW = 0
    SUBMITTED = 1
    REMOVED = 2


class Task:

    def __init__(self, task_id, n_outputs, n_workers, config, keep, inputs):
        self.task_id = task_id
        self.inputs = inputs
        self.n_outputs = n_outputs
        self.n_workers = n_workers
        self.config = config
        self.keep = keep
        self.config = config
        self.state = TaskState.NEW

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "inputs": [inp.to_dict() for inp in self.inputs],
            "n_outputs": self.n_outputs,
            "n_workers": self.n_workers,
            "config": self.config,
            "keep": self.keep,
        }

    def output(self, output_id, layout="all_to_all"):
        if layout == "all_to_all":
            layout = Layout(self.n_workers, 0, 0, 0, self.n_workers)
        elif layout == "cycle":
            layout = Layout(self.n_workers, 1, 0, 0, 1)
        else:
            assert isinstance(layout, Layout)
        return TaskInput(self, output_id, layout)
