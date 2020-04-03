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

    def output(self, output_ids="all", layout="all_to_all"):
        if isinstance(output_ids, int):
            output_ids = [output_ids]
        elif output_ids == "all":
            output_ids = list(range(self.n_outputs))
        if layout == "all_to_all":
            layout = Layout(0, 0, 0, self.n_workers * len(output_ids))
        elif layout == "cycle":
            layout = Layout(1, 0, 0, 1)
        else:
            assert isinstance(layout, Layout)
        return TaskInput(self, output_ids, layout)


DEFAULT_ENV = {}
PY_JOB_ARGS = ("python3", "-m", "quake.job", "$TASK_ID", "$RANK", "$DS_PORT")


def new_mpirun_task(n_outputs, n_workers, args, keep=False, task_data=None, inputs=()):
    config = {
        "type": "mpirun",
        "args": args,
        "env": DEFAULT_ENV
    }
    if task_data is not None:
        assert isinstance(task_data, bytes)
        config["data"] = task_data
    return Task(None, n_outputs, n_workers, config, keep, inputs)


def new_py_task(n_outputs, n_workers, keep=False, task_data=None, inputs=()):
    return new_mpirun_task(n_outputs, n_workers, PY_JOB_ARGS, keep, task_data, inputs)


def upload_data(data, keep=False):
    assert isinstance(data, list)
    for d in data:
        assert isinstance(d, bytes)
    config = {
        "type": "upload",
        "data": data,
    }
    return self.new_task(1, len(data), config, keep, ())

