import collections
import inspect
import pickle

import cloudpickle

from .base.task import new_py_task, make_input, Task
from .job import _set_rank
from ..job.config import JobConfiguration


def task_runner(jctx, input_data, python_job):
    _set_rank(jctx.rank)
    return python_job.run(input_data)


def _load(obj):
    if isinstance(obj, bytes):
        return pickle.loads(obj)
    if len(obj) == 1:
        return _load(obj[0])
    return [_load(o) for o in obj]


class PythonJob:

    def __init__(self, pickled_fn, task_args, const_args):
        self.pickled_fn = pickled_fn
        self.task_args = task_args
        self.const_args = const_args

    def run(self, input_data):
        # kwargs = {name: pickle.loads(input_data[value]) for name, value in self.task_args.items()}
        kwargs = self.const_args
        for name, value in self.task_args.items():
            kwargs[name] = _load(input_data[value])
        result = cloudpickle.loads(self.pickled_fn)(**kwargs)
        return [pickle.dumps(result)]


ArgConfig = collections.namedtuple("ArgConfig", "layout")


class FunctionWrapper:

    def __init__(self, fn, n_processes, n_outputs, plan):
        self.fn = fn
        self.signature = inspect.signature(fn)
        self.n_processes = n_processes
        self.n_outputs = n_outputs
        self.plan = plan
        if hasattr(fn, "_quake_args"):
            self.arg_configs = fn._quake_args
            assert isinstance(self.arg_configs, dict)
            delattr(fn, "_quake_args")
        else:
            self.arg_configs = {}

        self._pickled_fn = None

    def pickle_fn(self):
        if self._pickled_fn:
            return self._pickled_fn
        else:
            self._pickled_fn = cloudpickle.dumps(self.fn)
            return self._pickled_fn

    def _prepare_inputs(self, args, kwargs):
        binding = self.signature.bind(*args, **kwargs)
        inputs = []
        task_args = {}
        const_args = {}
        for name, value in binding.arguments.items():
            arg_config = self.arg_configs.get(name)
            if isinstance(value, ResultProxy):
                if arg_config:
                    layout = arg_config.layout
                else:
                    layout = "all_to_all"
                task_args[name] = len(inputs)
                inputs.append(make_input(value.task, layout=layout))
            else:
                assert not isinstance(value, Task)
                if arg_config and arg_config.layout:
                    raise Exception("Non-task result is used as argument with layout")
                const_args[name] = value
        return inputs, task_args, const_args

    def __repr__(self):
        return "<FunctionWrapper of '{}'>".format(self.fn.__class__.__name__)

    def __call__(self, *args, keep=False, **kwargs):
        inputs, task_args, const_args = self._prepare_inputs(args, kwargs)
        payload = PythonJob(self.pickle_fn(), task_args, const_args)
        config = pickle.dumps(JobConfiguration(task_runner, self.n_outputs, payload))
        task = new_py_task(self.n_outputs, self.n_processes, keep, config, inputs)
        self.plan.add_task(task)
        return ResultProxy(task)


class ResultProxy:

    __slots__ = ["task"]

    def __init__(self, task):
        self.task = task