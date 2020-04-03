from ..job.config import JobConfiguration
from .task import new_py_task
import cloudpickle
import pickle


def task_runner(jctx, input_data, python_job):
    return python_job.run()


class PythonJob:

    def __init__(self, pickled_fn):
        self.pickled_fn = pickled_fn

    def run(self):
        result = cloudpickle.loads(self.pickled_fn)()
        return [pickle.dumps(result)]


class FunctionWrapper:

    def __init__(self, fn, n_processes, n_outputs, plan):
        self.fn = fn
        self.n_processes = n_processes
        self.n_outputs = n_outputs
        self.plan = plan

        self._pickled_fn = None

    def pickle_fn(self):
        if self._pickled_fn:
            return self._pickled_fn
        else:
            self._pickled_fn = cloudpickle.dumps(self.fn)
            return self._pickled_fn

    def __repr__(self):
        return "<FunctionWrapper of '{}'>".format(self.fn.__class__.__name__)

    def __call__(self, *args, keep=False, **kwargs):
        inputs = []
        payload = PythonJob(self.pickle_fn())
        config = pickle.dumps(JobConfiguration(task_runner, self.n_outputs, payload))
        task = new_py_task(self.n_outputs, self.n_processes, keep, config, inputs)
        self.plan.add_task(task)
        return task