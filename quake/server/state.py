from .scheduler import compute_b_levels
from .task import Task, TaskState
import logging

from ..common.taskinput import TaskInput

logger = logging.getLogger(__file__)


def _check_removal(task, tasks_to_remove):
    if task.keep or task.consumers:
        return
    assert task.state == TaskState.FINISHED
    logger.debug("Removing task %s", task)
    tasks_to_remove.append(task)


class State:

    def __init__(self, workers):
        self.tasks = {}
        self.ready_tasks = []
        self.all_workers = workers
        self.free_workers = list(workers)

    def add_tasks(self, serialized_tasks):
        new_ready_tasks = False
        new_tasks = set()

        task_map = self.tasks
        for tdict in serialized_tasks:
            task_id = tdict["task_id"]
            if task_id in task_map:
                raise Exception("Task id ({}) already used".format(task_id))

        for tdict in serialized_tasks:
            task_id = tdict["task_id"]
            task = Task(task_id, tdict["n_outputs"], tdict["n_workers"], tdict["config"], tdict["keep"])
            logger.debug("Task %s submitted", task_id)
            task_map[task_id] = task
            new_tasks.add(task)
            tdict["_task"] = task

        for tdict in serialized_tasks:
            task = tdict["_task"]
            unfinished_deps = 0
            inputs = [TaskInput.from_dict(data, task_map) for data in tdict["inputs"]]
            deps = frozenset(inp.task for inp in inputs)
            for t in deps:
                assert t.state != TaskState.RELEASED
                assert t.keep or t in new_tasks, "Dependency on not-keep task"
                t.consumers.add(task)
                if not t.state == TaskState.FINISHED:
                    unfinished_deps += 1
            task.inputs = inputs
            task.deps = deps
            task.unfinished_deps = unfinished_deps
            if not unfinished_deps:
                new_ready_tasks = True
                logger.debug("Task %s is ready", task)
                self.ready_tasks.append(task)

        compute_b_levels(task_map)
        return new_ready_tasks

    def schedule(self):
        logger.debug("Scheduling ... top_3_tasks: %s", self.ready_tasks[:3])
        for task in self.ready_tasks[:]:
            if task.n_workers <= len(self.free_workers):
                workers = self.free_workers[:task.n_workers]
                del self.free_workers[:task.n_workers]
                self.ready_tasks.remove(task)
                yield task, workers
        logger.debug("End of scheduling")

    def _release_task(self, task, tasks_to_remove):
        for inp in task.inputs:
            t = inp.task
            if task in t.consumers:
                t.consumers.remove(task)
                _check_removal(t, tasks_to_remove)
        _check_removal(task, tasks_to_remove)

    def on_task_failed(self, task, workers, message):
        logger.error("Task %s FAILED: %s", task, message)
        task.set_error(message)
        tasks_to_remove = []
        self._release_task(task, tasks_to_remove)
        for t in task.recursive_consumers():
            if t.state == TaskState.UNFINISHED:
                t.set_error(message)
        self.free_workers.extend(workers)
        return tasks_to_remove

    def on_task_finished(self, task, workers):
        logger.debug("Task %s finished", task)
        task.set_finished(workers)
        tasks_to_remove = []
        self._release_task(task, tasks_to_remove)
        for t in task.consumers:
            t.unfinished_deps -= 1
            if t.unfinished_deps <= 0:
                assert t.unfinished_deps == 0
                logger.debug("Task %s is ready", t)
                self.ready_tasks.append(t)
        self.free_workers.extend(workers)
        return tasks_to_remove

    def unkeep(self, task_id):
        task = self.tasks.get(task_id)
        if task is None:
            raise Exception("Task '{}' not found".format(task_id))
        if not task.keep:
            raise Exception("Task '{}' does have 'keep' flag".format(task_id))
        task.keep = False
        if task.state == TaskState.UNFINISHED:
            # Do nothing
            return
        elif task.state == TaskState.FINISHED:
            tasks_to_remove = []
            _check_removal(task, tasks_to_remove)
            return tasks_to_remove
        elif task.state == TaskState.ERROR:
            raise Exception(task.error)
        else:
            assert 0