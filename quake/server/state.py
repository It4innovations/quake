import logging
import sys

from ..common.taskinput import TaskInput
from .scheduler import compute_b_levels
from .task import Task, TaskState

logger = logging.getLogger(__file__)


def _check_removal(task, tasks_to_remove):
    if task.keep or task.consumers:
        return
    assert task.state == TaskState.FINISHED
    logger.debug("Removing task %s", task)
    tasks_to_remove.append(task)


def _task_b_level(task):
    return task.b_level


def _release_task(task, tasks_to_remove):
    for inp in task.inputs:
        t = inp.task
        if task in t.consumers:
            t.consumers.remove(task)
            _check_removal(t, tasks_to_remove)
    if task.state == TaskState.FINISHED:
        _check_removal(task, tasks_to_remove)


def _transfer_costs(task, part_id, worker):
    cost = 0
    for inp in task.inputs:
        t = inp.task
        placement = t.placement
        sizes = t.sizes
        for i, j in inp.layout.iterate(part_id, inp.output_ids, t.n_workers):
            if worker not in placement[i][j]:
                cost += sizes[i][j]
    return cost


def _update_placement(task, workers):
    for inp in task.inputs:
        t = inp.task
        output_ids = inp.output_ids
        n_parts = t.n_workers
        for rank in range(t.n_workers):
            worker = workers[rank]
            for i, j in inp.layout.iterate(rank, inp.output_ids, output_ids, n_parts):
                t.placement[i][j].add(worker)


def _choose_workers(task, workers):
    result = []
    workers = workers.copy()
    for part_id in range(task.n_workers):
        worker = min(workers, key=lambda w: _transfer_costs(task, part_id, w))
        workers.remove(worker)
        result.append(worker)
    return result


class State:
    def __init__(self, workers):
        self.tasks = {}
        self.ready_tasks = []
        self.all_workers = workers
        self.free_workers = set(workers)
        self.need_sort = False

    def add_tasks(self, serialized_tasks):
        new_ready_tasks = False
        new_tasks = set()

        task_map = self.tasks
        n_workers = len(self.all_workers)

        for tdict in serialized_tasks:
            task_id = tdict["task_id"]
            if task_id in task_map:
                raise Exception("Task id ({}) already used".format(task_id))

        for tdict in serialized_tasks:
            task_id = tdict["task_id"]
            task = Task(
                task_id,
                tdict["n_outputs"],
                tdict["n_workers"],
                tdict["config"],
                tdict["keep"],
            )
            if task.n_workers > n_workers:
                message = "Task '{}' asked for {} workers, but server has only {} workers".format(
                    task_id, task.n_workers, n_workers
                )
                logger.error(message)
                raise Exception(message)
            logger.debug("Task %s submitted", task_id)
            task_map[task_id] = task
            new_tasks.add(task)
            tdict["_task"] = task

        for tdict in serialized_tasks:
            task = tdict["_task"]
            unfinished_deps = 0
            inputs = [TaskInput.from_dict(data, task_map) for data in tdict["inputs"]]
            deps = tuple(frozenset(inp.task for inp in inputs))
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

        self.need_sort |= new_ready_tasks
        compute_b_levels(task_map)
        return new_ready_tasks

    def _fake_placement(self, task, placement, sizes):
        task._fake_finish(placement, sizes)
        if task in self.ready_tasks:
            self.ready_tasks.remove(task)
        for t in task.consumers:
            t.unfinished_deps -= 1
            if t.unfinished_deps <= 0:
                assert t.unfinished_deps == 0
                logger.debug("Task %s is ready", t)
                self.ready_tasks.append(t)
                self.need_sort = True

    def schedule(self):
        if self.need_sort:
            self.ready_tasks.sort(key=_task_b_level)
        logger.debug("Scheduling ... top_3_tasks: %s", self.ready_tasks[:3])

        free_workers = self.free_workers
        for idx in range(len(self.ready_tasks) - 1, -1, -1):
            task = self.ready_tasks[idx]
            if task.n_workers <= len(free_workers):
                workers = _choose_workers(task, free_workers)
                for worker in workers:
                    free_workers.remove(worker)
                del self.ready_tasks[idx]
                yield task, workers
                if not free_workers:
                    break

        logger.debug("End of scheduling")

    def on_task_failed(self, task, workers, message):
        # TODO: Remove inputs that was downloaded for execution but they are not in placement
        logger.error("Task %s FAILED: %s", task, message)
        task.set_error(message)
        tasks_to_remove = []
        _release_task(task, tasks_to_remove)
        for t in task.recursive_consumers():
            if t.state == TaskState.UNFINISHED:
                t.set_error(message)
        self.free_workers.update(workers)
        return tasks_to_remove

    def on_task_finished(self, task, workers, sizes):
        logger.debug("Task %s finished", task)
        task.set_finished(workers, sizes)
        tasks_to_remove = []
        _release_task(task, tasks_to_remove)
        for t in task.consumers:
            t.unfinished_deps -= 1
            if t.unfinished_deps <= 0:
                assert t.unfinished_deps == 0
                logger.debug("Task %s is ready", t)
                self.ready_tasks.append(t)
                self.need_sort = True
        self.free_workers.update(workers)
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

    def dump(self):
        print("--- State ({} tasks) --- ".format(len(self.tasks)))
        for task_id, task in sorted(self.tasks.items()):
            task.dump()
        print("--- End of state ---")
        sys.stdout.flush()
