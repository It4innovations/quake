
import logging
import asyncio
import os
import threading


from quake.task import TaskState

logger = logging.getLogger(__file__)


class Process:

    def __init__(self, task, workers):
        self.task = task
        self.workers = workers


def server_thread_main(server):
    try:
        asyncio.set_event_loop(server.loop)
        server.loop.run_until_complete(server.stop_event.wait())
        logging.debug("Server stopped")
    finally:
        server.loop.close()


class Client:

    def __init__(self, server):
        self.server = server

    def submit(self, tasks):
        self.server.loop.call_soon_threadsafe(self.server.submit, tasks)

    def wait_for_task(self, task):
        raise NotImplemented()


class Server:

    def __init__(self, workers, workdir="/tmp", run_prefix=(), run_cwd=None):
        logger.debug("Starting QUake launcher")
        logger.debug("Workdir: %s", workdir)
        for i, worker in enumerate(workers):
            assert worker.worker_id is None
            worker.worker_id = i
            logger.debug("Registering worker worker_id=%s host=%s", i, worker.hostname)
        self.workdir = os.path.abspath(workdir)
        self.id_counter = 0
        self.tasks = {}
        self.ready_tasks = []
        self.all_workers = workers
        self.free_workers = list(workers)
        self.processes = {}
        self.run_prefix = tuple(run_prefix)
        self.run_cwd = run_cwd
        self.stop_event = None
        self.loop = None

    def start(self):
        assert self.loop is None
        self.loop = asyncio.new_event_loop()
        self.stop_event = asyncio.Event(loop=self.loop)
        thread = threading.Thread(target=server_thread_main, args=(self,), daemon=True)
        thread.start()

    def stop(self):
        logging.debug("Stopping server")
        async def _helper():
            self.stop_event.set()
        future = asyncio.run_coroutine_threadsafe(_helper(), self.loop)
        future.result()

    def new_id(self):
        self.id_counter += 1
        return self.id_counter

    def submit(self, tasks):
        new_ready_tasks = False
        new_tasks = set(tasks)
        for task in tasks:
            assert task.task_id is None
            task_id = self.new_id()
            task.task_id = task_id
            logger.debug("Task %s submitted", task_id)
            self.tasks[task_id] = task

            unfinished_deps = 0
            for t in task.deps:
                assert t.state != TaskState.RELEASED
                assert t.keep or t in new_tasks, "Dependency on not-keep task"
                t.consumers.add(task)
                if not t.is_ready:
                    unfinished_deps += 1
            task.unfinished_deps = unfinished_deps
            if not unfinished_deps:
                new_ready_tasks = True
                logger.debug("Task %s is ready", task_id)
                self.ready_tasks.append(task)
        if new_ready_tasks:
            self.schedule()

    def wait_for_task(self, task):
        self.processes

    def schedule(self):
        for task in self.ready_tasks:
            if task.n_workers <= len(self.free_workers):
                workers = self.free_workers[:task.n_workers]
                del self.free_workers[:task.n_workers]
                self._start_task(task, workers)

    def _start_task(self, task, workers):
        logging.debug("Starting task %s on %s", task, workers)
        assert task.state == TaskState.UNFINISHED and task.is_ready()
        assert task not in self.processes

        hostnames = ",".join(worker.hostname for worker in workers)
        command = self.run_prefix
        command += ("mpirun", "--host", hostnames, "np", str(task.n_workers), "--bynode")
        command += task.args
        self._exec(task, command)

    async def _exec(self, task, args):
        await asyncio.create_subprocess_exec(args[0], args[1:], cwd=self.run_cwd)
        logging.debug("Task %s finished", task)