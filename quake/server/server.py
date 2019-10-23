import asyncio
import logging
import os
import tempfile
import threading

import uvloop

from quake.common.task import TaskState

# !!!!!!!!!!!!!!!
uvloop.install()
# !!!!!!!!!!!!!!!

logger = logging.getLogger(__file__)


class Process:

    def __init__(self, task, workers):
        self.task = task
        self.workers = workers


"""
def server_thread_main(server):
    try:
        asyncio.set_event_loop(server.loop)
        server.loop.run_until_complete(server.stop_event.wait())
        logger.debug("Server stopped")
    finally:
        server.loop.close()
"""
from .worker import Worker


class Server:

    def __init__(self, worker_hostnames, run_prefix=(), run_cwd=None):
        logger.debug("Starting QUake server")

        workers = []
        for i, hostname in enumerate(worker_hostnames):
            worker = Worker(hostname)
            worker.worker_id = i
            logger.info("Registering worker worker_id=%s host=%s", i, worker.hostname)
            workers.append(worker)

        self.id_counter = 0
        self.tasks = {}
        self.ready_tasks = []
        self.all_workers = workers
        self.free_workers = list(workers)
        self.processes = {}
        self.run_prefix = tuple(run_prefix)
        self.run_cwd = run_cwd

    """
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
    """

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
                if not t.is_ready():
                    unfinished_deps += 1
            task.unfinished_deps = unfinished_deps
            if not unfinished_deps:
                new_ready_tasks = True
                logger.debug("Task %s is ready", task_id)
                self.ready_tasks.append(task)
        if new_ready_tasks:
            self.schedule()

    def schedule(self):
        logger.debug("Scheduling ...")
        for task in self.ready_tasks:
            if task.n_workers <= len(self.free_workers):
                workers = self.free_workers[:task.n_workers]
                del self.free_workers[:task.n_workers]
                self._start_task(task, workers)

    def _start_task(self, task, workers):
        logger.debug("Starting task %s on %s", task, workers)
        assert task.state == TaskState.UNFINISHED and task.is_ready()
        assert task not in self.processes

        hostnames = ",".join(worker.hostname for worker in workers)
        command = self.run_prefix
        command += ("mpirun", "--host", hostnames, "--np", str(task.n_workers), "--map-by", "node")
        command += task.args
        asyncio.ensure_future(self._exec(task, command))

    async def _exec(self, task, args):
        print("ARGS", args)
        with tempfile.TemporaryFile() as stdout_file:
            with tempfile.TemporaryFile() as stderr_file:
                process = await asyncio.create_subprocess_exec(
                    *args, cwd=self.run_cwd, loop=self.loop, stderr=stderr_file, stdout=stdout_file, stdin=asyncio.subprocess.DEVNULL)
                exitcode = await process.wait()
                if exitcode != 0:
                    logger.debug("Task %s FAILED", task)
                    stderr_file.seek(0)
                    stderr = stderr_file.read().decode()
                    stdout_file.seek(0)
                    stdout = stdout_file.read().decode()
                    message = "Task id={} failed. Exit code: {}\nStdout:\n{}\nStderr:\n{}\n".format(
                        task.task_id, exitcode, stdout, stderr)
                    task.set_error(message)
                    for t in task.recursive_consumers():
                        if t.state == TaskState.UNFINISHED:
                            t.set_error(message)
                else:
                    logger.debug("Task %s finished", task)
                    task.state = TaskState.FINISHED
                    task.fire_events()

    async def connect_to_workers(self):
        fs = [asyncio.open_connection(w.hostname, port=8500) for w in self.all_workers]
        connections = await asyncio.gather(*fs)
        print(connections)
