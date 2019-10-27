import asyncio
import logging
import tempfile

import abrpc
import uvloop

from .task import TaskState, Task
from ..common.taskinput import TaskInput

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

    def __init__(self, worker_hostnames, local_ds_port):
        logger.debug("Starting QUake server")

        workers = []
        for i, hostname in enumerate(worker_hostnames):
            worker = Worker(hostname)
            worker.worker_id = i
            logger.info("Registering worker worker_id=%s host=%s", i, worker.hostname)
            workers.append(worker)

        # self.id_counter = 0

        self.tasks = {}
        self.ready_tasks = []
        self.all_workers = workers
        self.free_workers = list(workers)

        self.processes = {}
        #self.run_prefix = tuple(run_prefix)
        #self.run_cwd = run_cwd

        self.ds_connections = {}
        self.local_ds_connection = None
        self.ds_port = local_ds_port

    @abrpc.expose()
    async def wait(self, task_id):
        task = self.tasks.get(task_id)
        if task is None:
            raise Exception("Task '{}' not found".format(task_id))
        state = task.state

        if task.state == TaskState.UNFINISHED:
            event = asyncio.Event()
            task.add_event(event)
            await event.wait()
            state = task.state

        if state == TaskState.FINISHED:
            return
        elif task.state == TaskState.ERROR:
            raise Exception(task.error)
        else:
            assert 0

    @abrpc.expose()
    async def submit(self, tasks):
        new_ready_tasks = False
        new_tasks = set()

        task_map = self.tasks
        for tdict in tasks:
            task_id = tdict["task_id"]
            if task_id in task_map:
                raise Exception("Task id ({}) already used".format(task_id))

        for tdict in tasks:
            task_id = tdict["task_id"]
            task = Task(task_id, tdict["n_outputs"], tdict["n_workers"], tdict["args"], tdict["keep"])
            logger.debug("Task %s submitted", task_id)
            task_map[task_id] = task
            new_tasks.add(task)
            tdict["_task"] = task

        for tdict in tasks:
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

        if new_ready_tasks:
            self.schedule()

    def schedule(self):
        logger.debug("Scheduling ... top_3_tasks: %s", self.ready_tasks[:3])
        for task in self.ready_tasks[:]:
            if task.n_workers <= len(self.free_workers):
                workers = self.free_workers[:task.n_workers]
                del self.free_workers[:task.n_workers]
                self.ready_tasks.remove(task)
                self._start_task(task, workers)
        logger.debug("End of scheduling")

    def _start_task(self, task, workers):
        logger.debug("Starting task %s on %s", task, workers)
        assert task.state == TaskState.UNFINISHED and task.is_ready()
        assert task not in self.processes

        hostnames = ",".join(worker.hostname for worker in workers)
        command = () #  self.run_prefix
        command += ("mpirun", "--host", hostnames, "--np", str(task.n_workers), "--map-by", "node")
        command += task.args
        asyncio.ensure_future(self._exec(task, command, workers))

    async def _exec(self, task, args, workers):
        with tempfile.TemporaryFile() as stdout_file:
            with tempfile.TemporaryFile() as stderr_file:
                process = await asyncio.create_subprocess_exec(
                    *args, stderr=stderr_file, stdout=stdout_file, stdin=asyncio.subprocess.DEVNULL)
                exitcode = await process.wait()
                new_ready_tasks = False
                self.free_workers.extend(workers)
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
                    for t in task.consumers:
                        t.unfinished_deps -= 1
                        if t.unfinished_deps <= 0:
                            assert t.unfinished_deps == 0
                            logger.debug("Task %s is ready", t)
                            self.ready_tasks.append(t)
                            new_ready_tasks = True
                    if new_ready_tasks:
                        self.schedule()

    async def connect_to_ds(self):
        async def connect(hostname, port):
            connection = abrpc.Connection(await asyncio.open_connection(hostname, port=port))
            asyncio.ensure_future(connection.serve())
            return connection

        fs = [connect(w.hostname, self.ds_port) for w in self.all_workers]
        connections = await asyncio.gather(*fs)
        self.ds_connections = dict(zip(self.all_workers, connections))
        self.local_ds_connection = connections[0]