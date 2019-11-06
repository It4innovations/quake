import asyncio
import logging
import tempfile

import abrpc
import uvloop
import os

from .task import TaskState, Task
from ..common.taskinput import TaskInput
import random

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


async def _wait_for_task(task):
    if not task.keep:
        raise Exception("Waiting on non-keep tasks are not allowed (task={})".format(task))
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


async def _remove_task(task):
    fs = []
    for output_id in range(task.n_outputs):
        for part in range(task.n_workers):
            fs.append(_remove_from_workers(task.placement[output_id][part], task.make_data_name(output_id, part)))
    await asyncio.gather(*fs)
    logger.debug("All parts of task % was removed (%s calls)", task, len(fs))


async def _upload_on_workers(workers, name, data):
    fs = [w.ds_connection.call("upload", name, data) for w in workers]
    await asyncio.gather(*fs)


async def _remove_from_workers(workers, name):
    fs = [w.ds_connection.call("remove", name) for w in workers]
    await asyncio.wait(fs)


def _check_removal(task):
    if task.keep or task.consumers:
        return
    assert task.state == TaskState.FINISHED

    logger.debug("Removing task %s", task)
    asyncio.ensure_future(_remove_task(task))


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

        self.local_ds_connection = None
        self.ds_port = local_ds_port

    @abrpc.expose()
    async def gather(self, task_id, output_id):
        task = self.tasks.get(task_id)
        if task is None:
            raise Exception("Task '{}' not found".format(task_id))
        await _wait_for_task(task)
        workers = [random.choice(tuple(ws)) for ws in task.placement[output_id]]
        assert len(workers) == task.n_workers
        fs = [w.ds_connection.call("get_data", task.make_data_name(output_id, i)) for i, w in enumerate(workers)]
        return await asyncio.gather(*fs)

    @abrpc.expose()
    async def wait(self, task_id):
        task = self.tasks.get(task_id)
        if task is None:
            raise Exception("Task '{}' not found".format(task_id))
        await _wait_for_task(task)

    @abrpc.expose()
    async def unkeep(self, task_id):
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
            _check_removal(task)
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
            task = Task(task_id, tdict["n_outputs"], tdict["n_workers"], tdict["config"], tdict["keep"])
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

        task_type = task.config.get("type")
        if task_type == "upload":  # UPLOAD TASK
            logger.debug("Executing upload task %s to workers %s", task, workers)
            asyncio.ensure_future(self._upload(task, workers))
            logger.debug("Upload of task %s finished", task)
            return

        #command = () #  self.run_prefix
        #command += ("mpirun", "--host", hostnames, "--np", str(task.n_workers), "--map-by", "node")
        #command += task.args

        elif task_type == "mpirun":
            args = ["mpirun"]
            config_args = task.config["args"]
            for rank, worker in enumerate(workers):
                if rank != 0:
                    args.append(":")
                args.append("-np")
                args.append("1")
                args.append("--host")
                args.append(worker.hostname)
                for arg in config_args:
                    if arg == "$RANK":
                        args.append(str(rank))
                    else:
                        args.append(arg)
            asyncio.ensure_future(self._exec(task, args, workers))
        else:
            raise Exception("Invalid task type")

    def _task_failed(self, task, workers, message):
        logger.error("Task %s FAILED: %s", task, message)
        task.set_error(message)
        for t in task.inputs:
            t.consumers.remove(self)
        _check_removal(task)
        for t in task.recursive_consumers():
            if t.state == TaskState.UNFINISHED:
                t.set_error(message)
        self.free_workers.extend(workers)
        self.schedule()

    def _task_finished(self, task, workers):
        logger.debug("Task %s finished", task)
        task.set_finished(workers)
        new_ready_tasks = False
        for t in task.inputs:
            t.consumers.remove(self)
        _check_removal(task)
        for t in task.consumers:
            t.unfinished_deps -= 1
            if t.unfinished_deps <= 0:
                assert t.unfinished_deps == 0
                logger.debug("Task %s is ready", t)
                self.ready_tasks.append(t)
                new_ready_tasks = True
        self.free_workers.extend(workers)
        if new_ready_tasks:
            self.schedule()

    async def _upload(self, task, workers):
        parts = task.config["data"]
        try:
            fs = [workers[i].ds_connection.call("upload", task.make_data_name(0, i), data)
                  for i, data in enumerate(parts)]
            await asyncio.wait(fs)
            self._task_finished(task, workers)
        except Exception as e:
            logger.error(e)
            self._task_failed(task, workers, "Upload failed: " + str(e))

    async def _exec(self, task, args, workers):
        env = os.environ.copy()
        env["QUAKE_TASK_ID"] = str(task.task_id)
        env["QUAKE_DATA_PLACEMENT"] = ""
        env["QUAKE_LOCAL_DS_PORT"] = str(self.ds_port)

        task_data = task.config.get("data")
        if task_data is not None:
            data_key = "taskdata_{}".format(task.task_id)
            await _upload_on_workers(workers, data_key, task_data)
        else:
            data_key = None

        try:
            with tempfile.TemporaryFile() as stdout_file:
                with tempfile.TemporaryFile() as stderr_file:
                    logger.debug("Starting %s: %s", task, args)
                    process = await asyncio.create_subprocess_exec(
                        *args, stderr=stderr_file, stdout=stdout_file, stdin=asyncio.subprocess.DEVNULL, env=env)
                    exitcode = await process.wait()
                    if exitcode != 0:
                        stderr_file.seek(0)
                        stderr = stderr_file.read().decode()
                        stdout_file.seek(0)
                        stdout = stdout_file.read().decode()
                        message = "Task id={} failed. Exit code: {}\nStdout:\n{}\nStderr:\n{}\n".format(
                            task.task_id, exitcode, stdout, stderr)
                        self._task_failed(task, workers, message)
                    else:
                        self._task_finished(task, workers)
        finally:
            if data_key:
                await _remove_from_workers(workers, data_key)

    async def connect_to_ds(self):
        async def connect(hostname, port):
            connection = abrpc.Connection(await asyncio.open_connection(hostname, port=port))
            asyncio.ensure_future(connection.serve())
            return connection

        fs = [connect(w.hostname, self.ds_port) for w in self.all_workers]
        connections = await asyncio.gather(*fs)

        for w, c in zip(self.all_workers, connections):
            w.ds_connection = c
        self.local_ds_connection = connections[0]
