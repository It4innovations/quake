import asyncio
import json
import logging
import os
import random
import tempfile

import abrpc
import uvloop

from .state import State
from .task import TaskState
from aiofile import AIOFile, Writer

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
    state = task.state
    if task.state == TaskState.UNFINISHED:
        event = asyncio.Event()
        task.add_event(event)
        await event.wait()
        state = task.state
    if state == TaskState.FINISHED or state == TaskState.RELEASED:
        return
    elif task.state == TaskState.ERROR:
        raise Exception(task.error)
    else:
        assert 0


async def _remove_task(task):
    fs = []

    # The ordering of two following calls is important!
    placement = task.placement
    task.set_released()

    for output_id in range(task.n_outputs):
        for part in range(task.n_workers):
            fs.append(
                _remove_from_workers(
                    placement[output_id][part], task.make_data_name(output_id, part)
                )
            )

    await asyncio.gather(*fs)
    logger.debug("All parts of task %s was removed (%s calls)", task, len(fs))


async def _upload_on_workers(workers, name, data):
    fs = [w.ds_connection.call("upload", name, data) for w in workers]
    await asyncio.gather(*fs)


async def _remove_from_workers(workers, name):
    fs = [w.ds_connection.call("remove", name) for w in workers]
    await asyncio.wait(fs)


async def _download_sizes(task, workers):
    fs = [
        w.ds_connection.call(
            "get_sizes",
            [
                task.make_data_name(output_id, part_id)
                for output_id in range(task.n_outputs)
            ],
        )
        for part_id, w in enumerate(workers)
    ]
    sizes = await asyncio.gather(*fs)
    return [
        [sizes[part_id][output_id] for part_id in range(task.n_workers)]
        for output_id in range(task.n_outputs)
    ]


async def _fetch_stats(worker):
    logger.debug("Fetching stats from worker %s", worker.hostname)
    data = await worker.ds_connection.call("get_stats")
    data["hostname"] = worker.hostname
    return data


class Server:
    def __init__(self, worker_hostnames, local_ds_port, monitor_filename):
        logger.debug("Starting QUake server")

        workers = []
        for i, hostname in enumerate(worker_hostnames):
            worker = Worker(i, hostname)
            logger.info("Registering worker worker_id=%s host=%s", i, worker.hostname)
            workers.append(worker)

        self.state = State(workers)
        self.processes = {}
        # self.run_prefix = tuple(run_prefix)
        # self.run_cwd = run_cwd

        self.local_ds_connection = None
        self.ds_port = local_ds_port

        self.monitor_filename = monitor_filename

    @staticmethod
    async def _gather_output(task, output_id):
        workers = [random.choice(tuple(ws)) for ws in task.placement[output_id]]
        assert len(workers) == task.n_workers
        fs = [
            w.ds_connection.call("get_data", task.make_data_name(output_id, i))
            for i, w in enumerate(workers)
        ]
        return await asyncio.gather(*fs)

    @abrpc.expose()
    async def gather(self, task_id, output_id):
        task = self.state.tasks.get(task_id)
        if task is None:
            raise Exception("Task '{}' not found".format(task_id))
        await _wait_for_task(task)
        if output_id is None:
            fs = [
                self._gather_output(task, output_id)
                for output_id in range(task.n_outputs)
            ]
            return await asyncio.gather(*fs)
        else:
            return await self._gather_output(task, output_id)

    @abrpc.expose()
    async def wait(self, task_id):
        task = self.state.tasks.get(task_id)
        if task is None:
            raise Exception("Task '{}' not found".format(task_id))
        await _wait_for_task(task)

    @abrpc.expose()
    async def wait_all(self, task_ids):
        fs = []
        for task_id in task_ids:
            task = self.state.tasks.get(task_id)
            if task is None:
                raise Exception("Task '{}' not found".format(task_id))
            fs.append(_wait_for_task(task))
        await asyncio.wait(fs)

    @abrpc.expose()
    async def unkeep(self, task_id):
        tasks_to_remove = self.state.unkeep(task_id)
        if tasks_to_remove:
            for task in tasks_to_remove:
                asyncio.ensure_future(_remove_task(task))

    @abrpc.expose()
    async def submit(self, tasks):
        if self.state.add_tasks(tasks):
            self.schedule()

    def schedule(self):
        for task, workers in self.state.schedule():
            self._start_task(task, workers)

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

        # command = () #  self.run_prefix
        # command += ("mpirun", "--host", hostnames, "--np", str(task.n_workers), "--map-by", "node")
        # command += task.args

        elif task_type == "mpirun":
            args = ["mpirun"]
            args.append("--tag-output")
            config_args = task.config["args"]
            for rank, worker in enumerate(workers):
                if rank != 0:
                    args.append(":")
                args.append("-np")
                args.append("1")
                args.append("--host")
                args.append(worker.hostname)
                if "env" in task.config:
                    for name, value in task.config["env"].items():
                        if value is None:
                            value = os.environ.get(name)
                            if value is None:
                                continue
                        args.append("-x")
                        args.append("{}={}".format(name, value))
                for arg in config_args:
                    if arg == "$RANK":
                        args.append(str(rank))
                    elif arg == "$TASK_ID":
                        args.append(str(task.task_id))
                    elif arg == "$DS_PORT":
                        args.append(str(self.ds_port))
                    else:
                        args.append(arg)
            asyncio.ensure_future(self._exec(task, args, workers))
        else:
            raise Exception("Invalid task type")

    def _task_failed(self, task, workers, message):
        for task in self.state.on_task_failed(task, workers, message):
            asyncio.ensure_future(_remove_task(task))
        self.schedule()

    def _task_finished(self, task, workers, sizes):
        for task in self.state.on_task_finished(task, workers, sizes):
            asyncio.ensure_future(_remove_task(task))
        self.schedule()

    async def _upload(self, task, workers):
        parts = task.config["data"]
        try:
            fs = [
                workers[i].ds_connection.call("upload", task.make_data_name(0, i), data)
                for i, data in enumerate(parts)
            ]
            await asyncio.wait(fs)
            self._task_finished(task, workers, [[len(data) for data in parts]])
        except Exception as e:
            logger.error(e)
            self._task_failed(task, workers, "Upload failed: " + str(e))

    def _create_placement_data(self, task):
        placements = {}
        for inp in task.inputs:
            for output_id in inp.output_ids:
                p = inp.task.placement[output_id]
                for i in range(inp.task.n_workers):
                    name = inp.task.make_data_name(output_id, i)
                    placements[name] = [(w.hostname, self.ds_port) for w in p[i]]
        inputs = [
            {
                "task_id": inp.task.task_id,
                "output_ids": inp.output_ids,
                "n_parts": inp.task.n_workers,
                "layout": inp.layout.serialize(),
            }
            for inp in task.inputs
        ]

        return {"placements": placements, "inputs": inputs}

    async def _exec(self, task, args, workers):
        data_key = None
        placement_key = None
        try:
            task_data = task.config.get("data")
            upload_fs = []
            if task_data is not None:
                data_key = "taskdata_{}".format(task.task_id)
                upload_fs.append(_upload_on_workers(workers, data_key, task_data))
            placement_key = "placement_{}".format(task.task_id)
            upload_fs.append(
                _upload_on_workers(
                    workers,
                    placement_key,
                    json.dumps(self._create_placement_data(task)).encode(),
                )
            )
            await asyncio.wait(upload_fs)

            with tempfile.TemporaryFile() as stdout_file:
                with tempfile.TemporaryFile() as stderr_file:
                    logger.debug("Starting %s: %s", task, args)
                    process = await asyncio.create_subprocess_exec(
                        *args,
                        stderr=stderr_file,
                        stdout=stdout_file,
                        stdin=asyncio.subprocess.DEVNULL
                    )
                    exitcode = await process.wait()
                    if exitcode != 0:
                        stderr_file.seek(0)
                        stderr = stderr_file.read().decode()
                        stdout_file.seek(0)
                        stdout = stdout_file.read().decode()
                        message = "Task id={} failed. Exit code: {}\nStdout:\n{}\nStderr:\n{}\n".format(
                            task.task_id, exitcode, stdout, stderr
                        )
                        self._task_failed(task, workers, message)
                    else:
                        # DEBUG
                        stderr_file.seek(0)
                        stderr = stderr_file.read().decode()
                        stdout_file.seek(0)
                        stdout = stdout_file.read().decode()
                        logger.info(
                            "Task id={} finished.\nStdout:\n{}\nStderr:\n{}\n".format(
                                task.task_id, stdout, stderr
                            )
                        )
                        sizes = await _download_sizes(task, workers)
                        logger.debug(
                            "Sizes of task=%s downloaded sizes=%s", task.task_id, sizes
                        )
                        self._task_finished(task, workers, sizes)
        finally:
            if data_key:
                await _remove_from_workers(workers, data_key)
            await _remove_from_workers(workers, placement_key)

    async def connect_to_ds(self):
        async def connect(hostname, port):
            connection = abrpc.Connection(
                await asyncio.open_connection(hostname, port=port)
            )
            asyncio.ensure_future(connection.serve())
            return connection

        fs = [connect(w.hostname, self.ds_port) for w in self.state.all_workers]
        connections = await asyncio.gather(*fs)

        if self.monitor_filename:
            asyncio.ensure_future(self.collect_stats())

        for w, c in zip(self.state.all_workers, connections):
            w.ds_connection = c
        self.local_ds_connection = connections[0]

    async def collect_stats(self):
        logger.info("Monitoring streamed into: %s", self.monitor_filename)
        async with AIOFile(self.monitor_filename, "w") as monitor_file:
            writer = Writer(monitor_file)
            while True:
                await asyncio.sleep(1)
                results = []
                try:
                    for data in asyncio.as_completed(
                        [_fetch_stats(w) for w in self.state.all_workers], timeout=1
                    ):
                        results.append(json.dumps(await data))
                except asyncio.TimeoutError:
                    logger.error("Fetching stats timeouted")
                if results:
                    await writer("\n".join(results) + "\n")
                    await monitor_file.fsync()
