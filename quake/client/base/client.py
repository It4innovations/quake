import asyncio
import logging

import abrpc
import uvloop

from quake.client.base.task import TaskState

uvloop.install()
logger = logging.getLogger(__name__)


class Client:
    DEFAULT_ENV = {}

    def __init__(self, hostname="localhost", port=8600):
        self.connection = None
        self.loop = asyncio.get_event_loop()
        self.id_counter = 0
        self._connect(hostname, port)

    def _connect(self, hostname, port):
        async def connect():
            max_tries = 20
            i = 0
            while True:
                try:
                    connection = abrpc.Connection(
                        await asyncio.open_connection(hostname, port=port)
                    )
                    break
                except ConnectionError as e:
                    i += 1
                    logger.error(
                        "Could not connect to server (attempt [%s,%s])", i, max_tries
                    )
                    if i == max_tries:
                        raise e
                    await asyncio.sleep(1.0)
            asyncio.ensure_future(connection.serve())
            logger.info("Connection to server established")
            return connection

        logger.info("Connecting to server ...")
        self.connection = self.loop.run_until_complete(connect())

    def remove(self, task):
        logger.debug("Unkeeping id=%s", task.task_id)
        if not task.keep:
            raise Exception("'keep' flag is not set for task")
        if task.state == TaskState.NEW:
            pass  # Do nothing
        elif task.state == TaskState.SUBMITTED:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.connection.call("unkeep", task.task_id))
        else:
            raise Exception("Invalid task state")
        task.keep = False

    def _prepare_submit(self, tasks):
        for task in tasks:
            assert task.state == TaskState.NEW
            task.state = TaskState.SUBMITTED
            task.task_id = self.id_counter
            self.id_counter += 1
        return [task.to_dict() for task in tasks]

    def submit(self, tasks):
        logger.debug("Submitting %s tasks", len(tasks))
        serialized_tasks = self._prepare_submit(tasks)
        if serialized_tasks:
            self.loop.run_until_complete(
                self.connection.call("submit", serialized_tasks)
            )

    def wait(self, task):
        logger.debug("Waiting on task id=%s", task.task_id)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connection.call("wait", task.task_id))

    def wait_all(self, tasks):
        ids = [task.task_id for task in tasks]
        logger.debug("Waiting on tasks %s", ids)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connection.call("wait_all", ids))

    def gather(self, task, output_id=None, part_id=None):
        logger.debug("Gathering task id=%s", task.task_id)
        if not task.keep:
            raise Exception("'keep' flag is not set for a task")
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(
            self.connection.call("gather", task.task_id, output_id, part_id)
        )
