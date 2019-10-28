
import logging
import asyncio
import uvloop
import abrpc

from .task import Task, TaskState

uvloop.install()
logger = logging.getLogger(__name__)


class Client:

    PY_JOB_ARGS = ("python3", "-m", "quake.job", "$RANK")

    def __init__(self, hostname="localhost", port=8600):
        self.connection = None
        self.unsubmitted_tasks = []
        self.loop = asyncio.get_event_loop()
        self._connect(hostname, port)
        self.id_counter = 0

    def _connect(self, hostname, port):
        async def connect():
            connection = abrpc.Connection(await asyncio.open_connection(hostname, port=port))
            asyncio.ensure_future(connection.serve())
            logger.info("Connection to server established")
            return connection

        logger.info("Connecting to server ...")
        self.connection = self.loop.run_until_complete(connect())

    def new_task(self, n_outputs, n_workers, args, keep=False, config=None, inputs=()):
        task = Task(self.id_counter, n_outputs, n_workers, args, keep, config, inputs)
        self.id_counter += 1
        self.unsubmitted_tasks.append(task)
        return task

    def new_py_task(self, n_outputs, n_workers, keep=False, config=None, inputs=()):
        return self.new_task(n_outputs, n_workers, self.PY_JOB_ARGS, keep, config, inputs)

    def upload_data(self, data, keep=False):
        assert isinstance(data, list)
        return self.new_task(1, len(data), ["UPLOAD"] + data, keep, None, ())

    def submit(self):
        logger.debug("Submitting %s tasks", len(self.unsubmitted_tasks))
        if not self.unsubmitted_tasks:
            return
        for task in self.unsubmitted_tasks:
            assert task.state == TaskState.NEW
            task.state = TaskState.SUBMITTED
        tasks = [task.to_dict() for task in self.unsubmitted_tasks]
        self.unsubmitted_tasks = []
        self.loop.run_until_complete(self.connection.call("submit", tasks))

    def wait(self, task):
        logger.debug("Waiting on task id=%s", task.task_id)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connection.call("wait", task.task_id))

    def gather(self, task, output_id):
        logger.debug("Gathering task id=%s", task.task_id)
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.connection.call("gather", task.task_id, output_id))