
import logging
import abrpc
import asyncio
#import cloudpickle
import pickle

import uvloop
from quake.common.utils import make_data_name

logger = logging.getLogger(__name__)


class JobContext:

    def __init__(self, rank, inputs):
        self.rank = rank
        self.inputs = inputs


class Job:

    def __init__(self, task_id, rank, ds_local_port, data_placements):
        logger.info("Starting task=%s, rank=%s", task_id, rank)
        self.task_id = task_id
        self.rank = rank
        self.ds_local_port = ds_local_port
        self.ds_connection = None
        self.ds_task = None
        self.data_placements = data_placements


    async def connect_to_ds(self):
        logger.info("Connecting to data service on port %s", self.ds_local_port)
        self.ds_connection = abrpc.Connection(await asyncio.open_connection("localhost", self.ds_local_port))
        self.ds_task = asyncio.ensure_future(self.ds_connection.serve())
        logger.info("Connection to data service established")

    #async def disconnect_ds(self):
    #    await self.connection.close()
    #    self.ds_task.cancel()
    #    self.ds_connection = None
    #    self.ds_task = None

    async def download_object(self, name):
        logger.info("Downloading %s", name)
        v = self.data_placements.get(name)
        if v is None:
            return await self.ds_connection.call("get_data", name)
        else:
            hostname, port = v
            return await self.ds_connection.call("get_data", name, hostname, port)

    async def download_config(self):
        data = await self.download_object("taskdata_{}".format(self.task_id))
        return pickle.loads(data)

    async def download_input(self, task_id, output_id, parts):
        return await asyncio.gather(
            *[await self.download_object(make_data_name(task_id, output_id, part))
              for part in parts])

    async def upload_data(self, output_id, data):
        name = make_data_name(self.task_id, output_id, self.rank)
        await self.ds_connection.call("upload", name, data)

    async def start(self):
        logger.info("Starting task id=%s", self.task_id)

        await self.connect_to_ds()
        config = await self.download_config()

        fs = []
        for job_inp in config.inputs:
            parts = range(job_inp.n_outputs)
            fs.append(self.download_input(job_inp.task_id, job_inp.output_id, parts))

        input_data = await asyncio.gather(*fs)
        jctx = JobContext(self.rank, input_data)
        output = config.fn(jctx)
        assert len(output) == config.n_outputs

        for i, data in enumerate(output):
            await self.upload_data(i, data)