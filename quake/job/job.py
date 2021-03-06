import asyncio
import json
import logging

# import cloudpickle
import pickle
import random
from datetime import datetime

import abrpc

from quake.common.layout import Layout
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
        self.ds_connection = abrpc.Connection(
            await asyncio.open_connection("localhost", self.ds_local_port)
        )
        self.ds_connection.set_nr_error_handle(
            lambda name, args: logger.error("Call '{}' failed".format(name))
        )
        self.ds_task = asyncio.ensure_future(self.ds_connection.serve())
        logger.info("Connection to data service established")

    # async def disconnect_ds(self):
    #    await self.connection.close()
    #    self.ds_task.cancel()
    #    self.ds_connection = None
    #    self.ds_task = None

    async def download_object(self, name: str):
        v = self.data_placements.get(name)
        if v is None:
            logger.info("Downloading %s", name)
            return await self.ds_connection.call("get_data", name)
        else:
            hostname, port = random.choice(v)
            logger.info("Downloading %s [remote %s:%s]", name, hostname, port)
            return await self.ds_connection.call("get_data", name, hostname, port)

    async def download_config(self):
        data = await self.download_object("taskdata_{}".format(self.task_id))
        return pickle.loads(data)

    async def download_placement_dict(self):
        data = await self.download_object("placement_{}".format(self.task_id))
        return json.loads(data.decode())

    async def download_input(self, task_id, pairs):
        return await asyncio.gather(
            *[
                self.download_object(make_data_name(task_id, output_id, part))
                for output_id, part in pairs
            ]
        )

    async def upload_data(self, output_id, data):
        name = make_data_name(self.task_id, output_id, self.rank)
        await self.ds_connection.call("upload", name, data)

    async def send_event(self, event):
        event["timestamp"] = datetime.now().isoformat()
        await self.ds_connection.call_no_response("add_event", event)

    async def start(self):
        rank = self.rank
        task_id = self.task_id
        logger.info("Starting task id=%s on rank=%s", task_id, rank)

        await self.connect_to_ds()
        await self.send_event({"type": "init", "task": task_id, "rank": rank})
        config = await self.download_config()
        pd = await self.download_placement_dict()
        self.data_placements = pd["placements"]
        inputs = pd["inputs"]

        fs = []
        for inp_dict in inputs:
            layout = Layout.deserialize(inp_dict["layout"])
            fs.append(
                self.download_input(
                    inp_dict["task_id"],
                    layout.iterate(rank, inp_dict["output_ids"], inp_dict["n_parts"]),
                )
            )

        await self.send_event({"type": "start", "task": task_id, "rank": rank})

        input_data = await asyncio.gather(*fs)
        jctx = JobContext(rank, input_data)
        output = config.fn(jctx, input_data, config.payload)
        if len(output) != config.n_outputs:
            raise Exception(
                "Task produced output of size {} but {} was expected".format(
                    len(output), config.n_outputs
                )
            )

        for i, data in enumerate(output):
            await self.upload_data(i, data)

        await self.send_event({"type": "end", "task": task_id, "rank": rank})
