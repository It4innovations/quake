import argparse
import asyncio
import logging

import uvloop

from .job import Job

uvloop.install()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("task_id", type=int)
    parser.add_argument("rank", type=int)
    parser.add_argument("ds_local_port", type=int)
    return parser.parse_args()


"""
def read_env():
    data_placements = {}
    for line in os.environ["QUAKE_DATA_PLACEMENT"].split("\n"):
        if line == "":
            continue
        name, hostname, port = line.split(",")
        data_placements[name] = (hostname, int(port))
    ds_local_port = int(os.environ["QUAKE_LOCAL_DS_PORT"])
    task_id = int(os.environ["QUAKE_TASK_ID"])
    return data_placements, ds_local_port, task_id
"""


async def main():
    args = parse_args()
    job = Job(args.task_id, args.rank, args.ds_local_port, {})
    await job.start()


if __name__ == "__main__":
    logging.basicConfig(level=0)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
