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


async def main():
    args = parse_args()
    job = Job(args.task_id, args.rank, args.ds_local_port, {})
    await job.start()


if __name__ == "__main__":
    logging.basicConfig(level=0)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
