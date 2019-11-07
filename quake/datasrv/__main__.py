import argparse
import asyncio
import logging
import os

from abrpc import on_connection

from .service import Service

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("workdir")
    parser.add_argument("--port", type=int, default=8602)
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.DEBUG)
    args = parse_args()
    service = Service(args.workdir)
    logger.info("Starting data service on port %s", args.port)

    logger.info("Working directory is: %s", args.workdir)
    os.makedirs(args.workdir)

    if os.listdir(args.workdir):
        raise Exception("Working directory '{}' is not empty".format(args.workdir))

    async def handle(conn):
        logger.info("New connection %s", conn)
        await conn.serve(service)
        logger.info("Connection %s closed", conn)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.start_server(on_connection(handle), port=args.port))
    loop.run_forever()


if __name__ == "__main__":
    main()
