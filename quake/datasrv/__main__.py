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


def run_data_service(workdir, port):
    service = Service(workdir)
    logger.info("Starting data service on port %s", port)

    logger.info("Working directory is: %s", workdir)
    os.makedirs(workdir, exist_ok=True)

    if os.listdir(workdir):
        raise Exception("Working directory '{}' is not empty".format(workdir))

    async def handle(conn):
        logger.info("New connection %s", conn)
        await conn.serve(service)
        logger.info("Connection %s closed", conn)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.start_server(on_connection(handle), host="0.0.0.0", port=port)
    )
    loop.run_forever()


def main():
    logging.basicConfig(level=logging.DEBUG)
    args = parse_args()
    run_data_service(args.workdir, args.port)


if __name__ == "__main__":
    main()
