import asyncio
import logging
import os
from abrpc import expose, on_connection
from .server import Server

logger = logging.getLogger(__name__)


def get_worker_hostnames():
    if "QUAKE_WORKERS" not in os.environ:
        raise Exception("Set 'QUAKE_WORKERS' env variable")
    return os.environ.get("QUAKE_WORKERS").split(",")


def main():
    logging.basicConfig(level=0)
    worker_hostnames = get_worker_hostnames()
    server = Server(worker_hostnames)

    async def handle(conn):
        logger.info("New connection %s", conn)
        await conn.serve(server)
        logger.info("Connection %s closed", conn)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(server.connect_to_workers())

    loop.run_until_complete(
        asyncio.start_server(on_connection(handle), port=8600))
    loop.run_forever()


if __name__ == "__main__":
    main()