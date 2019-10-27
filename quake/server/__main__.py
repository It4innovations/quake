import argparse
import asyncio
import logging
import os
from abrpc import expose, on_connection
from .server import Server

logger = logging.getLogger(__name__)


#def get_worker_hostnames():
#    if "QUAKE_WORKERS" not in os.environ:
#        raise Exception("Set 'QUAKE_WORKERS' env variable")
#    return os.environ.get("QUAKE_WORKERS").split(",")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8600)
    parser.add_argument("--ds-port", type=int, default=8602)
    parser.add_argument("--workers", type=str, default="localhost")
    return parser.parse_args()


def main():
    logging.basicConfig(level=0)
    args = parse_args()
    server = Server(args.workers.split(","), args.ds_port)

    async def handle(conn):
        logger.info("New client connection %s", conn)
        await conn.serve(server)
        logger.info("Client connection %s closed", conn)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(server.connect_to_ds())

    loop.run_until_complete(
        asyncio.start_server(on_connection(handle), port=args.port))
    loop.run_forever()


if __name__ == "__main__":
    main()