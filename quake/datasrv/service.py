import asyncio
import uvloop
uvloop.install()


class Service:

    def __init__(self, workdir):
        self.workdir = workdir