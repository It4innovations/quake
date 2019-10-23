
import logging

logger = logging.getLogger(__name__)


class Client:

    def __init__(self, server):
        self.server = server

    def submit(self, tasks):
        self.server.loop.call_soon_threadsafe(self.server.submit, tasks)

    def wait_for_task(self, task):
        async def wait_for_task():
            logger.debug("Waiting for task %s", task)
            if task.state == TaskState.UNFINISHED:
                event = asyncio.Event()
                task.add_event(event)
                await event.wait()
            if task.state == TaskState.ERROR:
                return task.error

        f = asyncio.run_coroutine_threadsafe(wait_for_task(), loop=self.server.loop)
        result = f.result()
        if result is not None:
            raise Exception(result)
