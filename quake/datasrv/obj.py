import aiofiles
import asyncio
import os


class Object:

    def __init__(self, name, data):
        self.name = name
        self.data = data
        self.filename = None
        self.size = len(data)
        self.lock = asyncio.Lock()

    def to_dict(self):
        return {
            "name": self.name,
            "size": self.size,
        }

    async def remove(self):
        async with self.lock:
            if self.filename:
                await aiofiles.os.remove(self.filename)
            self.data = None
            self.filename = None

    async def get_data(self):
        if self.data is not None:
            return self.data

        async with self.lock:
            if self.filename:
                async with aiofiles.open(self.filename, "rb") as f:
                    return await f.read()
            else:
                return None

    async def map_to_fs(self, workdir):
        async with self.lock:
            if self.filename is not None:
                return self.filename
            if self.data is None:
                return None
            filename = os.path.join(workdir, self.name)
            self.filename = filename
            async with aiofiles.open(filename, "wb") as f:
                await f.write(self.data)
                self.data = None
            return filename