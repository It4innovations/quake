from abrpc import expose, Connection
import asyncio
import uvloop
from .obj import Object

uvloop.install()


def validate_name(name):
    if not name.isidentifier():
        raise Exception("String '{}' is not valid name")


class Service:

    def __init__(self, workdir):
        self.workdir = workdir
        self.objects = {}
        self.connections = {}

        self.stats_obj_fetched = 0
        self.stats_obj_data_provided = 0
        self.stats_obj_file_provided = 0

    async def _serve(self, connection, hostname, port):
        await connection.serve()
        del self.connections[(hostname, port)]

    async def _connect(self, hostname, port):
        key = (hostname, port)
        conn_f = self.connections.get(key)
        if conn_f is None:
            f = asyncio.Future()
            self.connections[key] = f
            connection = Connection(await asyncio.open_connection(hostname, port))
            asyncio.ensure_future(self._serve(connection, hostname, port))
            f.set_result(connection)
            return connection
        else:
            return await conn_f

    async def _get_object(self, name, hostname=None, port=None):
        obj_f = self.objects.get(name)
        if obj_f is None:
            if hostname is None:
                raise Exception("Object '{}' is not available.".format(name))
            f = asyncio.Future()
            self.objects[name] = f
            conn = await self._connect(hostname, port)
            data = await conn.call("get_data", name)
            self.stats_obj_fetched += 1
            obj = Object(name, data)
            f.set_result(obj)
            return obj
        return await obj_f

    @expose()
    async def list_objects(self):
        result = []
        for obj in self.objects.values():
            if obj.done():
                result.append(obj.result().to_dict())
        return result

    @expose()
    async def upload(self, name, data):
        validate_name(name)
        if name in self.objects:
            raise Exception("Object '{}' already exists".format(name))
        if not isinstance(data, bytes):
            raise Exception("Data is not bytes")
        obj_f = asyncio.Future()
        obj_f.set_result(Object(name, data))
        self.objects[name] = obj_f

    @expose()
    async def get_data(self, name, hostname=None, port=None):
        validate_name(name)
        obj = await self._get_object(name, hostname, port)
        self.stats_obj_data_provided += 1
        data = await obj.get_data()
        if data is None:
            # This can happen in case of racing with .remove()
            raise Exception("Object removed")
        return data

    @expose()
    async def map_to_fs(self, name, hostname=None, port=None):
        validate_name(name)
        obj = await self._get_object(name, hostname, port)
        self.stats_obj_file_provided += 1
        filename = await obj.map_to_fs(self.workdir)
        if filename is None:
            # This can happen in case of racing with .remove()
            raise Exception("Object removed")
        return filename

    @expose()
    async def remove(self, name):
        obj_f = self.objects.get(name)
        if obj_f is None:
            raise Exception("Object not found")
        del self.objects[name]
        obj = await obj_f
        await obj.remove()

    @expose()
    async def get_stats(self):
        return {
            "obj_file_provided": self.stats_obj_file_provided,
            "obj_data_provided": self.stats_obj_data_provided,
            "obj_fetched": self.stats_obj_fetched,
            "connections": len(self.connections),
        }
