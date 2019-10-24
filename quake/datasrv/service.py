from abrpc import expose
import uvloop
uvloop.install()


class Object:

    def __init__(self, name, data):
        self.name = name
        self.data = data
        self.size = len(data)

    def to_dict(self):
        return {
            "name": self.name,
            "size": self.size,
        }


class Service:

    def __init__(self, workdir):
        self.workdir = workdir
        self.objects = {}

    @expose()
    async def list_objects(self):
        return [obj.to_dict() for obj in self.objects.values()]

    @expose()
    async def upload(self, name, data):
        if name in self.objects:
            raise Exception("Object '{}' already exists".format(name))
        if not isinstance(data, bytes):
            raise Exception("Data is not bytes")
        self.objects[name] = Object(name, data)
