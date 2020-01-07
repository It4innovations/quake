class Worker:

    def __init__(self, worker_id, hostname):
        self.worker_id = worker_id
        self.hostname = hostname
        self.ds_connection = None
        self.tasks = set()

    def __repr__(self):
        return "<Worker id={}>".format(self.worker_id)
