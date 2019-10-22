class Worker:

    def __init__(self, hostname):
        self.worker_id = None
        self.hostname = hostname

    def __repr__(self):
        return "<Worker id={}>".format(self.worker_id)
