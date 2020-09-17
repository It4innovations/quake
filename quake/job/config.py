class JobConfiguration:
    def __init__(self, fn, n_outputs, payload=None):
        self.fn = fn
        self.n_outputs = n_outputs
        self.payload = payload
