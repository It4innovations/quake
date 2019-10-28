
class InputConfig:

    def __init__(self, task_id, output_id, n_parts, layout):
        self.task_id = task_id
        self.output_id = output_id
        self.n_parts = n_parts
        self.layout = layout



class JobConfiguration:

    def __init__(self, fn, inputs, n_outputs):
        self.fn = fn
        self.inputs = inputs
        self.n_outputs = n_outputs
