from .layout import Layout


class TaskInput:

    __slots__ = ("task", "output_id", "layout")

    def __init__(self, task, output_id: int, layout: Layout):
        assert 0 <= output_id < task.n_outputs
        self.task = task
        self.output_id = output_id
        self.layout = layout

    def to_dict(self):
        return {
            "task": self.task.task_id,
            "output_id": self.output_id,
            "layout": self.layout.serialize()
        }

    @staticmethod
    def from_dict(data, tasks):
        return TaskInput(
                tasks[data["task"]],
                data["output_id"],
                Layout.deserialize(data["layout"]))

    def __repr__(self):
        return "<Input task={} o={} l={}>".format(self.task.task_id, self.output_id, self.layout)
