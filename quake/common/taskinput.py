from .layout import Layout


class TaskInput:
    __slots__ = ("task", "output_ids", "layout")

    def __init__(self, task, output_ids, layout: Layout):
        assert all(0 <= i < task.n_outputs for i in output_ids)
        self.task = task
        self.output_ids = output_ids
        self.layout = layout

    def to_dict(self):
        return {
            "task": self.task.task_id,
            "output_ids": self.output_ids,
            "layout": self.layout.serialize(),
        }

    @staticmethod
    def from_dict(data, tasks):
        return TaskInput(
            tasks[data["task"]], data["output_ids"], Layout.deserialize(data["layout"])
        )

    def __repr__(self):
        return "<Input task={} o={} l={}>".format(
            self.task.task_id, self.output_ids, self.layout
        )
