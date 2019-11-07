class TaskInput:
    __slots__ = ("task", "output_id", "layout")

    def __init__(self, task, output_id: int, layout):
        assert 0 <= output_id < task.n_outputs
        self.task = task
        self.output_id = output_id
        self.layout = layout

    def to_dict(self):
        return {
            "task": self.task.task_id,
            "output_id": self.output_id,
            "layout": self.layout,
        }

    @staticmethod
    def from_dict(data, tasks):
        return TaskInput(tasks[data["task"]], data["output_id"], data["layout"])
