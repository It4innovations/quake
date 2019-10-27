

class TaskInput:

    def __init__(self, task: "Task", output_id: int, layout=None):
        assert 0 <= output_id < task.n_outputs
        self.task = task
        self.output_id = output_id
        self.layout = layout

    def to_dict(self):
        return {
            "task": self.task.task_id,
            "output_id": self.output_id
        }

    @staticmethod
    def from_dict(data, tasks):
        return TaskInput(tasks[data["task"]], data["output_id"])