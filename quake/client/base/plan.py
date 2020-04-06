
class Plan:

    def __init__(self):
        self.tasks = []

    def add_task(self, task):
        assert task.task_id is None
        self.tasks.append(task)

    def take_tasks(self):
        tasks = self.tasks
        self.tasks = []
        return tasks