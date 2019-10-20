

class Launcher:

    def __init__(self, workers):
        self.id_counter = 0
        self.tasks = {}
        self.ready_tasks = []
        self.workers = workers

    def new_id(self):
        self.id_counter += 1
        return self.id_counter

    def submit(self, tasks):
        for task in tasks:
            assert task.task_id is None
            task_id = self.new_id()
            task.task_id = task_id
            self.tasks[task_id] = task

            unfinished_deps = 0
            for t in task.deps:
                t.consumers.add(task)
                if not t.is_ready:
                    unfinished_deps += 1
            task.unfinished_deps = unfinished_deps
            if not unfinished_deps:
                self.ready_tasks.append(task)




