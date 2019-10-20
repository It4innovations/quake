

from quake import Task, TaskInput


def test_submit():
    t1 = Task(1, 2)
    t2 = Task(1, 3, [TaskInput(t1, 0)])
