

from quake import Task, TaskInput, Server, Worker


def test_submit(client):
    t1 = Task(1, 2, ["ls"])
    #t2 = Task(1, 3, ["ls"])"[TaskInput(t1, 0)])

    client.submit([t1])