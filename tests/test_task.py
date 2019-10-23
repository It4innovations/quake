import pytest

from quake.client import Task


def test_task_fail(client):
    t1 = Task(1, 2, ["ls", "/xxx"])

    client.submit([t1])
    with pytest.raises(Exception, match="Task id=. failed."):
        client.wait_for_task(t1)


def test_simple_task(client):
    t1 = Task(1, 2, ["ls", "/"])
    #client.submit([t1])
    #client.wait_for_task(t1)
