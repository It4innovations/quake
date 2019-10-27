import pytest


def test_task_fail(client):
    t1 = client.new_task(1, 2, ["ls", "/xxx"])
    client.submit()
    with pytest.raises(Exception, match="Task id=. failed."):
        client.wait(t1)


def test_simple_task(client):
    t1 = client.new_task(1, 2, ["ls", "/"], keep=True)
    t2 = client.new_task(1, 2, ["ls", "/"], keep=False, inputs=[t1.output(0)])
    t3 = client.new_task(1, 2, ["ls", "/"], keep=True, inputs=[t2.output(0)])
    client.submit()
    client.wait(t3)
