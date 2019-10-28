import pytest
import cloudpickle
import quake.job


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


def test_upload_download(client):
    t1 = client.upload_data([b"123", b"567"], keep=True)
    client.submit()
    client.wait(t1)

    t2 = client.upload_data([b"abcd", b"xyz", b"fffff"], keep=True)
    client.submit()
    assert [b"abcd", b"xyz", b"fffff"] == client.gather(t2, 0)
    assert [b"123", b"567"] == client.gather(t1, 0)


def test_py_job(client):
    def job1(_inputs):
        return [b"out1"]

    cfg = quake.job.config.JobConfiguration(job1, (), 1)
    cfg_data = cloudpickle.dumps(cfg)

    t1 = client.new_py_task(1, 2, config=cfg_data, keep=True)
    client.submit()
    client.wait(t1)
