# import cloudpickle
import pickle

import pytest

import quake.job


def test_task_fail(client):
    t1 = client.new_task(1, 2, {"type": "mpirun", "args": ["ls", "/xxx"]}, keep=True)
    client.submit()
    with pytest.raises(Exception, match="Task id=. failed."):
        client.wait(t1)


def test_simple_mpi_task(client):
    t1 = client.new_mpirun_task(1, 2, ["ls", "/"], keep=True)
    t2 = client.new_mpirun_task(1, 2, ["ls", "/"], keep=False, inputs=[t1.output(0)])
    t3 = client.new_mpirun_task(1, 2, ["ls", "/"], keep=True, inputs=[t2.output(0)])
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
    assert [b"123", b"567"] == client.gather(t1, 0)

    client.unkeep(t1)

    with pytest.raises(Exception, match=".*keep.*"):
        client.gather(t1, 0)


def job1(job, input_data):
    assert input_data == []
    return [b"out" + str(job.rank).encode("ascii")]


def job2(job, input_data):
    return [str(job.rank).encode() + input_data[0][0] + input_data[0][1]]


def test_py_job(client):
    cfg = quake.job.config.JobConfiguration(job1, 1)
    task_data = pickle.dumps(cfg)

    t1 = client.new_py_task(1, 2, task_data=task_data, keep=True)
    # client.submit()
    # client.wait(t1)
    # assert [b"out0", b"out1"] == client.gather(t1, 0)

    cfg = quake.job.config.JobConfiguration(job2, 1)
    task_data = pickle.dumps(cfg)
    t2 = client.new_py_task(1, 3, task_data=task_data, keep=True, inputs=[t1.output(0)])
    client.submit()
    client.wait(t2)

    assert [b"out0", b"out1"] == client.gather(t1, 0)
    assert [b"out0", b"out1"] == client.gather(t1, 0)  # Check the same call again
    assert [b"0out0out1", b"1out0out1", b"2out0out1"] == client.gather(t2, 0)
    assert [b"0out0out1", b"1out0out1", b"2out0out1"] == client.gather(t2, 0)  # Check the same call again
