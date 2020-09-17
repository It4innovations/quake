import quake.client as quake
import pytest
import subprocess
from conftest import DOCKER_DIR
import json
import time


@quake.mpi_task(n_processes=2)
def my_sleep():
    time.sleep(quake.job.get_rank() + 1)


@quake.mpi_task(n_processes=1)
def my_sleep2(x):
    time.sleep(1)


@pytest.fixture()
def monitor_client(client):
    quake.set_global_client(client)
    s = my_sleep()
    s2 = my_sleep2(s)
    s3 = my_sleep2(s)
    quake.wait_all([s2, s3])
    import sys
    time.sleep(2)
    sys.stderr.write("Getting a /tmp/monitoring\n")
    output = subprocess.check_output(["docker-compose", "exec", "-T", "mpihead", "cat", "/tmp/monitoring"], cwd=DOCKER_DIR)
    #print(output)
    #with open("/tmp/x", "wb") as f:
    #    f.write(output)
    lines = output.decode().split("\n")[:-1]
    assert len(lines) >= 12
    data = [json.loads(line) for line in lines]
    hostnames = set()
    for value in data:
        assert set(value.keys()).issubset({"timestamp", "resources", "service", "hostname", "events"})
        hostnames.add(value["hostname"])
    assert hostnames == {"mpihead", "mpinode1", "mpinode2", "mpinode3"}

def test_monitoring(monitor_client):
    pass