import quake.client as quake
import pytest
import subprocess
from conftest import DOCKER_DIR
import json

@quake.mpi_task(n_processes=2)
def my_sleep():
    import time
    time.sleep(4)


@pytest.fixture()
def monitor_client(client):
    quake.set_global_client(client)
    quake.wait(my_sleep())
    import sys
    sys.stderr.write("Getting a /tmp/monitoring\n")
    output = subprocess.check_output(["docker-compose", "exec", "-T", "mpihead", "cat", "/tmp/monitoring"], cwd=DOCKER_DIR)
    lines = output.decode().split("\n")[:-1]
    assert len(lines) >= 12
    data = [json.loads(line) for line in lines]
    hostnames = set()
    for value in data:
        assert set(value.keys()) == {"timestamp", "resources", "service", "hostname"}
        hostnames.add(value["hostname"])
    assert hostnames == {"mpihead", "mpinode1", "mpinode2", "mpinode3"}

def test_monitoring(monitor_client):
    pass