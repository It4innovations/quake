import quake.client as quake
import pytest
import subprocess
from conftest import DOCKER_DIR


@quake.mpi_task(n_processes=2)
def my_sleep():
    import time
    time.sleep(4)


@pytest.fixture()
def monitor_client(client):
    quake.set_global_client(client)
    quake.wait(my_sleep())
    output = subprocess.check_output(["docker-compose", "exec", "mpihead", "cat", "/tmp/monitoring"], cwd=DOCKER_DIR)
    print(output)

def test_monitoring(monitor_client):
    pass