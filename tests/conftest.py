import logging
import os
import subprocess
import sys

import pytest

logging.basicConfig(level=0)

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DOCKER_DIR = os.path.join(TESTS_DIR, "docker")
ROOT_DIR = os.path.dirname(TESTS_DIR)

sys.path.insert(0, ROOT_DIR)

from quake import Client, Server, Worker  # noqa


@pytest.fixture(scope="session")
def docker_cluster():
    no_shutdown = os.environ.get("QUAKE_TEST_NO_SHUTDOWN") == "1"
    nodes = 3
    subprocess.check_call(
        ["docker-compose", "up", "-d", "--scale", "mpi_head=1", "--scale", "mpi_node={}".format(nodes)],
        cwd=DOCKER_DIR)
    hostnames = tuple(["mpi_head"] + ["mpi_node_{}".format(i) for i in range(1, nodes + 1)])
    yield hostnames

    if not no_shutdown:
        subprocess.check_call(["docker-compose", "down"],
                              cwd=DOCKER_DIR)


@pytest.fixture(scope="function")
def client(docker_cluster):
    workers = [Worker(hostname=hostname) for hostname in docker_cluster]
    server = Server(workers, run_prefix=("docker-compose", "exec", "-T", "--user", "mpirun", "--privileged", "mpi_head"),
                    run_cwd=DOCKER_DIR)
    server.start()
    client = Client(server)
    yield client
    server.stop()
