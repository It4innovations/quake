import logging
import os
import socket
import subprocess
import sys
import time

import pytest

logging.basicConfig(level=0)

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DOCKER_DIR = os.path.join(TESTS_DIR, "docker")
ROOT_DIR = os.path.dirname(TESTS_DIR)

sys.path.insert(0, ROOT_DIR)

from quake.client import Client  # noqa
from quake.client.functions import reset_global_plan, set_global_client  # noqa


nodes = 3


@pytest.fixture(scope="session")
def docker_cluster():
    no_shutdown = os.environ.get("QUAKE_TEST_NO_SHUTDOWN") == "1"
    subprocess.check_call(["docker-compose", "up", "-d"], cwd=DOCKER_DIR)
    hostnames = tuple(
        ["mpihead"] + ["mpinode{}".format(i) for i in range(1, nodes + 1)]
    )
    yield hostnames

    if not no_shutdown:
        subprocess.check_call(["docker-compose", "down"], cwd=DOCKER_DIR)


cmd_prefix = ["docker-compose", "exec", "-T", "--user", "mpirun", "--privileged"]


def make_cmds(cmd):
    result = [cmd_prefix + ["mpihead"] + cmd]
    for i in range(1, nodes + 1):
        result.append(cmd_prefix + ["mpinode{}".format(i)] + cmd)
    return result


def run_cmds(cmd):
    for c in make_cmds(cmd):
        subprocess.call(c, cwd=DOCKER_DIR)


def wait_for_port(port):
    print("Waiting for port", port)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", port))
    s.settimeout(6)
    s.close()


def popen_helper(cmd, logfile, **kwargs):
    with open(logfile, "w") as f:
        return subprocess.Popen(
            cmd,
            cwd=DOCKER_DIR,
            stdin=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            stdout=f,
            **kwargs
        )


@pytest.fixture(scope="function")
def client(docker_cluster, tmpdir):
    print("Working directory:", tmpdir)
    ps = []
    logdir = tmpdir.mkdir("logs")
    for i, cmd in enumerate(
        make_cmds(
            [
                "/bin/bash",
                "-c",
                "pgrep python3 | xargs kill; sleep 0.1 ; rm -rf /tmp/data ; python3 -m quake.datasrv /tmp/data",
            ]
        )
    ):
        p = popen_helper(cmd, logfile=logdir.join("cleanup-{}".format(i)))
        ps.append(p)

    time.sleep(1.5)

    hostnames = ",".join(docker_cluster)
    # cmd = cmd_prefix + ["mpi_head", "/bin/bash", "-c", "kill `pgrep -f quake.server` ; sleep 0.1; echo 'xxx'; python3 -m quake.server --workers={}".format(hostnames)]
    cmd = cmd_prefix + [
        "mpihead",
        "/bin/bash",
        "-c",
        "python3 -m quake.server --debug --workers={}".format(hostnames),
    ]
    # print(" ".join(cmd))
    popen_helper(cmd, logfile=logdir.join("server"))
    ps.append(p)

    time.sleep(3)
    client = Client(port=7600)
    client.DEFAULT_ENV = {"PYTHONPATH": "/app:/app/tests"}

    # mapped in docker-compose.yml
    # wait_for_port(7602)
    # wait_for_port(7603)
    # wait_for_port(7604)
    # wait_for_port(7605)

    reset_global_plan()
    set_global_client(None)

    yield client

    reset_global_plan()
    set_global_client(None)

    print("Clean up")
    for p in ps:
        p.kill()
    time.sleep(0.1)
    for p in ps:
        p.terminate()
        p.wait()


@pytest.fixture()
def root_dir():
    return ROOT_DIR
