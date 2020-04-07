import logging
import os
import pathlib
import subprocess

import click as click

CURRENT_DIR = pathlib.Path(__file__).absolute().parent
ROOT_DIR = CURRENT_DIR.parent

DATASRV_PORT = 8602


def prepare_directory(path):
    os.makedirs(path, exist_ok=True)


def start_process(commands, host=None, workdir=None, name=None, env=None, init_cmd=""):
    if not workdir:
        workdir = os.getcwd()
    workdir = os.path.abspath(workdir)

    if init_cmd:
        init_cmd = f"{init_cmd} || exit 1"

    args = []
    if env:
        args += ["env"]
        for (key, val) in env.items():
            args += [f"{key}={val}"]

    args += [str(cmd) for cmd in commands]

    if not name:
        name = "process"
    output = os.path.join(workdir, name)

    logging.info(f"Running {' '.join(str(c) for c in commands)} on {host}")

    stdout_file = f"{output}.out"
    stderr_file = f"{output}.err"
    command = f"""
cd {workdir} || exit 1
{init_cmd}
ulimit -c unlimited
{' '.join(args)} > {stdout_file} 2> {stderr_file} &
ps -ho pgid $!
""".strip()

    cmd_args = []
    if host:
        cmd_args += ["ssh", host]
    else:
        cmd_args += ["setsid"]
    cmd_args += ["/bin/bash"]
    process = subprocess.Popen(cmd_args, cwd=workdir, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    out, err = process.communicate(command.encode())
    pid = out.strip()

    if not pid:
        logging.error(
            f"Process startup failed with status: {process.returncode}, stderr: {err.decode()}, stdout: {out.decode()}")
        if os.path.isfile(stderr_file):
            with open(stderr_file) as f:
                logging.error("".join(f.readlines()))
        raise Exception(f"Process startup failed on {host if host else 'localhost'}: {command}")
    pid = int(pid)
    logging.info(f"PID: {pid}")
    return (pid, command)


def is_inside_pbs():
    return "PBS_NODEFILE" in os.environ


def get_pbs_nodes():
    assert is_inside_pbs()

    with open(os.environ["PBS_NODEFILE"]) as f:
        return [line.strip() for line in f]


def start_datasrv(node, workdir, env, init_cmd):
    datasrv_dir = workdir / f"{node}-datasrv"
    prepare_directory(datasrv_dir)

    datasrv_data_dir = datasrv_dir / "data"
    prepare_directory(datasrv_data_dir)

    name = "datasrv"
    commands = ["python", "-m", "quake.datasrv", str(datasrv_data_dir), "--port", DATASRV_PORT]
    start_process(commands, host=node, workdir=str(datasrv_dir), name=name, env=env,
                  init_cmd=init_cmd)


def start_server(workers, workdir, env, init_cmd):
    workdir = workdir / "server"
    prepare_directory(workdir)

    commands = ["python", "-m", "quake.server", "--ds-port", DATASRV_PORT, "--workers",
                ",".join(workers)]
    start_process(commands, workdir=str(workdir), name="server", env=env, init_cmd=init_cmd)


@click.command()
@click.argument("workdir")
@click.option("--init-cmd", default="")
def pbs_deploy(workdir, init_cmd):
    nodes = get_pbs_nodes()

    workdir = pathlib.Path(workdir).absolute()
    prepare_directory(workdir)

    env = {}
    env["PYTHONPATH"] = f'{ROOT_DIR}:{env.get("PYTHONPATH", "")}'

    for node in nodes:
        start_datasrv(node, workdir, env, init_cmd)
    start_server(nodes, workdir, env, init_cmd)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    pbs_deploy()
