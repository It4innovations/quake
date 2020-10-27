import logging
import os
import pathlib

import click as click

from cluster import CLUSTER_FILENAME, Cluster, HOSTNAME, start_process

CURRENT_DIR = pathlib.Path(__file__).absolute().parent
ROOT_DIR = CURRENT_DIR.parent

DATASRV_PORT = 8602


def prepare_directory(path):
    os.makedirs(path, exist_ok=True)


def is_inside_pbs():
    return "PBS_NODEFILE" in os.environ


def get_pbs_nodes(use_short_names):
    if not is_inside_pbs():
        raise Exception("Not in PBS job")

    with open(os.environ["PBS_NODEFILE"]) as f:
        nodes = [line.strip() for line in f]
    if use_short_names:
        nodes = [line.split(".")[0] for line in nodes]
    return nodes


def start_datasrv(cluster, node, workdir, env, init_cmd):
    datasrv_dir = workdir / f"{node}-datasrv"
    prepare_directory(datasrv_dir)

    datasrv_data_dir = datasrv_dir / "data"
    prepare_directory(datasrv_data_dir)

    name = "datasrv"
    commands = ["python", "-m", "quake.datasrv", "--port", DATASRV_PORT]
    pid, cmd = start_process(commands, host=node, workdir=str(datasrv_dir), name=name, env=env,
                             init_cmd=init_cmd)
    cluster.add(node, pid, cmd, key="datasrv")


def start_server(cluster, workers, workdir, env, init_cmd):
    workdir = workdir / "server"
    prepare_directory(workdir)

    commands = ["python", "-m", "quake.server", "--ds-port", DATASRV_PORT, "--workers",
                ",".join(workers)]
    pid, cmd = start_process(commands, workdir=str(workdir), name="server", env=env,
                             init_cmd=init_cmd)
    cluster.add(HOSTNAME, pid, cmd, key="server")


@click.command()
@click.argument("workdir")
@click.option("--init-cmd", default="")
@click.option("--short-names", default=False, is_flag=True)
def up(workdir, init_cmd, short_names):
    nodes = get_pbs_nodes(short_names)

    workdir = pathlib.Path(workdir).absolute()
    prepare_directory(workdir)

    env = None
    #env["PYTHONPATH"] = f'{ROOT_DIR}:{env.get("PYTHONPATH", "")}'

    cluster = Cluster(str(workdir))
    for node in nodes:
        start_datasrv(cluster, node, workdir, env, init_cmd)
    start_server(cluster, nodes, workdir, env, init_cmd)

    cluster_path = workdir / CLUSTER_FILENAME
    logging.info(f"Writing cluster into {cluster_path}")
    with open(cluster_path, "w") as f:
        cluster.serialize(f)


@click.command()
@click.argument("workdir")
def down(workdir):
    with open(os.path.join(workdir, CLUSTER_FILENAME)) as f:
        cluster = Cluster.deserialize(f)
        cluster.kill()


@click.group()
def cli():
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    cli.add_command(up)
    cli.add_command(down)
    cli()
