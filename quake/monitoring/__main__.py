import click
import pandas as pd
import plotly.express as px

from .events import EventStream


def write_fig(fig, name):
    print("Creating '{}'".format(name))
    fig.write_html(name)


@click.command()
@click.argument("filename")
def cli(filename):
    stream = EventStream.load(filename)
    resources = stream.resources()

    res = resources[["timestamp", "hostname", "cpu", "mem"]].melt(
        ["timestamp", "hostname"]
    )
    fig = px.line(
        res,
        x="timestamp",
        y="value",
        color="variable",
        facet_row="hostname",
        render_mode="svg",
    )
    write_fig(fig, "cpumem.html")

    res = resources[
        ["timestamp", "hostname", "net-write", "net-read", "disk-write", "disk-read"]
    ].melt(["timestamp", "hostname"])
    fig = px.line(
        res,
        x="timestamp",
        y="value",
        color="variable",
        facet_row="hostname",
        render_mode="svg",
    )
    write_fig(fig, "transfers.html")

    res = resources[
        ["timestamp", "hostname", "obj_fetched", "obj_provided", "obj_uploaded"]
    ].melt(["timestamp", "hostname"])
    fig = px.line(
        res,
        x="timestamp",
        y="value",
        color="variable",
        facet_row="hostname",
        render_mode="svg",
    )
    write_fig(fig, "services-objs.html")

    res = resources[
        ["timestamp", "hostname", "bytes_fetched", "bytes_provided", "bytes_uploaded"]
    ].melt(["timestamp", "hostname"])
    fig = px.line(
        res,
        x="timestamp",
        y="value",
        color="variable",
        facet_row="hostname",
        render_mode="svg",
    )
    write_fig(fig, "services-bytes.html")

    task_events = stream.task_events()
    pivoted = task_events.pivot(index=["task", "rank", "hostname"], columns=["type"])
    pivoted.reset_index(inplace=True)
    pivoted.columns = [y if y else x for x, y in pivoted.columns]

    df1 = pivoted.rename(columns={"init": "Begin", "start": "End"})
    df1["type"] = "init"

    df2 = pivoted.rename(columns={"start": "Begin", "end": "End"})
    df2["type"] = "run"
    df = pd.concat([df1, df2])

    fig = px.timeline(df, x_start="Begin", x_end="End", y="hostname", color="type")
    write_fig(fig, "tasks-per-node.html")

    df3 = df.groupby("task").agg(
        {"Begin": "min", "End": "max", "hostname": lambda x: ",".join(sorted(set(x)))}
    )
    df3.reset_index(inplace=True)
    print(df3)

    fig = px.timeline(df3, x_start="Begin", x_end="End", y="task", color="hostname")
    write_fig(fig, "tasks.html")
    print(pivoted)


if __name__ == "__main__":
    cli()
