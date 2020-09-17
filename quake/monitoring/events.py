import json
from datetime import datetime
from pandas import DataFrame
import numpy as np


class EventStream:
    def __init__(self, events):
        self.events = events

    def resources(self):

        res_names = ["mem", "net-write", "net-read", "disk-write", "disk-read"]

        srv_names = [
            "obj_fetched",
            "bytes_fetched",
            "obj_provided",
            "bytes_provided",
            "obj_uploaded",
            "bytes_uploaded",
        ]

        timestamps = []
        hostnames = []
        cpus = []
        resources = [[] for _ in res_names]
        services = [[] for _ in srv_names]

        for event in self.events:
            hostnames.append(event["hostname"])
            timestamps.append(event["timestamp"])

            res = event["resources"]
            cpus.append(np.mean(res["cpu"]))
            for i in range(len(res_names)):
                resources[i].append(res[res_names[i]])

            srv = event["service"]
            for i in range(len(srv_names)):
                services[i].append(srv[srv_names[i]])

        columns = {
            "timestamp": timestamps,
            "hostname": hostnames,
            "cpu": cpus,
        }

        for name, value in zip(res_names, resources):
            columns[name] = value

        for name, value in zip(srv_names, services):
            columns[name] = value

        df = DataFrame(columns)
        g = df.groupby("hostname")
        for column in ["net-write", "net-read", "disk-write", "disk-read"]:
            df[column] = g[column].transform(lambda x: x - x.min())
        return df

    def task_events(self):
        rows = []
        for event in self.events:
            evs = event.get("events")
            hostname = event["hostname"]
            if evs:
                for ev in evs:
                    rows.append(
                        (
                            hostname,
                            ev["timestamp"],
                            ev["type"],
                            ev["task"],
                            ev.get("rank"),
                        )
                    )

        return DataFrame(
            rows, columns=["hostname", "timestamp", "type", "task", "rank"]
        )

    @staticmethod
    def load(filename):
        events = []
        with open(filename, "r") as f:
            for line in f:
                if line:
                    event = json.loads(line)
                    event["timestamp"] = datetime.fromisoformat(event["timestamp"])
                    evs = event.get("events")
                    if evs:
                        for ev in evs:
                            ev["timestamp"] = datetime.fromisoformat(ev["timestamp"])
                    events.append(event)
        return EventStream(events)
