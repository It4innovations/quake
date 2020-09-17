import psutil


def get_resources():
    cpus = psutil.cpu_percent(percpu=True)
    mem = psutil.virtual_memory().percent
    connections = len(psutil.net_connections())
    bytes = psutil.net_io_counters()
    io = psutil.disk_io_counters()

    return {
        "cpu": cpus,
        "mem": mem,
        "connections": connections,
        "net-write": 0 if bytes is None else bytes.bytes_sent,
        "net-read": 0 if bytes is None else bytes.bytes_recv,
        "disk-write": 0 if io is None else io.write_bytes,
        "disk-read": 0 if io is None else io.read_bytes,
    }
