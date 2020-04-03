import quake.client as quake
import mpi4py

@quake.mpi_task(n_processes=1)
def my_function():
    return 12


def test_wrapper_simple(client):
    f = my_function(10, keep=True)
    quake.set_global_client(client)
    quake.wait(f)