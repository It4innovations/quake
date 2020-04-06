import quake.client as quake
import pytest


@quake.mpi_task(n_processes=1)
def my_const():
    return 12


@quake.mpi_task(n_processes=1)
def my_sum(a, b):
    return a + b


@quake.mpi_task(n_processes=1)
def my_sum_c(a, b):
    return a + b


@quake.mpi_task(n_processes=4)
def my_const4():
    return 12 + quake.job.get_rank()


@quake.mpi_task(n_processes=4)
@quake.arg("a", layout="scatter")
def my_mul4(a, b):
    return a * b



def test_wrapper_wait_and_gather(client):
    quake.set_global_client(client)

    f = my_const()
    quake.wait(f)
    quake.wait(f)
    with pytest.raises(Exception, match="flag is not set"):
        quake.remove(f)

    f = my_const(keep=True)
    quake.wait(f)
    quake.wait(f)

    assert quake.gather(f, collapse_single_output=False) == [[12]]
    assert quake.gather(f) == [12]
    assert quake.gather(f, 0) == [12]

    quake.remove(f)
    with pytest.raises(Exception, match="flag is not set"):
        quake.remove(f)

    f = my_const(keep=True)

    assert quake.gather(f, collapse_single_output=False) == [[12]]
    assert quake.gather(f) == [12]
    assert quake.gather(f, 0) == [12]

    quake.remove(f)
    with pytest.raises(Exception, match="flag is not set"):
        quake.remove(f)

    f = my_const4(keep=True)

    assert quake.gather(f, collapse_single_output=False) == [[12, 13, 14, 15]]
    assert quake.gather(f) == [12, 13, 14, 15]
    assert quake.gather(f, 0) == [12, 13, 14, 15]

    quake.remove(f)


def test_wrapper_args(client):
    quake.set_global_client(client)

    f = my_const()
    g = my_sum(f, f)
    h = my_sum(g, my_const())
    j = my_sum(h, f)
    g = my_sum_c(j, 7)

    assert quake.gather(g) == [55]

    f = my_const()
    g = my_mul4(f, 2)
    assert quake.gather(g) == [24] * 4

    f = my_const4()
    g = my_mul4(f, 2)
    assert quake.gather(g) == [24, 26, 28, 30]