# Quake

Quake is a tool for running MPI jobs defined as Python functions.

# Example

```python
#
# The following example submits two MPI tasks
#  - The first one takes no parameters, runs on 1 node and produces 4 outputs
#  - The second one runs on 4 nodes, takes a configuration (the same for each process)
#    and each MPI rank gets 1 output from task 1.
#
#
#    /---------------------------\
#    | Task 1 (my_preprocessing) |
#    |---------------------------|
#    | Rank 0                    |
#    | Out1 Out2 Out3 Out4       |
#    \---------------------------/
#        |   |    |    \----------------\
#        |   |    |                     |
#        |   |    \----------\          |
#        |   |               |          |
#        |   \-----\         |          |
#        v         v         v          v
#    /---------------------------------------\
#    | Task 2 (my_computation)               |
#    |---------------------------------------|
#    | Rank 0  | Rank 1  | Rank 0  | Rank 0  |
#    \---------------------------------------/


import quake.client as quake


@quake.mpi_task(n_processes=1, n_outputs=4)
def my_preprocessing():
    # Let us produce 4 pieces of something in a simple MPI application with 1 process
    return ["something1", "something2", "something3", "something4"]


@quake.mpi_task(n_processes=4)
@quake.arg("my_data", layout="scatter")
def my_computation(my_config, my_data):
    # This is called in 4 MPI processes
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    return "Computation at rank {}: configuration={}, data={}".format(
        rank, my_config, my_data
    )


# Creating a plan

data = my_preprocessing()
computation = my_computation("my_configuration", data)


# Submitting the plan & waiting for results

result = quake.gather(computation)

for i, r in enumerate(result):
    print("Output {}: {}".format(i, r))

#  The expected output:
#  Output 0: Computation at rank 0: configuration=my_configuration, data=something1
#  Output 1: Computation at rank 1: configuration=my_configuration, data=something2
#  Output 2: Computation at rank 2: configuration=my_configuration, data=something3
#  Output 3: Computation at rank 3: configuration=my_configuration, data=something4
```