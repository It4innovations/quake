from mpi4py import MPI
import socket

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

print("Hello from rank={} hostname={}".format(rank, socket.gethostname()))
