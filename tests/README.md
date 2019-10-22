
Tests starts docker cluster and it may take some time to start it and close it.
You can set variable ``QUAKE_TEST_NO_SHUTDOWN`` to ``1`` to disable shutting the cluster.
It makes tests faster (because docker cluster does not have to be started again in the next test)
However after the testing, you have to manually call 
``docker-compose down`` in ``tests/docker`` directory. 
