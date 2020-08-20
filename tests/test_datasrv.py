import asyncio
import subprocess
import time

import abrpc
import pytest


def test_data_service(tmpdir, root_dir):
    PORT1 = 15001
    PORT2 = 15002
    env = {"PYTHONPATH": root_dir}
    ps = []
    ps.append(
        subprocess.Popen(
            [
                "python3",
                "-m",
                "quake.datasrv",
                "--port",
                str(PORT1),
                str(tmpdir.join("srv1")),
            ],
            env=env,
        )
    )
    ps.append(
        subprocess.Popen(
            [
                "python3",
                "-m",
                "quake.datasrv",
                "--port",
                str(PORT2),
                str(tmpdir.join("srv2")),
            ],
            env=env,
        )
    )

    async def main():
        connection1 = abrpc.Connection(
            await asyncio.open_connection("localhost", PORT1)
        )
        asyncio.ensure_future(connection1.serve())
        assert [] == await connection1.call("list_objects")
        await connection1.call("upload", "x_1", b"123")
        assert [{"name": "x_1", "size": 3}] == await connection1.call("list_objects")

        data = await connection1.call("get_data", "x_1")
        assert data == b"123"

        connection2 = abrpc.Connection(
            await asyncio.open_connection("localhost", PORT2)
        )
        asyncio.ensure_future(connection2.serve())
        c1 = connection2.call("get_data", "x_1", "localhost", PORT1)
        c2 = connection2.call("get_data", "x_1", "localhost", PORT1)
        assert [b"123", b"123"] == await asyncio.gather(c1, c2)

        # path1 = await connection2.call("map_to_fs", "x_1")
        # assert isinstance(path1, str)
        # path2 = await connection2.call("map_to_fs", "x_1")
        # assert path2 == path1

        await connection1.call("remove", "x_1")

        with pytest.raises(abrpc.RemoteException):
            await connection1.call("get_data", "x_1")

        s1 = await connection1.call("get_stats")
        s2 = await connection2.call("get_stats")

        m = s1.pop("resources")
        assert "cpu" in m

        m = s2.pop("resources")
        assert "cpu" in m

        assert s1 == {
            "connections": 0,
            "obj_data_provided": 2,
            "obj_fetched": 0,
            # 'obj_file_provided': 0
        }
        assert s2 == {
            "connections": 1,
            "obj_data_provided": 2,
            "obj_fetched": 1,
            # 'obj_file_provided': 2
        }

        assert b"123" == await connection1.call("get_data", "x_1", "localhost", PORT2)
        assert b"123" == await connection1.call("get_data", "x_1", "localhost", PORT2)

        s1 = await connection1.call("get_stats")
        s2 = await connection2.call("get_stats")

        m = s1.pop("resources")
        assert "cpu" in m

        m = s2.pop("resources")
        assert "cpu" in m

        assert s1 == {
            "connections": 1,
            "obj_data_provided": 4,
            "obj_fetched": 1,
            # 'obj_file_provided': 0
        }
        assert s2 == {
            "connections": 1,
            "obj_data_provided": 3,
            "obj_fetched": 1,
            # 'obj_file_provided': 2
        }

    try:
        time.sleep(0.3)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    finally:
        for p in ps:
            p.kill()
