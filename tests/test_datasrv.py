import asyncio
import subprocess
import time
import abrpc


def test_data_service(tmpdir, root_dir):
    PORT = 15001
    env = {"PYTHONPATH": root_dir}
    ps = []
    ps.append(subprocess.Popen(["python3", "-m", "quake.datasrv", "--port", str(PORT), str(tmpdir.join("srv1"))], env=env))

    async def main():
        connection = abrpc.Connection(await asyncio.open_connection("localhost", PORT))
        asyncio.ensure_future(connection.serve())
        assert [] == await connection.call("list_objects")
        await connection.call("upload", "x_1", b"123")
        assert [{"name": "x_1", "size": 3}] == await connection.call("list_objects")

    try:
        time.sleep(0.3)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    finally:
        for p in ps:
            p.kill()
