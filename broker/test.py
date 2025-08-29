# test_connections.py
import asyncio
import json


async def test_client(name):
    reader, writer = await asyncio.open_connection("127.0.0.1", 9092)
    print(f"{name} connected")

    for i in range(100):
        writer.write(b"PULL|\n")
        await writer.drain()

        response = await reader.readline()
        data = json.loads(response)
        print(f"{name} got: {data['status']}")

        await asyncio.sleep(1)

    writer.close()
    await writer.wait_closed()


async def main():
    # Dos clientes simultáneos
    await asyncio.gather(test_client("Client1"), test_client("Client2"))


asyncio.run(main())
