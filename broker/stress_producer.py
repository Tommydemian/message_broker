# stress_producer.py
import asyncio


async def producer():
    reader, writer = await asyncio.open_connection("127.0.0.1", 9092)

    for i in range(1000):
        writer.write(f"PUSH|Msg{i}\n".encode())
        await writer.drain()
        await reader.readline()

    print("Pushed 1000 messages")
    writer.close()


asyncio.run(producer())
