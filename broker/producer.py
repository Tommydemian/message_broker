import asyncio


async def producer():
    reader, writer = await asyncio.open_connection("127.0.0.1", 9092)

    for i in range(100):
        writer.write(f"PUSH\x01Message {i}\n".encode())
        await writer.drain()
        response = await reader.readline()
        print(f"Pushed message {i}")
        await asyncio.sleep(0.1)  # 10 mensajes/segundo

    # Falta esto:
    writer.close()
    await writer.wait_closed()
    print("Producer finished")


asyncio.run(producer())  # Y esto para ejecutar
