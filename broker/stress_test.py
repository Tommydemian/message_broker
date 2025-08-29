# stress_test.py
import asyncio
import json


async def aggressive_worker(name):
    reader, writer = await asyncio.open_connection("127.0.0.1", 9092)

    for _ in range(100):
        writer.write(b"PULL|\n")
        await writer.drain()
        response = await reader.readline()
        data = json.loads(response)

        if data["status"] == "200":
            msg = data["message"]
            # ACK inmediato
            writer.write(f"ACK|{msg['msg_id']}\n".encode())
            await writer.drain()
            ack_response = await reader.readline()
            print(f"{name} processed {msg['msg_id']}")

        # Sin sleep - máxima velocidad

    writer.close()


async def main():
    # 10 workers agresivos
    tasks = [aggressive_worker(f"W{i}") for i in range(10)]
    await asyncio.gather(*tasks)


asyncio.run(main())
