import asyncio
import json
import sys


async def worker(name, process_time):
    print(f"{name} attempting to connect...")  # ADD
    reader, writer = await asyncio.open_connection("127.0.0.1", 9092)
    print(f"{name} connected, process time: {process_time}s")

    while True:
        try:
            writer.write(b"PULL\x01\n")
            await writer.drain()
            print(f"{name}: Sent PULL, waiting for response...")
            response = await reader.readline()

            if not response:
                break

            data = json.loads(response.decode())

            if data.get("status") == "204":  # Queue empty
                print(f"{name}: Queue empty, waiting...")
                await asyncio.sleep(1)
                continue

            msg = data.get("message", {})
            msg_id = msg.get("msg_id")
            body = msg.get("body")

            print(f"{name} processing: {body}")
            await asyncio.sleep(process_time)  # Simulate work

            # ACK
            writer.write(f"ACK|{msg_id}\n".encode())
            await writer.drain()
            ack_response = await reader.readline()
            print(f"{name} completed: {body}")

        except Exception as e:
            print(f"{name} error: {e}")
            break

    writer.close()
    await writer.wait_closed()


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "Worker"
    speed = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    asyncio.run(worker(name, speed))
