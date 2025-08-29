# evil_test2.py
import asyncio
import json


async def evil_worker(name, delay_before_ack):
    reader, writer = await asyncio.open_connection("127.0.0.1", 9092)

    while True:
        writer.write(b"PULL|\n")
        await writer.drain()
        response = await reader.readline()
        data = json.loads(response)

        if data["status"] == "200":
            msg = data["message"]
            msg_id = msg["msg_id"]
            print(f"{name}: Got message {msg_id}, waiting {delay_before_ack}s...")

            await asyncio.sleep(delay_before_ack)

            print(f"{name}: Sending ACK for {msg_id}")
            writer.write(f"ACK|{msg_id}\n".encode())
            await writer.drain()
            ack_response = await reader.readline()
            ack_data = json.loads(ack_response)

            print(f"{name}: ACK response: {ack_data}")

            if ack_data["status"] == "404":
                print("RACE CONDITION DETECTED! Message was requeued before ACK")

            break

        await asyncio.sleep(0.1)

    writer.close()


async def main():
    # Push un mensaje
    reader, writer = await asyncio.open_connection("127.0.0.1", 9092)
    writer.write(b"PUSH|Test message\n")
    await writer.drain()
    await reader.readline()
    writer.close()
    print("Message pushed")

    # Múltiples workers con diferentes delays cerca del timeout
    await asyncio.gather(
        evil_worker("Evil1", 4.9),
        evil_worker("Evil2", 5.0),
        evil_worker("Evil3", 5.1),
    )


if __name__ == "__main__":
    asyncio.run(main())
