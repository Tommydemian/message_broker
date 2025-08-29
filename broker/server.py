import asyncio
import json
import uuid
from enum import Enum

from broker.message_queue import MessageQueue, Priority


class Method(Enum):
    PUSH = "PUSH"
    PULL = "PULL"
    ACK = "ACK"


class BrokerServer:
    def __init__(self):
        self.message_queue = MessageQueue()
        self.protocol = BrokerProtocol()

    async def send_response(self, response: dict, writer: asyncio.StreamWriter):
        formated_res = self.protocol.format_response(response)
        writer.write(formated_res)
        await writer.drain()

    # NETWORK
    async def handle_worker(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        print(f"Worker connected: {addr}")
        worker_id = str(uuid.uuid4())[:8]

        current_task = asyncio.current_task()
        print(f"New connection in task: {current_task.get_name()}")

        try:
            while True:
                print(f"[{worker_id}] Waiting for command...")  # ADD
                decoded_msg: str = ""

                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=5.0)
                    if not line:
                        break
                    decoded_msg = line.decode("utf-8").strip()
                # except asyncio.TimeoutError:
                #     print("Client sent malformed message (no newline)")
                #     writer.close()
                #     break
                except asyncio.TimeoutError:
                    print(f"[{worker_id}] Client timeout")
                    break
                except Exception as e:
                    print(f"[{worker_id}] Error processing message: {e}")

                print(f"[{worker_id}] Received: {decoded_msg}")  # ADD

                method_dict, error = self.protocol.parse_method(raw_string=decoded_msg)

                if error:
                    response = {
                        "status": "400",
                        "body": "No closing \x01 in the request",
                    }
                    await self.send_response(response=response, writer=writer)
                    continue

                if method_dict:
                    method = method_dict["method"]

                    valid, error = self.protocol.validate_method(method=method)

                    if not valid and error:
                        response = {"status": "400", "body": "Method now allowed"}
                        await self.send_response(response=response, writer=writer)
                        continue

                    body = method_dict["body"]

                    if method == Method.PUSH.value:
                        print("pushing")

                        msg_id = self.message_queue.push(
                            priority=Priority.NORMAL, body=body
                        )
                        response = {"status": "200", "message": msg_id}
                        await self.send_response(response=response, writer=writer)

                    if method == Method.PULL.value:
                        try:
                            msg = self.message_queue.pull(worker_id)
                            if msg:
                                response = {
                                    "status": "200",
                                    "message": msg.model_dump(mode="json"),
                                }
                            else:
                                response = {"status": "204", "message": "Queue empty"}

                            print(
                                f"[{worker_id}] About to send PULL response: {response.get('status')}"
                            )  # ADD
                            await self.send_response(response=response, writer=writer)
                            print(
                                f"[{worker_id}] PULL response sent successfully"
                            )  # ADD

                        except Exception as e:
                            print(f"[{worker_id}] ERROR in PULL: {e}")
                            import traceback

                            traceback.print_exc()

                    # if method == Method.PULL.value:
                    #     msg = self.message_queue.pull(worker_id)
                    #     if msg:
                    #         response = {"status": "200", "message": msg.model_dump()}
                    #     else:
                    #         response = {"status": "204", "message": "Queue empty"}
                    #     await self.send_response(response=response, writer=writer)

                    if method == Method.ACK.value:
                        if body is None:
                            response = {"status": "400", "body": "msg_id not present"}
                            await self.send_response(response=response, writer=writer)
                            continue

                        result = self.message_queue.ack(
                            msg_id=body, worker_id=worker_id
                        )
                        if result == "404":
                            response = {"status": "404", "body": "msg_id not found"}
                        elif result == "403":
                            response = {
                                "status": "403",
                                "body": "Forbidden - not your message",
                            }
                        else:
                            response = {
                                "status": "200",
                                "message": "Acknowledgment received successfully",
                            }

                        await self.send_response(response=response, writer=writer)
                        print(f"[{worker_id}] Sent response, loop continues")  # ADD
        except Exception as e:
            print(f"[{worker_id}] Fatal error in handle_worker: {e}")
        finally:
            print(f"[{worker_id}] Starting cleanup...")
        try:
            await self.message_queue.cleanup_worker(worker_id)
        except Exception as e:
            print(f"[{worker_id}] Cleanup failed: {e}")

        try:
            writer.close()
            await writer.wait_closed()
        except:
            pass
            print(f"[{worker_id}] Fully disconnected")

        # finally:
        #     await self.message_queue.cleanup_worker(worker_id)
        #     print(f"Worker {worker_id} disconnected and cleaned up")
        #     writer.close()

    async def start_server(self):
        await self.message_queue.start_background_tasks()
        server = await asyncio.start_server(
            self.handle_worker,  # Método de la clase
            "127.0.0.1",
            9092,
        )
        print("Ber listening on 9092...")
        async with server:
            await server.serve_forever()


class BrokerProtocol:
    def validate_method(self, method: str):
        valid = [Method.PULL.value, Method.PUSH.value, Method.ACK.value]
        if method not in valid:
            return False, "Invalid method"
        return True, None

    def format_response(self, response_ditc: dict):
        return json.dumps(response_ditc).encode() + b"\n"

    def parse_method(self, raw_string):
        if "\x01" not in raw_string:
            return None, "No delimiter found"

        parts = raw_string.split("\x01", 1)
        return {"method": parts[0], "body": parts[1] if parts[1] else None}, None


if __name__ == "__main__":
    broker = BrokerServer()
    asyncio.run(broker.start_server())
