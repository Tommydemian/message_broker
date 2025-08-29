import asyncio
import time
import uuid
from collections import deque
from datetime import datetime
from enum import IntEnum

from pydantic import BaseModel, ConfigDict, Field

from broker.wal import WAL


class Priority(IntEnum):
    HIGH = 0
    NORMAL = 1
    LOW = 2


class Message(BaseModel):
    model_config = ConfigDict(extra="ignore")

    msg_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    body: str
    priority: Priority
    created_at: datetime = Field(default_factory=datetime.now)


class MessageQueue:
    def __init__(self, v_timeout_time: int = 5) -> None:
        self.queues = {
            Priority.HIGH: deque(),
            Priority.NORMAL: deque(),
            Priority.LOW: deque(),
        }
        self.in_flight = {}
        self.v_timeout_time = v_timeout_time

        self.stats = {"push": 0, "pull": 0, "ack": 0, "duplicates": 0}
        self.processed_messages = set()

        self.wal = WAL()

    async def report(self):
        while True:
            await asyncio.sleep(5)
            print(self.stats)

    async def visibility_timeout_checker(self):
        while True:
            await asyncio.sleep(5)  # check cada segundo
            current_time = time.time()

            messages_to_requeue = []

            for msg_id, info in list(self.in_flight.items()):
                if current_time - info["pulled_at"] > self.v_timeout_time:
                    messages_to_requeue.append((msg_id, info))

            for msg_id, info in messages_to_requeue:
                del self.in_flight[msg_id]
                # Reconstruir mensaje y requeue
                msg = Message(**info)
                self.queues[msg.priority].append(msg)

            if messages_to_requeue:
                print(f"Requeued {len(messages_to_requeue)} timed-out messages")

            stats = {
                **self.stats,
                "in_flight": len(self.in_flight),
                "queued": sum(len(q) for q in self.queues.values()),
                "duplicates": self.stats.get("duplicates", 0),
            }
            print(f"STATS: {stats}")

    async def start_background_tasks(self):
        asyncio.create_task(self.visibility_timeout_checker())
        self.wal.open()

    async def cleanup_worker(self, worker_id):
        messages_to_return = []

        for msg_id, info in list(self.in_flight.items()):
            if info["worker_id"] == worker_id:
                # FIX: No buscar info["message"]
                messages_to_return.append(info)
                del self.in_flight[msg_id]

        for info in reversed(messages_to_return):
            # Reconstruir Message desde el dict
            msg = Message(**info)
            self.queues[msg.priority].appendleft(msg)

        if messages_to_return:
            print(
                f"Worker {worker_id} died with {len(messages_to_return)} unacked messages"
            )

    def ack(self, msg_id, worker_id):
        if msg_id in self.in_flight:
            msg = self.in_flight[msg_id]
            if msg["worker_id"] == worker_id:
                self.wal.write_ack(msg_id=msg_id, worker_id=worker_id)
                del self.in_flight[msg_id]
                self.stats["ack"] += 1
                return "200"
            else:
                return "403"
        else:
            return "404"

    def push(self, body, priority=Priority.NORMAL) -> str:
        message = Message(body=body, priority=priority)
        try:
            self.wal.write_push(msg_id=message.msg_id, body=body, priority=priority)
        except Exception as e:
            print(e)
            raise

        self.queues[priority].append(message)
        self.stats["push"] += 1
        msg_id = message.msg_id
        return msg_id

    def get_stats(self):
        return {
            **self.stats,
            "in_flight": len(self.in_flight),
            "queued": sum(len(q) for q in self.queues.values()),
        }

    def pull(self, worker_id) -> Message | None:
        print(f"Pull called by {worker_id}")  # ADD
        # Iterate queues
        for priority in [Priority.HIGH, Priority.NORMAL, Priority.LOW]:
            # if priority[i]
            if self.queues[priority]:
                # get queue[0] message -> popleft() == pop(0)
                msg: Message = self.queues[priority].popleft()
                self.wal.write_pull(msg.msg_id, worker_id)
                if msg:
                    # if msg.msg_id in self.processed_messages:
                    #     self.stats["duplicates"] += 1
                    #     print(f"WARNING: Message {msg.msg_id} pulled again!")
                    #     self.processed_messages.add(msg.msg_id)
                    #     return msg

                    self.in_flight[msg.msg_id] = {
                        **msg.model_dump(mode="json"),
                        "worker_id": worker_id,
                        "pulled_at": time.time(),
                    }
                    self.stats["pull"] += 1
                    return msg
        print("No messages available")  # ADD
        return None
