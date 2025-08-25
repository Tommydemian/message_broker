import asyncio
import uuid
from collections import deque
from datetime import datetime
from time import time

from pydantic import BaseModel


class Message(BaseModel):
    id: str = str(uuid.uuid4())
    content: str
    created_at: datetime = datetime.now()
    ttl: float = 0
    failure_count: int = 0


class Broker:
    def __init__(self, timeout_n: int = 30, max_retries: int = 3):
        self.queue = deque()
        self.in_flight = {}
        self.dead_letter_queue = deque()
        self.timeout = timeout_n
        self.max_retries = max_retries

    def push(self, message):
        self.queue.append(message)

    async def pull(self):
        if self.queue:
            msg: Message = self.queue.popleft()
            self.in_flight[msg.id] = msg
            msg.ttl = time() + self.timeout

            # Schedule timeout check for THIS message
            asyncio.create_task(self._timeout_checker(msg.id, self.timeout))

            return msg
        return None

    async def _timeout_checker(self, msg_id, wait_seconds):
        await asyncio.sleep(wait_seconds)

        if msg_id in self.in_flight:
            msg: Message = self.in_flight.pop(msg_id)
            if msg.failure_count < self.max_retries:
                msg.failure_count += 1
                self.queue.append(msg)
                print(f"Message {msg_id} timed out, back to queue")
            else:
                self.dead_letter_queue.append(msg)  # RIP
                print(
                    f"Message {msg_id} sent to DLQ after {self.max_retries} attempts reached."
                )

    def ack(self, msg_id):
        if msg_id in self.in_flight:
            del self.in_flight[msg_id]
            return True
        return False
