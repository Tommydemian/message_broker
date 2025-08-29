import asyncio
from collections import deque
from time import time

from broker.schemas import SubscriberQueueItem
from models.message import Message
from old_broker.persistence import PersistenceMixin
from old_broker.pubsub import PubSubMixin


class Broker(PubSubMixin, PersistenceMixin):
    def __init__(self, timeout_n: int = 30, max_retries: int = 3):
        self.queue = deque()
        self.dead_letter_queue = deque()
        self.in_flight = {}
        self.timeout = timeout_n
        self.max_retries = max_retries

        # PUB/SUB
        self.events: dict[str, list[str]] = {}
        # {'uer_login': [sub1, sub2, sub3]}
        self.subscriber_queues: dict[str, deque[SubscriberQueueItem]] = {}

        self.load_state()

    # LOAD BALANCING
    async def _timeout_checker(self, msg_id, wait_seconds):
        await asyncio.sleep(wait_seconds)

        if msg_id in self.in_flight:
            msg: Message = self.in_flight.pop(msg_id)
            msg.failure_count += 1

            if msg.failure_count >= self.max_retries:
                self.dead_letter_queue.append(msg)  # RIP
                print(
                    f"Message {msg_id} sent to DLQ after >{self.max_retries} attempts"
                )
            else:
                self.queue.append(msg)  # try again

    def push(self, msg: Message):
        self.queue.append(msg)

    async def pull(self):
        if self.queue:
            msg = self.queue.popleft()
            self.in_flight[msg.id] = msg
            msg.ttl = time() + self.timeout

            # Schedule timeout check for THIS message
            asyncio.create_task(self._timeout_checker(msg.id, self.timeout))

            return msg
        return None

    def ack(self, msg_id):
        if msg_id in self.in_flight:
            del self.in_flight[msg_id]
            return True
        return False
