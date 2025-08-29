from collections import deque
from datetime import datetime

from models.message import Message


class PubSubMixin:
    def subscribe(self, subscriber_id: str, event: str):
        # defensive programming
        if not hasattr(self, "subscriber_queues"):
            self.subscriber_queues = {}
        if not hasattr(self, "events"):
            self.events = {}

        if subscriber_id not in self.subscriber_queues:
            self.subscriber_queues[subscriber_id] = deque()

        if event not in self.events:
            self.events[event] = []
        self.events[event].append(subscriber_id)

    def publish(self, event: str, message: Message):
        if event in self.events:
            sub_ids = self.events[event]
            # [sub1, sub2, sub3]
            for sub_id in sub_ids:
                self.subscriber_queues[sub_id].append(
                    {"event": event, "data": message, "timestamp": datetime.now()}
                )
        else:
            print("No subscribers")
