import asyncio
import json
from collections import deque

from models.message import Message


class PersistenceMixin:
    """Mixin to add persistence to broker"""

    def load_state(self):
        try:
            with open("./broker_progress.json") as f:
                data = json.load(f)
                self.queue = deque([Message(**msg_dict) for msg_dict in data["queue"]])
                self.dead_letter_queue = deque(
                    [Message(**msg_dict) for msg_dict in data["dead_letter_queue"]]
                )
                self.in_flight = {
                    id: Message(**msg_dict)
                    for id, msg_dict in data["in_flight"].items()
                }
        except FileNotFoundError:
            print("File not found")

    async def auto_save(self):
        while True:
            await asyncio.sleep(5)
            state = {
                "queue": [msg.model_dump() for msg in self.queue],
                "dead_letter_queue": [
                    msg.model_dump() for msg in self.dead_letter_queue
                ],
                "in_flight": {
                    msg_id: msg_object.model_dump()
                    for msg_id, msg_object in self.in_flight.items()
                },
            }
            with open("broker_progress.json", "w") as f:
                try:
                    json.dump(state, f, default=str)
                except Exception as e:
                    print(e)
