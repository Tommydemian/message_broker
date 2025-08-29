import time
import uuid
from datetime import datetime
from enum import IntEnum

from pydantic import BaseModel, ConfigDict, Field


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


lib = {
    "a1b2c3d4": {  # msg_id
        "msg_id": "a1b2c3d4",
        "body": "Hello world!",
        "priority": 0,  # Priority.HIGH
        "created_at": datetime(2025, 8, 28, 22, 10, 5, 123456),
        "worker_id": "worker-1",
        "pulled_at": 1693254605.876543,  # timestamp en segundos (time.time())
    },
    "a1b2c334": {  # msg_id
        "msg_id": "a1b2c3gt",
        "body": "FIFA",
        "priority": 1,  # Priority.HIGH
        "created_at": datetime(2025, 8, 28, 22, 10, 5, 123456),
        "worker_id": "worker-1",
        "pulled_at": 19,  # timestamp en segundos (time.time())
    },
}

messages_to_requeue = []

for msg_id, info in list(lib.items()):
    if time.time() - info["pulled_at"] > 10:
        # messages_to_requeue.append((msg_id, info))
        messages_to_requeue.append(Message(**info))

# print(messages_to_requeue)


line = "PUSH\x01msg_id\x01body\x01priority\x01timestamp"
parts = line.split("\x01")
# splitted_line = line.split("|")
print(parts)
