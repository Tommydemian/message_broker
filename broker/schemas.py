# broker/types.py
from datetime import datetime
from typing import TypedDict

from models.message import Message


class SubscriberQueueItem(TypedDict):
    event: str
    data: Message  # Will be Message.dict()
    timestamp: datetime
