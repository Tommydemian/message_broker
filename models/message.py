import uuid
from datetime import datetime

from pydantic import BaseModel, Field


class Message(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    content: str
    created_at: datetime = Field(default_factory=datetime.now)
    ttl: float = 0
    failure_count: int = 0
