import uuid
from datetime import datetime

from broker import Broker

msg_content = [
    "User A sent a chat message to User B",
    "Sensor #42 reports temperature = 21.7°C",
    "Payment of $59.99 completed for order #8843",
    "System health check: all services are online",
    "New follower: @dog123 started following @catlover",
    "Error log: Database connection timeout at 14:23",
    "Video encoding finished for file movie.mp4",
    "Notification: Your password will expire in 5 days",
    "Task assigned: Finish backend docs by Tuesday",
    "Weather alert: Heavy rain expected tomorrow",
]


class Message:
    def __init__(self, content):
        self.id = str(uuid.uuid4())
        self.content = content
        self.created_at = datetime.now()
        self.ttl = 0
        self.failure_count = 0


def main():
    broker = Broker()
    for content in msg_content:
        message = Message(content=content)
        broker.push(message)
