# PUSH|msg_id|body|priority|timestamp
# PULL|msg_id|worker_id|timestamp
# ACK|msg_id|worker_id|timestamp
import os
import time
from io import TextIOWrapper


class WAL:
    def __init__(self, filename="broker.wal") -> None:
        self.filename = filename
        self.file = None

    def open(self):
        self.file = open(self.filename, "a", buffering=1)

    # def recover_from_wall(self):
    #     if not os.path.exists("broker.wal"):
    #         return

    #     with open(self.filename, "r") as f:
    #         for line in f:
    #             pass

    def _write_line(self, line):
        assert isinstance(self.file, TextIOWrapper)
        self.file.write(line + "\n")
        self.file.flush()
        os.fsync(self.file.fileno())

    def write_push(self, msg_id, body, priority):
        line = f"PUSH\x01{msg_id}\x01{body}\x01{priority}\x01{time.time()}"
        self._write_line(line)

    def write_pull(self, msg_id, worker_id):
        line = f"PULL\x01{msg_id}\x01{worker_id}\x01{time.time()}"
        self._write_line(line)

    def write_ack(self, msg_id, worker_id):
        line = f"ACK\x01{msg_id}\x01{worker_id}\x01{time.time()}"
        self._write_line(line)

    def close(self):
        if self.file:
            self.file.close()
