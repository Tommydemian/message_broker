import asyncio
import uuid
from datetime import datetime, timedelta
from enum import IntEnum

from pydantic import BaseModel, Field


class Priority(IntEnum):
    CRITICAL = 1
    URGENT = 2
    HIGH = 3
    MEDIUM_HIGH = 4
    ABOVE_NORMAL = 5
    NORMAL = 6
    BELOW_NORMAL = 7
    MEDIUM_LOW = 8
    LOW = 9
    LOWEST = 10


class Task(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    priority: Priority
    scheduled_time: datetime
    payload: str


class TaskScheduler:
    def __init__(self):
        self.completed: list[Task] = []
        self.pending: list[Task] = []

    # Las tareas se ejecutan por prioridad, pero SOLO si su scheduled_time ya pasó
    def insert_into_idx(self, task_priority):
        left = 0
        right = len(self.pending) - 1
        boundary_index = -1

        while left <= right:
            mid = (left + right) // 2

            if self.pending[mid]:
                if self.pending[mid] >= task_priority:
                    boundary_index = mid
                    right = mid - 1
                else:
                    left = mid + 1

        return boundary_index

    async def _save(self):
        # Persiste su estado en JSON
        # Debe sobrevivir reinicios
        # Guarda tanto las pending como las completed
        while True:
            await asyncio.sleep(5)
            with open("task_scheduler", "w") as f:
                # json.dump(self.tasks, f, default=str)
                pass

    def shedule(self, payload: str, priority: Priority, delay_seconds: int):
        scheduled_time = datetime.now() + timedelta(seconds=delay_seconds)
        task = Task(payload=payload, priority=priority, scheduled_time=scheduled_time)
        self.insert_into_idx(task_priority=task.priority.value)
        # programa una tarea
        pass

    def get_next(self):
        # get_next() - retorna la próxima tarea ejecutable (mayor prioridad cuyo tiempo ya pasó)
        pass

    def complete(self, task_id):
        if task_id in self.pending:
            # pop task from pending {} - O(1)
            task = self.pending.pop(task_id)
            # append it on completed
            self.completed.append(task)

    def get_stats(self):
        stats = {
            "pending_count": len(self.pending.keys()),
            "completed_count": len(self.completed),
            # "next_task_time": 10,
        }
        return stats

    # get_stats() - retorna dict con: pending_count, completed_count, next_task_time


# El twist:

# Usa un dict para lookup O(1) por id
# Usa una deque para completed (no necesitas buscar ahí)
# Pero para pending... ¿qué estructura usas si necesitas ordenar por prioridad Y tiempo?

# Restricción:
# Sin heapq ni librerías fancy. Solo dict, list, deque, json, time.
# Código directo. Nada de explicaciones. Si funciona, funciona.
