"""Priority queue with in-memory heap + SQLite persistence.

Tasks are sorted by (-priority, created_at) so that higher priority tasks
are dequeued first, with FIFO ordering for same-priority tasks.
"""

from __future__ import annotations

import asyncio
import heapq
import logging
from dataclasses import dataclass, field
from typing import Callable, Optional

logger = logging.getLogger("ulrds.queue")


@dataclass(order=True)
class QueueEntry:
    """A comparable entry for the priority heap.

    Sorting key: (-priority, created_at) ensures highest priority first,
    earliest submission first for ties.
    """
    sort_key: tuple = field(compare=True)
    task: dict = field(compare=False)

    @staticmethod
    def from_task(task: dict) -> "QueueEntry":
        return QueueEntry(
            sort_key=(-task["priority"], task["created_at"]),
            task=task,
        )


class TaskQueue:
    """Thread-safe async priority queue backed by an in-memory heap.

    Persistence is handled by the database layer — on startup, we reload
    all QUEUED tasks from SQLite to rebuild the heap.
    """

    def __init__(self) -> None:
        self._heap: list[QueueEntry] = []
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Event()
        self._task_ids: set[str] = set()
        self._on_push: Optional[Callable] = None

    def set_on_push(self, callback: Callable) -> None:
        """Register a callback to invoke when a task is pushed (e.g., wake scheduler)."""
        self._on_push = callback

    async def push(self, task: dict) -> None:
        """Add a task to the queue."""
        async with self._lock:
            tid = task["task_id"]
            if tid in self._task_ids:
                logger.debug("Task %s already in queue, skipping", tid)
                return
            entry = QueueEntry.from_task(task)
            heapq.heappush(self._heap, entry)
            self._task_ids.add(tid)
            self._not_empty.set()
            logger.info(
                "Task enqueued: %s (priority=%d, level=%d, queue_size=%d)",
                tid, task["priority"], task["level"], len(self._heap),
            )
        if self._on_push:
            self._on_push()

    async def pop(self) -> Optional[dict]:
        """Remove and return the highest-priority task. Returns None if empty."""
        async with self._lock:
            while self._heap:
                entry = heapq.heappop(self._heap)
                tid = entry.task["task_id"]
                if tid in self._task_ids:
                    self._task_ids.discard(tid)
                    if not self._heap:
                        self._not_empty.clear()
                    return entry.task
            self._not_empty.clear()
            return None

    async def peek(self) -> Optional[dict]:
        """Return the highest-priority task without removing it."""
        async with self._lock:
            if self._heap:
                return self._heap[0].task
            return None

    async def remove(self, task_id: str) -> bool:
        """Remove a specific task from the queue (e.g. on timeout)."""
        async with self._lock:
            if task_id not in self._task_ids:
                return False
            self._task_ids.discard(task_id)
            # Lazy removal: mark as removed; actual cleanup happens on pop
            # For correctness we rebuild the heap without this task
            self._heap = [e for e in self._heap if e.task["task_id"] != task_id]
            heapq.heapify(self._heap)
            if not self._heap:
                self._not_empty.clear()
            return True

    async def load_from_db(self, queued_tasks: list[dict]) -> None:
        """Rebuild queue from persisted QUEUED tasks on startup."""
        async with self._lock:
            self._heap.clear()
            self._task_ids.clear()
            for task in queued_tasks:
                entry = QueueEntry.from_task(task)
                heapq.heappush(self._heap, entry)
                self._task_ids.add(task["task_id"])
            if self._heap:
                self._not_empty.set()
            logger.info("Loaded %d queued tasks from database", len(self._heap))

    @property
    def size(self) -> int:
        return len(self._task_ids)
