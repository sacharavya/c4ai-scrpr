"""Queue implementations for scheduling crawl jobs."""
from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator, List

import orjson

from app.orchestrator.jobs import Job


class JobQueue:
    """In-memory queue with JSONL persistence for crash recovery."""

    def __init__(self, *, path: Path) -> None:
        self._path = path
        self._queue: asyncio.Queue[Job] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._load_from_disk()

    def _load_from_disk(self) -> None:
        if not self._path.exists():
            self._path.parent.mkdir(parents=True, exist_ok=True)
            return
        for line in self._path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            data = orjson.loads(line)
            if "created_at" in data and isinstance(data["created_at"], str):
                data["created_at"] = datetime.fromisoformat(data["created_at"])
            job = Job(**data)
            self._queue.put_nowait(job)

    async def _persist(self) -> None:
        async with self._lock:
            items: List[Job] = list(self._queue._queue)  # type: ignore[attr-defined]
            with self._path.open("w", encoding="utf-8") as handle:
                for job in items:
                    payload = asdict(job)
                    for key, value in list(payload.items()):
                        if isinstance(value, datetime):
                            payload[key] = value.isoformat()
                    handle.write(orjson.dumps(payload).decode())
                    handle.write("\n")

    async def clear(self) -> None:
        """Remove all jobs from the queue and truncate the persistence file."""
        async with self._lock:
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                    self._queue.task_done()
                except asyncio.QueueEmpty:
                    break
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text("", encoding="utf-8")

    async def enqueue(self, job: Job) -> None:
        """Add a job to the queue and persist the updated state."""
        await self._queue.put(job)
        await self._persist()

    async def dequeue(self) -> Job:
        """Obtain the next job, blocking until one is available."""
        job = await self._queue.get()
        await self._persist()
        return job

    def empty(self) -> bool:
        """Return True when no jobs remain."""
        return self._queue.empty()

    async def task_done(self) -> None:
        """Mark the current task as complete and persist state."""
        self._queue.task_done()
        await self._persist()
