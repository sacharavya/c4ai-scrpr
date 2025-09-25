"""Cron-like scheduler loop built on asyncio."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List, Optional

try:
    from croniter import croniter
except ImportError:  # pragma: no cover - fallback for tests without croniter
    def croniter(expr: str, start_time: datetime):  # type: ignore
        class _Cron:
            def get_next(self, cls=None):
                return start_time

        return _Cron()

from app.storage.layout import DataLayout


@dataclass
class ScheduledJob:
    source_type: str
    cron: str
    limit: int

    @classmethod
    def from_config(cls, payload: Dict[str, object]) -> "ScheduledJob":
        return cls(
            source_type=str(payload.get("source_type", "events")),
            cron=str(payload.get("cron", "*/30 * * * *")),
            limit=int(payload.get("limit", 100)),
        )


def _prepare_jobs(settings: Dict[str, object]) -> List[ScheduledJob]:
    jobs_cfg = settings.get("scheduler", {}).get("jobs", [])
    return [ScheduledJob.from_config(item) for item in jobs_cfg]


def _resolve_run_id(source_type: str, checkpoints: Path) -> str:
    existing = sorted(checkpoints.glob(f"{source_type}-*.json"))
    if existing:
        return existing[0].stem
    return f"{source_type}-{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}"


async def run_schedule_loop(
    settings: Dict[str, object],
    *,
    layout: DataLayout,
    interval_seconds: int = 60,
    ticks: Optional[int] = None,
) -> None:
    from app.main import run_crawl

    jobs = _prepare_jobs(settings)
    if not jobs:
        return
    tick = 0
    while ticks is None or tick < ticks:
        now = datetime.utcnow()
        for job in jobs:
            schedule = croniter(job.cron, now)
            _ = schedule.get_next(datetime)
            run_id = _resolve_run_id(job.source_type, layout.checkpoints)
            args = SimpleNamespace(
                type=job.source_type,
                limit=job.limit,
                source_id="all",
                concurrency=settings["fetch"]["max_concurrency"],
                qps=settings["fetch"]["max_qps"],
                timeout=settings["fetch"]["timeout_seconds"],
                since=None,
                until=None,
                run_id=run_id,
                dry_run=False,
            )
            await run_crawl(args, settings)
        tick += 1
        if ticks is None or tick < ticks:
            await asyncio.sleep(interval_seconds)
