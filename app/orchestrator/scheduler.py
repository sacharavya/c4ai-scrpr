"""Scheduler helpers for turning source configs into jobs."""
from __future__ import annotations

import uuid
from typing import Iterable, List

from app.orchestrator.jobs import Job
from app.orchestrator.source_loader import SourceConfig


def plan_jobs(
    *,
    sources: Iterable[SourceConfig],
    entity_type: str,
    limit: int,
) -> List[Job]:
    """Produce crawl jobs for the run constrained by the limit supplied."""
    jobs: List[Job] = []
    for source in sources:
        if entity_type != "all" and source.type != entity_type:
            continue
        job = Job(
            job_id=str(uuid.uuid4()),
            source_id=source.source_id,
            entity_type=source.type,
            url=str(source.base_url),
            metadata={
                "css_rules_path": str(source.css_rules_path),
                "max_qps": source.max_qps,
                "concurrency": source.concurrency,
            },
        )
        jobs.append(job)
        if len(jobs) >= limit:
            break
    return jobs
