"""Definitions for crawl jobs and their lifecycle."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional


@dataclass
class Job:
    """Represents a queued crawl job associated with a source."""

    job_id: str
    source_id: str
    entity_type: str
    url: str
    attempts: int = 0
    max_attempts: int = 3
    status: str = "pending"
    last_error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, object] = field(default_factory=dict)

    def mark_started(self) -> None:
        """Transition the job into the in-progress state."""
        self.status = "in_progress"
        self.attempts += 1

    def mark_succeeded(self) -> None:
        """Mark the job as successfully completed."""
        self.status = "succeeded"

    def mark_failed(self, error: Exception) -> None:
        """Record a failure and capture the error message."""
        self.status = "failed" if self.attempts >= self.max_attempts else "retry"
        self.last_error = str(error)

    def should_retry(self) -> bool:
        """Return True when the job is eligible for another attempt."""
        return self.status == "retry" and self.attempts < self.max_attempts
