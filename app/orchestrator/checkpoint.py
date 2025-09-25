"""Checkpoint utilities for resumable crawl runs."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import orjson


@dataclass(slots=True)
class JobCheckpoint:
    """Serializable checkpoint data for a single job."""

    job_id: str
    url_cursor: str
    page_idx: int
    discovered_urls_hash: str


def checkpoint_path(root: Path, run_id: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{run_id}.json"


def load_checkpoint(root: Path, run_id: str) -> Optional[JobCheckpoint]:
    path = checkpoint_path(root, run_id)
    if not path.exists():
        return None
    try:
        payload = orjson.loads(path.read_text(encoding="utf-8"))
    except orjson.JSONDecodeError:
        return None
    return JobCheckpoint(**payload)


def save_checkpoint(root: Path, run_id: str, checkpoint: JobCheckpoint) -> Path:
    path = checkpoint_path(root, run_id)
    path.write_text(orjson.dumps(asdict(checkpoint)).decode(), encoding="utf-8")
    return path


def clear_checkpoint(root: Path, run_id: str) -> None:
    path = checkpoint_path(root, run_id)
    if path.exists():
        path.unlink()
