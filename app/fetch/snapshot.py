"""Representations of fetched documents saved to bronze storage."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional


@dataclass(slots=True)
class Snapshot:
    """Represents a fetched page along with metadata for caching."""

    url: str
    html: str
    headers: Dict[str, str]
    fetched_at: datetime
    path: Optional[Path] = None

    def save(self, root: Path) -> Path:
        """Persist the snapshot HTML and headers under the bronze area."""
        root.mkdir(parents=True, exist_ok=True)
        target = root / f"{self.fetched_at.strftime('%Y%m%dT%H%M%S')}.html"
        target.write_text(self.html, encoding="utf-8")
        meta_path = target.with_suffix(".headers.json")
        import orjson

        meta_path.write_text(orjson.dumps({"url": self.url, "headers": self.headers}).decode(), encoding="utf-8")
        self.path = target
        return target
