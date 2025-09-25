"""Path helpers for bronze/silver/gold storage layout."""
from __future__ import annotations

from pathlib import Path


class DataLayout:
    """Computes structured output paths inside the data root."""

    def __init__(
        self,
        *,
        bronze: Path,
        silver: Path,
        gold: Path,
        manifests: Path,
        checkpoints: Path,
        metrics: Path,
    ) -> None:
        self.bronze = bronze
        self.silver = silver
        self.gold = gold
        self.manifests = manifests
        self.checkpoints = checkpoints
        self.metrics = metrics
        for path in (bronze, silver, gold, manifests, checkpoints, metrics):
            path.mkdir(parents=True, exist_ok=True)

    def gold_sqlite(self) -> Path:
        """Return the SQLite file path shared by all entity types."""
        return self.gold / "events.db"
