"""Storage writer façade built on partitioned exports."""
from __future__ import annotations

from typing import Dict, Iterable, List

from app.storage.layout import DataLayout
from app.storage.partition import PartitionWriter, write_csv, write_silver, write_sqlite

__all__ = [
    "StorageWriter",
    "write_silver",
    "write_csv",
    "write_sqlite",
]


class StorageWriter:
    """Coordinates writing entities to partitioned outputs and SQLite."""

    def __init__(self, layout: DataLayout) -> None:
        self._layout = layout
        self._partition = PartitionWriter(
            base_gold=layout.gold,
            base_silver=layout.silver,
            sqlite_path=layout.gold_sqlite(),
        )

    def persist(self, *, entity_type: str, entities: List[Dict[str, object]], run_id: str) -> Dict[str, object]:
        """Persist the supplied entities to silver JSONL, gold CSV and SQLite."""
        return self._partition.persist(entity_type=entity_type, entities=entities, run_id=run_id)
