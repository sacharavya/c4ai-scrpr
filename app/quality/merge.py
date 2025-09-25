"""Entity merge helpers for silver to gold promotion."""
from __future__ import annotations

from typing import Dict, Tuple


class EntityMerger:
    """Applies simple precedence rules when merging duplicate entities."""

    def merge(self, existing: Dict[str, object], candidate: Dict[str, object]) -> Tuple[Dict[str, object], bool]:
        """Return the merged entity and whether it mutated the record."""
        mutated = False
        for key, value in candidate.items():
            if value in (None, "", [], {}):
                continue
            if existing.get(key) != value:
                existing[key] = value
                mutated = True
        return existing, mutated
