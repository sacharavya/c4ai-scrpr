"""Deduplication utilities for crawl outputs."""
from __future__ import annotations

from typing import Dict

from app.quality.keys import entity_key, nearby_keys


class Deduplicator:
    """Keeps track of seen entities to avoid duplicates."""

    def __init__(self) -> None:
        self._seen: Dict[str, Dict[str, object]] = {}

    def key_for(self, entity: Dict[str, object]) -> str:
        """Compute the canonical deduplication key for the entity."""
        return entity_key(entity)

    def is_duplicate(self, entity: Dict[str, object]) -> bool:
        """Return True when the entity matches or is near a known dedup key."""
        primary = self.key_for(entity)
        if primary in self._seen:
            return True
        for alt_key in nearby_keys(entity):
            if alt_key in self._seen:
                return True
        return False

    def remember(self, entity: Dict[str, object]) -> None:
        """Record the entity's dedup key to prevent future duplicates."""
        key = self.key_for(entity)
        self._seen[key] = dict(entity)
