"""Geocoding stub implementation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class GeoPoint:
    """Represents a resolved coordinate pair."""

    latitude: float
    longitude: float


class GeoResolver:
    """Interface placeholder for plugging external geocoding services."""

    def resolve(self, entity: Dict[str, object]) -> Optional[GeoPoint]:
        """Return coordinates for the supplied entity or None when unavailable."""
        return None
