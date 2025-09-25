"""Taxonomy helpers for mapping source categories to canonical labels."""
from __future__ import annotations

from typing import Dict

CATEGORY_MAP = {
    "jazz": "music",
    "art": "art",
    "football": "football",
    "running": "running",
}


def map_taxonomy(entity: Dict[str, object]) -> None:
    """Attach canonical taxonomy fields when source hints exist."""
    if not isinstance(entity.get("taxonomy"), list):
        entity["taxonomy"] = []
    title = str(entity.get("title") or "").lower()
    for key, value in CATEGORY_MAP.items():
        if key in title:
            if value not in entity["taxonomy"]:
                entity["taxonomy"].append(value)
    if entity.get("type") == "sports" and entity.get("sport_type"):
        sport_value = str(entity["sport_type"]).lower()
        if sport_value not in entity["taxonomy"]:
            entity["taxonomy"].append(sport_value)
