"""Deterministic key builders for deduplication and identity."""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from typing import Dict, Iterable, Iterator

from dateutil import parser as dateparser


def _normalise(text: str) -> str:
    return " ".join(text.lower().split())


def _bucket(date_str: str) -> str:
    dt = dateparser.isoparse(date_str)
    bucket = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return bucket.isoformat()


def entity_key(entity: Dict[str, object]) -> str:
    title = _normalise(str(entity.get("title") or ""))
    start = str(entity.get("start") or entity.get("end") or "1970-01-01T00:00:00Z")
    venue = _normalise(str(entity.get("venue_name") or entity.get("address") or ""))
    city = _normalise(str(entity.get("city") or ""))
    source_id = str(entity.get("source_id") or "")
    payload = "|".join([title, _bucket(start), venue, city, source_id])
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def nearby_keys(entity: Dict[str, object]) -> Iterator[str]:
    start = entity.get("start") or entity.get("end")
    if not start:
        return iter(())
    dt = dateparser.isoparse(str(start))
    for delta in (-1, 1):
        shifted = dt + timedelta(days=delta)
        clone = dict(entity)
        clone["start"] = shifted.isoformat()
        yield entity_key(clone)
