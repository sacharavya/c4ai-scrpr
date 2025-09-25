"""Field level normalisation helpers."""
from __future__ import annotations

import re
from datetime import timedelta, timezone
from typing import Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from dateutil import parser as dateparser

_PHONE_RE = re.compile(r"[+\d][\d\-().\s]{4,}")
_EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_PRICE_RE = re.compile(r"(\d+)(?:[.,](\d{2}))?")


def _resolve_timezone(name: Optional[str]):
    if not name:
        return None
    try:
        return ZoneInfo(name)
    except Exception:
        if name.startswith("UTC") and len(name) >= 6:
            sign = 1 if name[3] == "+" else -1
            hours = int(name[4:6])
            minutes = int(name[7:9]) if len(name) >= 9 else 0
            return timezone(sign * timedelta(hours=hours, minutes=minutes))
    return None


def _convert_datetime(value: str, timezone_hint: Optional[str]):
    dt = dateparser.isoparse(value)
    if dt.tzinfo is None:
        resolved = _resolve_timezone(timezone_hint)
        if resolved is not None:
            dt = dt.replace(tzinfo=resolved)
        else:
            dt = dt.replace(tzinfo=dateparser.tz.UTC)
    if timezone_hint:
        resolved = _resolve_timezone(timezone_hint)
        if resolved is not None:
            dt = dt.astimezone(resolved)
    return dt


def normalize_datetimes(entity: Dict[str, object]) -> None:
    """Ensure start/end/time_slots carry ISO-8601 timestamps with tzinfo."""
    timezone = entity.get("timezone") if isinstance(entity.get("timezone"), str) else None
    if start := entity.get("start"):
        dt = _convert_datetime(str(start), timezone)
        entity["start"] = dt.isoformat()
        if timezone is None and dt.tzinfo is not None:
            tzname = getattr(dt.tzinfo, "key", None) or dt.tzname()
            if not tzname and dt.utcoffset() is not None:
                total_minutes = int(dt.utcoffset().total_seconds() // 60)
                hours, minutes = divmod(abs(total_minutes), 60)
                sign = "+" if total_minutes >= 0 else "-"
                tzname = f"UTC{sign}{hours:02d}:{minutes:02d}"
            entity["timezone"] = tzname
            timezone = tzname
    if end := entity.get("end"):
        dt = _convert_datetime(str(end), timezone)
        entity["end"] = dt.isoformat()
    slots = entity.get("time_slots")
    if isinstance(slots, list):
        normalised: List[Dict[str, str]] = []
        for slot in slots:
            if isinstance(slot, dict):
                start_val = slot.get("start")
                end_val = slot.get("end")
                if start_val and end_val:
                    start_dt = _convert_datetime(str(start_val), timezone)
                    end_dt = _convert_datetime(str(end_val), timezone)
                    normalised.append({
                        "start": start_dt.isoformat(),
                        "end": end_dt.isoformat(),
                    })
        entity["time_slots"] = normalised


def normalise_contacts(entity: Dict[str, object]) -> None:
    """Extract phones/emails from free form text fields."""
    pool = " ".join(
        str(part)
        for part in [entity.get("price_text"), entity.get("organizer"), entity.get("address"), entity.get("title")]
        if part
    )
    entity["emails"] = sorted({match.lower() for match in _EMAIL_RE.findall(pool)})
    entity["phones"] = sorted({re.sub(r"[^+\d]", "", match) for match in _PHONE_RE.findall(pool)})


def price_to_number(entity: Dict[str, object]) -> None:
    """Derive a numeric price when obvious and place it under `price_value`."""
    price_text = entity.get("price_text")
    if not isinstance(price_text, str):
        return
    match = _PRICE_RE.search(price_text)
    if not match:
        return
    major = int(match.group(1))
    minor = int(match.group(2)) if match.group(2) else 0
    entity["price_value"] = major + minor / 100


def normalise_urls(entity: Dict[str, object]) -> None:
    """Deduplicate image URLs and normalise the primary url field."""
    url = entity.get("url")
    if isinstance(url, str):
        entity["url"] = url.strip()
    images = entity.get("images")
    if isinstance(images, list):
        seen = []
        for item in images:
            if isinstance(item, str):
                cleaned = item.strip()
                if cleaned and cleaned not in seen:
                    seen.append(cleaned)
        entity["images"] = seen
