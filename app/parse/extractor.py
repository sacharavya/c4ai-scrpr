"""Entity extraction orchestrator combining JSON-LD and rules."""
from __future__ import annotations

from typing import Dict, Iterable, List

from app.parse.jsonld import extract_events_from_jsonld
from app.parse.rules import RuleSpec, extract_with_rules


def _slots_from_strings(raw_slots: Iterable[str]) -> List[Dict[str, str]]:
    slots: List[Dict[str, str]] = []
    for raw in raw_slots:
        if "|" in raw:
            start, end = raw.split("|", 1)
        elif "-" in raw:
            start, end = raw.split("-", 1)
        else:
            start = end = raw
        slots.append({"start": start.strip(), "end": end.strip()})
    return slots


def extract_entities(
    *,
    html: str,
    source_id: str,
    entity_type: str,
    rule_spec: RuleSpec,
) -> List[Dict[str, object]]:
    """Extract structured entities preferring JSON-LD then rule fallbacks."""
    results = []
    for candidate in extract_events_from_jsonld(html):
        if candidate["type"] != entity_type:
            continue
        candidate["source_id"] = source_id
        results.append(candidate)

    fallback_items = extract_with_rules(html, rule_spec)
    for item in fallback_items:
        payload: Dict[str, object] = {
            "type": entity_type,
            "source_id": source_id,
            "title": item.get("title"),
            "start": item.get("start"),
            "end": item.get("end"),
            "timezone": item.get("timezone"),
            "venue_name": item.get("venue_name") or item.get("venue"),
            "address": item.get("address") or item.get("addr"),
            "city": item.get("city"),
            "country": item.get("country"),
            "time_slots": _slots_from_strings(item.get("time_slots", []) if isinstance(item.get("time_slots"), list) else []),
            "price_text": item.get("price_text"),
            "organizer": item.get("organizer"),
            "url": item.get("detail_url"),
            "images": item.get("images", []),
        }
        if entity_type == "sports":
            payload["sport_type"] = item.get("sport_type")
        if rule_spec.timezone and not payload.get("timezone"):
            payload["timezone"] = rule_spec.timezone
        results.append(payload)
    return results
