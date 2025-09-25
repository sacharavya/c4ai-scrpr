"""Parsers for Schema.org JSON-LD payloads."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

import orjson
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from pydantic import ValidationError

from app.storage.models import Event, Festival, SportsEvent

MODEL_MAP = {
    "events": Event,
    "festivals": Festival,
    "sports": SportsEvent,
}


def _flatten_graph(payload: object) -> Iterable[Dict[str, object]]:
    if isinstance(payload, dict):
        if "@graph" in payload and isinstance(payload["@graph"], list):
            for node in payload["@graph"]:
                if isinstance(node, dict):
                    yield from _flatten_graph(node)
        elif "@list" in payload and isinstance(payload["@list"], list):
            for node in payload["@list"]:
                if isinstance(node, dict):
                    yield node
        else:
            yield payload
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield from _flatten_graph(item)


def _first_str(value: object) -> Optional[str]:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.strip():
                return item.strip()
    return None


def _parse_time_slot(event: Dict[str, object]) -> Optional[Dict[str, str]]:
    start = _first_str(event.get("startDate"))
    end = _first_str(event.get("endDate"))
    if not start or not end:
        return None
    return {
        "start": dateparser.isoparse(start).isoformat(),
        "end": dateparser.isoparse(end).isoformat(),
    }


def _address_fields(location: object) -> Dict[str, Optional[str]]:
    venue_name = None
    address = None
    city = None
    country = None
    if isinstance(location, dict):
        venue_name = _first_str(location.get("name"))
        addr_obj = location.get("address")
        if isinstance(addr_obj, dict):
            address = _first_str(
                addr_obj.get("streetAddress")
                or addr_obj.get("street")
                or addr_obj.get("addressStreet")
            )
            city = _first_str(addr_obj.get("addressLocality") or addr_obj.get("city"))
            country = _first_str(addr_obj.get("addressCountry"))
    return {
        "venue_name": venue_name,
        "address": address,
        "city": city,
        "country": country,
    }


def _normalised_type(type_value: object) -> Optional[str]:
    if isinstance(type_value, list):
        for item in type_value:
            normalised = _normalised_type(item)
            if normalised:
                return normalised
        return None
    if isinstance(type_value, str):
        lowered = type_value.lower()
        if lowered in {"event", "music event", "eventseries"}:
            return "events"
        if lowered in {"festival"}:
            return "festivals"
        if lowered in {"sportsevent", "sports event"}:
            return "sports"
    return None


def _images(raw: object) -> List[str]:
    if isinstance(raw, list):
        return [item.strip() for item in raw if isinstance(item, str) and item.strip()]
    if isinstance(raw, str) and raw.strip():
        return [raw.strip()]
    return []


def _offers_price(offers: object) -> Optional[str]:
    if isinstance(offers, dict):
        return _first_str(offers.get("price"))
    if isinstance(offers, list):
        for offer in offers:
            if isinstance(offer, dict):
                price = _first_str(offer.get("price"))
                if price:
                    return price
    return None


def _time_slots(node: Dict[str, object]) -> List[Dict[str, str]]:
    slots: List[Dict[str, str]] = []
    top_slot = _parse_time_slot(node)
    if top_slot:
        slots.append(top_slot)
    sub_events = node.get("subEvent")
    if isinstance(sub_events, list):
        for sub in sub_events:
            if isinstance(sub, dict):
                slot = _parse_time_slot(sub)
                if slot:
                    slots.append(slot)
    return slots


def _base_payload(node: Dict[str, object], mapped_type: str) -> Dict[str, object]:
    address_fields = _address_fields(node.get("location"))
    payload: Dict[str, object] = {
        "type": mapped_type,
        "source_id": "",
        "title": _first_str(node.get("name")) or "",
        "start": (_parse_time_slot(node) or {}).get("start"),
        "end": (_parse_time_slot(node) or {}).get("end"),
        "timezone": _first_str(node.get("eventTimeZone")),
        "venue_name": address_fields.get("venue_name") or "",
        "address": address_fields.get("address") or "",
        "city": address_fields.get("city") or "",
        "country": address_fields.get("country") or "",
        "time_slots": _time_slots(node) or [],
        "price_text": _offers_price(node.get("offers")),
        "organizer": _first_str(node.get("organizer")) or None,
        "url": _first_str(node.get("url")) or None,
        "images": _images(node.get("image")),
    }
    if mapped_type == "sports":
        payload["sport_type"] = _first_str(node.get("sport")) or _first_str(node.get("sportType")) or ""
    return payload


def _validate_payload(mapped_type: str, payload: Dict[str, object]) -> Dict[str, object]:
    model = MODEL_MAP[mapped_type]
    try:
        instance = model.model_validate(payload)
        data = instance.model_dump(mode="json")
    except ValidationError:
        # fall back to partial payload when required keys missing
        data = payload
    data["type"] = mapped_type
    return data


def extract_events_from_jsonld(html: str) -> List[Dict[str, object]]:
    """Extract candidate entities from JSON-LD blobs embedded in HTML."""
    soup = BeautifulSoup(html, "html.parser")
    results: List[Dict[str, object]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = orjson.loads(script.string or "{}")
        except orjson.JSONDecodeError:
            continue
        for node in _flatten_graph(data):
            mapped_type = _normalised_type(node.get("@type"))
            if not mapped_type:
                continue
            payload = _base_payload(node, mapped_type)
            results.append(_validate_payload(mapped_type, payload))
    return results
