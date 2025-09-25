"""Pydantic models for canonical entities."""
from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field, HttpUrl


class TimeSlot(BaseModel):
    """Represents a contiguous window for an event."""

    start: str = Field(..., description="ISO-8601 start datetime")
    end: str = Field(..., description="ISO-8601 end datetime")


class BaseEntity(BaseModel):
    """Shared attributes across all entity types."""

    source_id: str
    title: str
    venue_name: str
    address: str
    city: str
    country: str
    time_slots: List[TimeSlot]
    timezone: Optional[str] = None
    start: Optional[str] = None
    end: Optional[str] = None
    price_text: Optional[str] = None
    price_value: Optional[float] = None
    organizer: Optional[str] = None
    url: Optional[HttpUrl] = None
    emails: Optional[List[str]] = None
    phones: Optional[List[str]] = None
    images: Optional[List[HttpUrl]] = None
    taxonomy: Optional[List[str]] = None


class Event(BaseEntity):
    """Canonical representation for events."""


class Festival(BaseEntity):
    """Canonical representation for festivals."""


class SportsEvent(BaseEntity):
    """Canonical representation for sports events."""

    sport_type: str
