from pathlib import Path

from app.quality.validate import SchemaRegistry


def test_schema_validation():
    registry = SchemaRegistry(Path("config/schemas"))
    entity = {
        "source_id": "demo",
        "title": "Sample Event",
        "start": "2024-07-01T20:00:00-04:00",
        "end": "2024-07-01T22:00:00-04:00",
        "timezone": "America/New_York",
        "venue_name": "Venue",
        "address": "123 Street",
        "city": "New York",
        "country": "US",
        "time_slots": [{"start": "2024-07-01T20:00:00-04:00", "end": "2024-07-01T22:00:00-04:00"}],
    }
    result = registry.validate("events", entity)
    assert result.ok
