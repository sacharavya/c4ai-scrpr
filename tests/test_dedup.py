from app.quality.dedup import Deduplicator


def test_deduplicator_near_duplicate():
    dedup = Deduplicator()
    entity = {
        "title": "Jazz Night",
        "start": "2024-07-01T20:00:00-04:00",
        "end": "2024-07-01T22:00:00-04:00",
        "venue_name": "Club",
        "address": "123",
        "city": "NYC",
        "source_id": "demo",
    }
    assert not dedup.is_duplicate(entity)
    dedup.remember(entity)
    shifted = dict(entity)
    shifted["start"] = "2024-07-02T20:00:00-04:00"
    assert dedup.is_duplicate(shifted)
