from pathlib import Path

from app.parse.jsonld import extract_events_from_jsonld


def test_extract_events_from_jsonld():
    html = Path("tests/fixtures/html/events_list.html").read_text(encoding="utf-8")
    results = extract_events_from_jsonld(html)
    assert any(item["title"] == "Jazz Night" for item in results)
    event = next(item for item in results if item["title"] == "Jazz Night")
    assert event["time_slots"]
    assert event["type"] == "events"
