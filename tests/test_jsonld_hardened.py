from pathlib import Path

from app.parse.jsonld import extract_events_from_jsonld


def test_jsonld_handles_graph_list():
    html = Path("tests/fixtures/html/sports_club_demo.html").read_text(encoding="utf-8")
    results = extract_events_from_jsonld(html)
    sports = [item for item in results if item["type"] == "sports"]
    assert sports
    first = sports[0]
    assert first["title"] == "Morning Swim"
    assert first["time_slots"]
    assert first["venue_name"] == "Community Pool"
