from pathlib import Path

from app.parse.rules import extract_with_rules, load_rule


def test_extract_with_rules():
    rule = load_rule(Path("source_registry/rules/demo_events.yaml"))
    html = Path("tests/fixtures/html/events_list.html").read_text(encoding="utf-8")
    results = extract_with_rules(html, rule)
    assert results
    first = results[0]
    assert first["title"] == "Art Expo"
    assert len(first["time_slots"]) == 2
