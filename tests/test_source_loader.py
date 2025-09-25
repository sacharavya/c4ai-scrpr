from pathlib import Path

import pytest

from app.orchestrator.source_loader import SourceConfig, load_sources, validate_sources


def _write_rule(path: Path) -> None:
    path.write_text("selectors:\n  list_item: 'div'\nfields:\n  title: 'h1'\n", encoding="utf-8")


def test_load_sources_skips_disabled(tmp_path):
    rules_a = tmp_path / "rules_a.yaml"
    rules_b = tmp_path / "rules_b.yaml"
    _write_rule(rules_a)
    _write_rule(rules_b)
    csv_path = tmp_path / "sources.csv"
    csv_path.write_text(
        """source_id,base_url,type,country,robots_ok,sitemap_url,css_rules_path,crawl_freq,max_qps,concurrency,enabled
active,file://events.html,events,US,true,,rules_a.yaml,daily,2,3,true
inactive,file://skip.html,events,US,true,,missing.yaml,daily,1,1,false
fest,file://fest.html,festivals,CA,true,,rules_b.yaml,weekly,0.5,1,true
""",
        encoding="utf-8",
    )

    sources = load_sources(csv_path)
    ids = [src.source_id for src in sources]
    assert ids == ["active", "fest"]
    assert all(isinstance(src, SourceConfig) for src in sources)

    with pytest.raises(ValueError):
        bad_csv = tmp_path / "bad.csv"
        bad_csv.write_text(
            """source_id,base_url,type,country,robots_ok,sitemap_url,css_rules_path,crawl_freq,max_qps,concurrency,enabled
broken,file://x.html,events,US,true,,missing.yaml,daily,1,1,true
""",
            encoding="utf-8",
        )
        load_sources(bad_csv)


def test_validate_sources_reports_failures(tmp_path):
    rules = tmp_path / "rules.yaml"
    _write_rule(rules)
    csv_path = tmp_path / "sources.csv"
    csv_path.write_text(
        """source_id,base_url,type,country,robots_ok,sitemap_url,css_rules_path,crawl_freq,max_qps,concurrency,enabled
ok,file://events.html,events,US,true,,rules.yaml,daily,1,1,true
bad,file://bad.html,events,US,true,,missing.yaml,daily,1,1,true
""",
        encoding="utf-8",
    )

    results = validate_sources(csv_path)
    mapping = {sid: (ok, detail) for sid, ok, detail in results}
    assert mapping["ok"] == (True, "ok")
    assert not mapping["bad"][0]
    assert "missing" in mapping["bad"][1]
