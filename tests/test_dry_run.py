import json
from pathlib import Path

from app import main as app_main


def _settings(tmp_path: Path) -> dict:
    data_root = tmp_path / "data"
    return {
        "app": {
            "data_root": str(data_root),
            "bronze_dir": str(data_root / "bronze"),
            "silver_dir": str(data_root / "silver"),
            "gold_dir": str(data_root / "gold"),
            "quarantine_dir": str(data_root / "quarantine"),
            "metrics_dir": str(data_root / "metrics"),
        },
        "fetch": {
            "user_agent": "test-agent",
            "timeout_seconds": 10,
            "max_concurrency": 1,
            "max_qps": 1,
        },
        "scheduler": {
            "run_manifest_dir": str(data_root / "manifests"),
            "job_checkpoint_dir": str(data_root / "checkpoints"),
        },
    }


def test_dry_run_lists_jobs(tmp_path, monkeypatch, capsys):
    settings = _settings(tmp_path)
    monkeypatch.setattr(app_main, "load_settings", lambda _path: settings)
    rules = tmp_path / "rules.yaml"
    rules.write_text(
        "selectors:\n  list_item: 'div'\nfields:\n  title: 'h1'\n",
        encoding="utf-8",
    )
    html_path = Path("tests/fixtures/html/events_list.html").resolve()
    csv_path = tmp_path / "sources.csv"
    csv_path.write_text(
        f"source_id,base_url,type,country,robots_ok,sitemap_url,css_rules_path,crawl_freq,max_qps,concurrency,enabled\n"
        f"demo_events,file://{html_path},events,US,true,,{rules},daily,1,1,true\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(app_main, "DEFAULT_SOURCES_CSV", csv_path)

    app_main.main(["crawl", "--dry-run"])
    output = json.loads(capsys.readouterr().out)
    assert len(output) == 1
    assert output[0]["source_id"] == "demo_events"


def test_validate_sources_command(tmp_path, monkeypatch, capsys):
    settings = _settings(tmp_path)
    monkeypatch.setattr(app_main, "load_settings", lambda _path: settings)
    rules = tmp_path / "rules.yaml"
    rules.write_text(
        "selectors:\n  list_item: 'div'\nfields:\n  title: 'h1'\n",
        encoding="utf-8",
    )
    csv_path = tmp_path / "sources.csv"
    csv_path.write_text(
        f"source_id,base_url,type,country,robots_ok,sitemap_url,css_rules_path,crawl_freq,max_qps,concurrency,enabled\n"
        f"good,file://{Path('tests/fixtures/html/events_list.html').resolve()},events,US,true,,{rules},daily,1,1,true\n"
        f"bad,file://{Path('tests/fixtures/html/events_list.html').resolve()},events,US,true,,missing.yaml,daily,1,1,true\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(app_main, "DEFAULT_SOURCES_CSV", csv_path)

    try:
        app_main.main(["validate-sources"])
    except SystemExit as exc:
        assert exc.code == 1
    else:
        assert False, "validate-sources should exit with failure"
    report = json.loads(capsys.readouterr().out)
    statuses = {item["source_id"]: item["status"] for item in report}
    assert statuses["good"] == "OK"
    assert statuses["bad"] == "FAIL"
