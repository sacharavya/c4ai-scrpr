import asyncio
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from app import main as app_main
from app.admin import cli


@pytest.fixture()
def prepared_run(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    settings = {
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
    csv_path = tmp_path / "sources.csv"
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        "selectors:\n  list_item: 'div'\nfields:\n  title: 'h1'\n",
        encoding="utf-8",
    )
    html_path = Path("tests/fixtures/html/city_events_demo.html").resolve()
    base_url = f"file://{html_path}"
    csv_path.write_text(
        f"source_id,base_url,type,country,robots_ok,sitemap_url,css_rules_path,crawl_freq,max_qps,concurrency,enabled\n"
        f"city_events_demo,{base_url},events,CA,true,,{rules_path},daily,1,1,true\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(app_main, "DEFAULT_SOURCES_CSV", csv_path)
    args = SimpleNamespace(
        type="events",
        limit=1,
        source_id="city_events_demo",
        concurrency=1,
        qps=1,
        timeout=10,
        since=None,
        until=None,
    )
    asyncio.run(app_main.run_crawl(args, settings))
    # add synthetic reject
    reject_dir = Path(settings["app"]["quarantine_dir"])
    reject_dir.mkdir(parents=True, exist_ok=True)
    sample_reject = {
        "entity": {"source_id": "city_events_demo"},
        "reason": ["missing timezone"],
    }
    filename = f"reject_{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}.json"
    (reject_dir / filename).write_text(json.dumps(sample_reject), encoding="utf-8")
    return settings, str(csv_path), base_url


def test_admin_status(prepared_run, capsys):
    settings, csv_path, _ = prepared_run
    args = cli.build_parser().parse_args([
        "status",
        "--sources",
        csv_path,
        "--manifests",
        settings["scheduler"]["run_manifest_dir"],
    ])
    cli.cmd_status(args)
    output = json.loads(capsys.readouterr().out)
    assert any(row["source_id"] == "city_events_demo" for row in output)


def test_admin_inspect_rejects(prepared_run, capsys):
    settings, csv_path, _ = prepared_run
    args = cli.build_parser().parse_args([
        "inspect-rejects",
        "--quarantine",
        settings["app"]["quarantine_dir"],
        "--source-id",
        "city_events_demo",
        "--last",
        "30",
    ])
    cli.cmd_rejects(args)
    output = json.loads(capsys.readouterr().out)
    assert output["missing timezone"] >= 1


def test_admin_explain(prepared_run, capsys):
    settings, csv_path, base_url = prepared_run
    args = cli.build_parser().parse_args([
        "explain",
        "--url",
        base_url,
        "--sources",
        csv_path,
    ])
    cli.cmd_explain(args)
    output = json.loads(capsys.readouterr().out)
    assert output["matched"] is True
