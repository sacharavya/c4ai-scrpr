import asyncio
import json
from pathlib import Path

from app import main as app_main
from app.observability.log import configure_logging
from app.orchestrator.schedule_loop import run_schedule_loop
from app.storage.layout import DataLayout


def test_schedule_loop_produces_manifests(tmp_path, monkeypatch):
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
            "jobs": [
                {"source_type": "events", "cron": "*/5 * * * *", "limit": 1},
            ],
        },
    }
    layout = DataLayout(
        bronze=Path(settings["app"]["bronze_dir"]),
        silver=Path(settings["app"]["silver_dir"]),
        gold=Path(settings["app"]["gold_dir"]),
        manifests=Path(settings["scheduler"]["run_manifest_dir"]),
        checkpoints=Path(settings["scheduler"]["job_checkpoint_dir"]),
        metrics=Path(settings["app"]["metrics_dir"]),
    )
    csv_path = tmp_path / "sources.csv"
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        "selectors:\n  list_item: 'div'\nfields:\n  title: 'h1'\n",
        encoding="utf-8",
    )
    html_path = Path("tests/fixtures/html/events_list.html").resolve()
    csv_path.write_text(
        f"source_id,base_url,type,country,robots_ok,sitemap_url,css_rules_path,crawl_freq,max_qps,concurrency,enabled\n"
        f"demo_events,file://{html_path},events,US,true,,{rules_path},daily,1,1,true\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(app_main, "DEFAULT_SOURCES_CSV", csv_path)
    configure_logging(Path("config/logging.yaml"))
    asyncio.run(run_schedule_loop(settings, layout=layout, interval_seconds=0, ticks=2))
    manifests = list(Path(settings["scheduler"]["run_manifest_dir"]).glob("run-*.json"))
    assert len(manifests) >= 2
    with manifests[0].open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
        assert "source_stats" in payload
