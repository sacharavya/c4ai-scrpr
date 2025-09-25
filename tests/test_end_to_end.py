import asyncio
import json
from datetime import datetime
from types import SimpleNamespace
from pathlib import Path

from app import main as app_main


def test_end_to_end(tmp_path, monkeypatch):
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
            "max_concurrency": 2,
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
    html_path = Path("tests/fixtures/html/events_list.html").resolve()
    csv_path.write_text(
        f"source_id,base_url,type,country,robots_ok,sitemap_url,css_rules_path,crawl_freq,max_qps,concurrency,enabled\n"
        f"demo_events,file://{html_path},events,US,true,,{rules_path},daily,1,1,true\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(app_main, "DEFAULT_SOURCES_CSV", csv_path)
    args = SimpleNamespace(
        type="events",
        limit=1,
        source_id="demo_events",
        concurrency=1,
        qps=1,
        timeout=10,
        since=None,
        until=None,
    )
    asyncio.run(app_main.run_crawl(args, settings))
    from datetime import datetime

    manifest = next((data_root / "manifests").glob("run-*.json"))
    run_id = json.loads(manifest.read_text(encoding="utf-8"))["run_id"]
    partition_name = datetime.strptime(run_id[:15], "%Y%m%dT%H%M%S").strftime("%Y-%m-%d")
    partitions = list((data_root / "gold" / partition_name).glob("events.csv"))
    assert partitions, "expected partitioned events.csv output"
