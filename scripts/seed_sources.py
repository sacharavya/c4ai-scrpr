#!/usr/bin/env python
"""Populate the source registry with built-in demo sources."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from dotenv import load_dotenv

DEMO_SOURCES = [
    {
        "source_id": "demo_events",
        "base_url": "https://demo.example.com/events",
        "type": "events",
        "country": "US",
        "robots_ok": "true",
        "sitemap_url": "",
        "css_rules_path": "source_registry/rules/demo_events.yaml",
        "crawl_freq": "daily",
        "max_qps": "2"
    },
    {
        "source_id": "demo_festivals",
        "base_url": "https://demo.example.com/festivals",
        "type": "festivals",
        "country": "US",
        "robots_ok": "true",
        "sitemap_url": "",
        "css_rules_path": "source_registry/rules/demo_festivals.yaml",
        "crawl_freq": "weekly",
        "max_qps": "1"
    }
]


def seed_sources(path: Path) -> None:
    """Write demo rows to the source registry CSV if it is empty."""
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "source_id",
                "base_url",
                "type",
                "country",
                "robots_ok",
                "sitemap_url",
                "css_rules_path",
                "crawl_freq",
                "max_qps",
            ],
        )
        if not exists:
            writer.writeheader()
        for row in DEMO_SOURCES:
            writer.writerow(row)


def main() -> None:
    """CLI entrypoint used by `python -m app.main seed-sources`."""
    load_dotenv()
    parser = argparse.ArgumentParser(description="Seed the source registry with demo sources")
    parser.add_argument(
        "--path",
        type=Path,
        default=Path("source_registry/sources.csv"),
        help="Path to the source registry CSV",
    )
    args = parser.parse_args()
    seed_sources(args.path)


if __name__ == "__main__":
    main()
