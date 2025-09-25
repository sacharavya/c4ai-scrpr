"""Partition-aware writers for the storage layer."""
from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

import orjson

from app.quality.keys import entity_key


def write_silver(entities: Iterable[Dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for entity in entities:
            handle.write(orjson.dumps(entity).decode())
            handle.write("\n")


def write_csv(entities: Iterable[Dict[str, object]], path: Path) -> None:
    rows = list(entities)
    if not rows:
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_sqlite(entities: Iterable[Dict[str, object]], path: Path, entity_type: str) -> None:
    import sqlite3

    rows = list(entities)
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        columns = [
            "source_id TEXT NOT NULL",
            "title TEXT NOT NULL",
            "start TEXT",
            "end TEXT",
            "timezone TEXT",
            "venue_name TEXT NOT NULL",
            "address TEXT NOT NULL",
            "city TEXT NOT NULL",
            "country TEXT",
            "time_slots_json TEXT NOT NULL",
            "price_text TEXT",
            "price_value REAL",
            "organizer TEXT",
            "url TEXT",
            "emails_json TEXT",
            "phones_json TEXT",
            "images_json TEXT",
            "taxonomy_json TEXT",
            "sport_type TEXT",
            "dedup_key TEXT UNIQUE",
        ]
        ddl = f"CREATE TABLE IF NOT EXISTS {entity_type} ({', '.join(columns)})"
        connection.execute(ddl)
        insert_sql = f"""
            INSERT INTO {entity_type} (
                source_id, title, start, end, timezone,
                venue_name, address, city, country, time_slots_json,
                price_text, price_value, organizer, url,
                emails_json, phones_json, images_json, taxonomy_json, sport_type, dedup_key
            ) VALUES (
                :source_id, :title, :start, :end, :timezone,
                :venue_name, :address, :city, :country, :time_slots_json,
                :price_text, :price_value, :organizer, :url,
                :emails_json, :phones_json, :images_json, :taxonomy_json, :sport_type, :dedup_key
            )
            ON CONFLICT(dedup_key) DO UPDATE SET
                start=excluded.start,
                end=excluded.end,
                timezone=excluded.timezone,
                price_text=excluded.price_text,
                price_value=excluded.price_value,
                organizer=excluded.organizer,
                url=excluded.url,
                emails_json=excluded.emails_json,
                phones_json=excluded.phones_json,
                images_json=excluded.images_json,
                taxonomy_json=excluded.taxonomy_json,
                sport_type=excluded.sport_type
        """
        prepared: List[Dict[str, object]] = []
        for entity in rows:
            prepared.append({
                "source_id": entity.get("source_id"),
                "title": entity.get("title"),
                "start": entity.get("start"),
                "end": entity.get("end"),
                "timezone": entity.get("timezone"),
                "venue_name": entity.get("venue_name"),
                "address": entity.get("address"),
                "city": entity.get("city"),
                "country": entity.get("country"),
                "time_slots_json": orjson.dumps(entity.get("time_slots", [])).decode(),
                "price_text": entity.get("price_text"),
                "price_value": entity.get("price_value"),
                "organizer": entity.get("organizer"),
                "url": entity.get("url"),
                "emails_json": orjson.dumps(entity.get("emails", [])).decode(),
                "phones_json": orjson.dumps(entity.get("phones", [])).decode(),
                "images_json": orjson.dumps(entity.get("images", [])).decode(),
                "taxonomy_json": orjson.dumps(entity.get("taxonomy", [])).decode(),
                "sport_type": entity.get("sport_type"),
                "dedup_key": entity_key(entity),
            })
        connection.executemany(insert_sql, prepared)
        connection.commit()
    finally:
        connection.close()


class PartitionWriter:
    """Writes outputs organised by run date partitions."""

    def __init__(self, *, base_gold: Path, base_silver: Path, sqlite_path: Path) -> None:
        self._gold = base_gold
        self._silver = base_silver
        self._sqlite_path = sqlite_path
        for path in (self._gold, self._silver, sqlite_path.parent):
            path.mkdir(parents=True, exist_ok=True)

    def persist(
        self,
        *,
        entity_type: str,
        entities: List[Dict[str, object]],
        run_id: str,
    ) -> Dict[str, Path]:
        if not entities:
            return {}
        date_token = run_id.split("-")[-1][:8]
        run_dt = datetime.strptime(date_token, "%Y%m%d")
        partition = run_dt.strftime("%Y-%m-%d")
        gold_dir = self._gold / partition
        gold_dir.mkdir(parents=True, exist_ok=True)
        gold_path = gold_dir / f"{entity_type}.csv"
        silver_path = self._silver / f"{entity_type}-{run_id}.jsonl"
        write_silver(entities, silver_path)
        write_csv(entities, gold_path)
        write_sqlite(entities, self._sqlite_path, entity_type)
        return {"gold": gold_path, "silver": silver_path, "sqlite": self._sqlite_path}
