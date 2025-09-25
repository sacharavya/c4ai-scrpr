"""Utilities for loading crawl sources from the registry CSV."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, List, Tuple

from pydantic import BaseModel, Field, HttpUrl, ValidationError, field_validator


class SourceConfig(BaseModel):
    """Validated configuration for a single crawl source."""

    source_id: str = Field(min_length=1)
    base_url: HttpUrl | str
    type: str = Field(pattern=r"^(events|festivals|sports)$")
    country: str = Field(pattern=r"^[A-Z]{2}$")
    robots_ok: bool = True
    sitemap_url: HttpUrl | str | None = None
    css_rules_path: Path
    crawl_freq: str = Field(pattern=r"^(daily|weekly|monthly)$")
    max_qps: float = Field(gt=0)
    concurrency: int = Field(gt=0)
    enabled: bool = True

    @field_validator("css_rules_path", mode="before")
    @classmethod
    def _resolve_path(cls, value: str | Path) -> Path:
        if isinstance(value, Path):
            return value
        return Path(value)

    def ensure_rules_exist(self) -> None:
        if not self.css_rules_path.exists():
            raise FileNotFoundError(f"Rule file not found: {self.css_rules_path}")


def _coerce_bool(value: str | bool | None, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _coerce_float(value: str | float | int | None, default: float = 1.0) -> float:
    if value in (None, ""):
        return default
    return float(value)


def _coerce_int(value: str | int | None, default: int = 1) -> int:
    if value in (None, ""):
        return default
    return int(value)


def _prepare_row(row: dict[str, str], base_dir: Path) -> dict[str, object]:
    mapped: dict[str, object] = {}
    for key, value in row.items():
        mapped[key.strip()] = value.strip() if isinstance(value, str) else value
    mapped["robots_ok"] = _coerce_bool(mapped.get("robots_ok"), default=True)
    mapped["enabled"] = _coerce_bool(mapped.get("enabled"), default=True)
    mapped["max_qps"] = _coerce_float(mapped.get("max_qps"), default=1.0)
    mapped["concurrency"] = _coerce_int(mapped.get("concurrency"), default=1)
    css_path = mapped.get("css_rules_path", "")
    mapped["css_rules_path"] = (base_dir / str(css_path)).resolve()
    if mapped.get("sitemap_url") in {"", None}:
        mapped["sitemap_url"] = None
    return mapped


def load_sources(csv_path: Path) -> List[SourceConfig]:
    """Load enabled sources from the registry CSV, validating each row."""
    configs: List[SourceConfig] = []
    base_dir = csv_path.parent
    with csv_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            if not raw or not raw.get("source_id"):
                continue
            prepared = _prepare_row(raw, base_dir)
            try:
                config = SourceConfig(**prepared)
                if config.enabled:
                    config.ensure_rules_exist()
            except (ValidationError, FileNotFoundError, ValueError) as exc:
                raise ValueError(f"Invalid source row {prepared.get('source_id')}: {exc}") from exc
            if not config.enabled:
                continue
            configs.append(config)
    return configs


def validate_sources(csv_path: Path) -> List[Tuple[str, bool, str]]:
    """Validate all rows, returning results per source without raising."""
    results: List[Tuple[str, bool, str]] = []
    base_dir = csv_path.parent
    with csv_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            if not raw or not raw.get("source_id"):
                continue
            prepared = _prepare_row(raw, base_dir)
            source_id = str(prepared.get("source_id"))
            try:
                config = SourceConfig(**prepared)
                if config.enabled:
                    config.ensure_rules_exist()
            except (ValidationError, FileNotFoundError, ValueError) as exc:
                results.append((source_id, False, str(exc)))
            else:
                status = "disabled" if not config.enabled else "ok"
                results.append((source_id, True, status))
    return results
