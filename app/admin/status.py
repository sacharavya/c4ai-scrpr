"""Administrative status helpers."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List


def load_sources(path: Path) -> List[Dict[str, str]]:
    """Read the registered sources from CSV."""
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def summarise_runs(manifest_dir: Path) -> Dict[str, Dict[str, str]]:
    """Summarise manifests written by the scheduler/orchestrator."""
    results: Dict[str, Dict[str, str]] = {}
    manifest_dir.mkdir(parents=True, exist_ok=True)
    for path in manifest_dir.glob("*.json"):
        results[path.stem] = {"path": str(path), "bytes": path.stat().st_size}
    return results
