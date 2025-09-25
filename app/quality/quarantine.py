"""Quarantine handling for rejected entities."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List

import orjson


class Quarantine:
    """Writes invalid entities to a quarantine directory for inspection."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)

    def reject(self, *, entity: Dict[str, object], reason: List[str]) -> Path:
        """Persist the rejected payload with accompanying reasons."""
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
        target = self._root / f"reject_{timestamp}.json"
        blob = {"entity": entity, "reason": reason}
        target.write_text(orjson.dumps(blob, option=orjson.OPT_INDENT_2).decode(), encoding="utf-8")
        return target
