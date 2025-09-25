"""Lightweight in-process metrics suitable for exporting later."""
from __future__ import annotations

import contextlib
import json
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict

LOGGER = logging.getLogger(__name__)


class MetricsRegistry:
    """Holds mutable counters for the current process."""

    def __init__(self) -> None:
        self._counters: Dict[str, int] = defaultdict(int)
        self._register_defaults()

    def _register_defaults(self) -> None:
        defaults = [
            "pages_fetched",
            "http_2xx",
            "http_3xx",
            "http_4xx",
            "http_5xx",
            "retries",
            "unchanged_skips",
            "parse_failures",
            "validates_failed",
            "entities_new",
            "entities_updated",
            "quarantine_rows",
            "duplicates",
            "run_duration_ms",
        ]
        for key in defaults:
            self._counters[key] = 0

    def incr(self, name: str, value: int = 1) -> None:
        """Increment the named counter by the supplied value."""
        self._counters[name] += value

    def get(self, name: str) -> int:
        """Return the current value for the counter, defaulting to zero."""
        return self._counters.get(name, 0)

    def snapshot(self) -> Dict[str, int]:
        """Return a shallow copy of all counters for reporting."""
        return dict(self._counters)

    def export(self, *, path: Path, run_id: str) -> Path:
        """Write counters to a JSON file under the provided directory."""
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "run_id": run_id,
            "counters": self.snapshot(),
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path


@contextlib.contextmanager
def record_duration(registry: MetricsRegistry, metric_name: str):
    """Measure elapsed time for a block and emit it when done."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        registry.incr(metric_name, int(elapsed * 1000))
        LOGGER.info("timer_stop", metric=metric_name, duration_ms=int(elapsed * 1000))
