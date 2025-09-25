"""Disk-backed ETag cache for conditional requests."""
from __future__ import annotations

import time
import asyncio
from pathlib import Path
from typing import Dict, Optional

import orjson

_ETAG_SCHEMA_VERSION = 1


class ETagCache:
    """Persist ETag and Last-Modified responses per URL."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._index: Dict[str, Dict[str, str]] = {}
        if path.exists():
            try:
                payload = orjson.loads(path.read_text(encoding="utf-8"))
            except orjson.JSONDecodeError:
                payload = {}
            if payload.get("version") == _ETAG_SCHEMA_VERSION:
                self._index = payload.get("data", {})
        else:
            path.parent.mkdir(parents=True, exist_ok=True)

    async def headers_for(self, url: str) -> Dict[str, str]:
        entry = self._index.get(url, {})
        headers: Dict[str, str] = {}
        if etag := entry.get("etag"):
            headers["If-None-Match"] = etag
        if last_modified := entry.get("last_modified"):
            headers["If-Modified-Since"] = last_modified
        return headers

    async def update(self, url: str, *, etag: Optional[str], last_modified: Optional[str]) -> None:
        self._index[url] = {
            "etag": etag or "",
            "last_modified": last_modified or "",
            "last_seen": str(int(time.time())),
        }
        await self._persist()

    async def _persist(self) -> None:
        await asyncio.to_thread(self._write_payload)

    def _write_payload(self) -> None:
        payload = {"version": _ETAG_SCHEMA_VERSION, "data": self._index}
        self._path.write_text(orjson.dumps(payload).decode(), encoding="utf-8")
