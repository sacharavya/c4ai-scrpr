"""High level fetching primitives with retries and conditional requests."""
from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import httpx
import logging

from app.fetch.robots import RobotsCache
from app.fetch.session import CrawlSession
from app.fetch.snapshot import Snapshot
from app.fetch.etag_cache import ETagCache as ConditionalCache
from app.observability.metrics import MetricsRegistry
from app.observability.tracing import log_fetch_result, log_retry, span

LOGGER = logging.getLogger(__name__)


def _hash_url(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


async def _do_fetch(
    session: CrawlSession,
    url: str,
    *,
    timeout: float,
    headers: Dict[str, str],
    metrics: MetricsRegistry,
) -> httpx.Response:
    delay = 1.0
    for attempt in range(1, 5):
        try:
            with span(name="fetch", url=url):
                start = time.perf_counter()
                response = await session.fetch(url, headers=headers, timeout=timeout)
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            log_fetch_result(
                url=url,
                status=response.status_code,
                bytes_read=len(response.content or b"") if response.content else 0,
                elapsed_ms=elapsed_ms,
            )
            return response
        except httpx.HTTPError as exc:
            metrics.incr("retries")
            log_retry(attempt=attempt, url=url, reason=str(exc))
            if attempt == 4:
                raise
            await asyncio.sleep(delay)
            delay *= 2


async def fetch_document(
    *,
    session: CrawlSession,
    url: str,
    robots: RobotsCache,
    cache: ConditionalCache,
    metrics: MetricsRegistry,
    bronze_root: Path,
    timeout: float = 30.0,
) -> Optional[Snapshot]:
    """Fetch a document respecting robots and conditional GET semantics."""
    if not await robots.allowed(url):
        LOGGER.info("robots_disallow", url=url)
        metrics.incr("robots_disallow")
        return None

    conditional_headers = await cache.headers_for(url)
    response = await _do_fetch(
        session,
        url,
        timeout=timeout,
        headers=conditional_headers,
        metrics=metrics,
    )

    status_bucket = response.status_code // 100
    metrics.incr("pages_fetched")
    metrics.incr(f"http_{status_bucket}xx")

    if response.status_code == httpx.codes.NOT_MODIFIED:
        metrics.incr("unchanged_skips")
        await cache.update(
            url,
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
        )
        return None

    response.raise_for_status()

    html = response.text
    snapshot = Snapshot(
        url=url,
        html=html,
        headers=dict(response.headers),
        fetched_at=datetime.utcnow(),
    )
    rel_dir = bronze_root / _hash_url(url)
    snapshot.save(rel_dir)
    await cache.update(
        url,
        etag=response.headers.get("ETag"),
        last_modified=response.headers.get("Last-Modified"),
    )
    return snapshot
