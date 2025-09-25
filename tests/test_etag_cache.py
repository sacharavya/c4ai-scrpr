import asyncio
from pathlib import Path

import httpx

from app.fetch.etag_cache import ETagCache
from app.fetch.fetcher import fetch_document
from app.fetch.robots import RobotsCache
from app.fetch.session import CrawlSession
from app.observability.metrics import MetricsRegistry


class SequenceSession(CrawlSession):
    def __init__(self, responses):
        super().__init__(crawler=None, client=None)
        self._responses = list(responses)

    async def fetch(self, url: str, *, headers=None, timeout: float = 30.0) -> httpx.Response:  # type: ignore[override]
        status, body = self._responses.pop(0)
        if status == 304:
            return httpx.Response(304, headers={"ETag": "abc"}, request=httpx.Request("GET", url))
        return httpx.Response(
            status,
            text=body,
            headers={"ETag": "abc", "Last-Modified": "Wed"},
            request=httpx.Request("GET", url),
        )


def test_etag_cache_prevents_refetch(tmp_path):
    html_path = tmp_path / "page.html"
    html_path.write_text("<html><body>hello</body></html>", encoding="utf-8")
    robots = RobotsCache(user_agent="test-bot")
    cache = ETagCache(tmp_path / "etag.json")
    metrics = MetricsRegistry()
    session = SequenceSession([
        (200, html_path.read_text(encoding="utf-8")),
        (304, ""),
    ])

    async def _run():
        snapshot = await fetch_document(
            session=session,
            url=f"file://{html_path}",
            robots=robots,
            cache=cache,
            metrics=metrics,
            bronze_root=tmp_path / "bronze",
        )
        assert snapshot is not None
        second = await fetch_document(
            session=session,
            url=f"file://{html_path}",
            robots=robots,
            cache=cache,
            metrics=metrics,
            bronze_root=tmp_path / "bronze",
        )
        assert second is None
        assert metrics.get("unchanged_skips") == 1
        assert metrics.get("http_3xx") == 1

    asyncio.run(_run())
