import asyncio
from pathlib import Path

import httpx

from app.fetch.fetcher import fetch_document
from app.fetch.etag_cache import ETagCache
from app.fetch.robots import RobotsCache
from app.fetch.session import CrawlSession
from app.observability.metrics import MetricsRegistry


class DummySession(CrawlSession):
    def __init__(self):
        super().__init__(crawler=None, client=None)

    async def fetch(self, url: str, *, headers=None, timeout: float = 30.0) -> httpx.Response:  # type: ignore[override]
        target = Path(url.replace("file://", ""))
        html = target.read_text(encoding="utf-8")
        response = httpx.Response(200, text=html, headers={"ETag": "abc"}, request=httpx.Request("GET", url))
        return response


def test_conditional_cache_roundtrip(tmp_path):
    async def _run():
        cache = ETagCache(tmp_path / "state.json")
        await cache.update("https://example.com", etag="abc", last_modified="Wed")
        headers = await cache.headers_for("https://example.com")
        assert headers["If-None-Match"] == "abc"
        assert headers["If-Modified-Since"] == "Wed"

    asyncio.run(_run())


def test_fetch_document_file_scheme(tmp_path):
    async def _run():
        robots = RobotsCache(user_agent="test-bot")
        cache = ETagCache(tmp_path / "state.json")
        metrics = MetricsRegistry()

        html_path = tmp_path / "page.html"
        html_path.write_text("<html><body>ok</body></html>", encoding="utf-8")

        session = DummySession()
        snapshot = await fetch_document(
            session=session,
            url=f"file://{html_path}",
            robots=robots,
            cache=cache,
            metrics=metrics,
            bronze_root=tmp_path / "bronze",
        )
        assert snapshot is not None
        assert snapshot.html.startswith("<html")
        assert metrics.get("pages_fetched") == 1

    asyncio.run(_run())
