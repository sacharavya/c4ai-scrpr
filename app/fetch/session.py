"""Factories for crawl4ai-backed fetch sessions."""
from __future__ import annotations

import contextlib
from pathlib import Path
from typing import AsyncIterator, Dict, Optional

import httpx
from urllib.parse import urlparse

try:
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
except ImportError:  # pragma: no cover - fallback when crawl4ai version differs
    AsyncWebCrawler = None  # type: ignore
    CrawlerRunConfig = None  # type: ignore


class CrawlSession:
    """Unified abstraction over crawl4ai or httpx fallback."""

    def __init__(self, crawler: Optional[AsyncWebCrawler], client: Optional[httpx.AsyncClient]) -> None:
        self._crawler = crawler
        self._client = client

    async def fetch(self, url: str, *, headers: Optional[Dict[str, str]] = None, timeout: float = 30.0) -> httpx.Response:
        """Fetch a URL returning an HTTPX-like response object."""
        parsed = urlparse(url)
        if parsed.scheme == "file":
            location = (parsed.netloc + parsed.path) or parsed.path
            target = Path(location)
            if not target.is_absolute():
                target = Path.cwd() / target
            html = target.read_text(encoding="utf-8")
            return httpx.Response(200, text=html, request=httpx.Request("GET", url))
        if self._crawler is not None and CrawlerRunConfig is not None:
            config = CrawlerRunConfig(headers=headers or {}, timeout=timeout)
            result = await self._crawler.arun(url, config=config)
            response = httpx.Response(
                status_code=result.status_code,
                headers=result.headers,
                text=result.content,
                request=httpx.Request("GET", url, headers=headers),
            )
            return response
        if self._client is None:
            raise RuntimeError("No crawl session available")
        return await self._client.get(url, headers=headers, timeout=timeout)


@contextlib.asynccontextmanager
async def create_crawl_session(*, user_agent: str, timeout: float, max_connections: int) -> AsyncIterator[CrawlSession]:
    """Yield a configured `CrawlSession` for the duration of the context."""
    headers = {"User-Agent": user_agent}
    if AsyncWebCrawler is not None:
        crawler = AsyncWebCrawler()
        await crawler.__aenter__()
        try:
            yield CrawlSession(crawler, None)
        finally:
            await crawler.__aexit__(None, None, None)
    else:
        limits = httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_connections)
        async with httpx.AsyncClient(headers=headers, limits=limits, timeout=timeout) as client:
            yield CrawlSession(None, client)
