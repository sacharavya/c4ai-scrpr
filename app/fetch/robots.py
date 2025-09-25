"""Robots.txt helper utilities."""
from __future__ import annotations

import asyncio
from typing import Dict
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx


class RobotsCache:
    """Caches robots.txt responses per netloc and answers allow checks."""

    def __init__(self, *, user_agent: str, timeout: float = 10.0) -> None:
        self._user_agent = user_agent
        self._timeout = timeout
        self._cache: Dict[str, RobotFileParser] = {}
        self._lock = asyncio.Lock()

    async def allowed(self, url: str) -> bool:
        """Return whether the supplied URL is permitted for the crawler."""
        parsed = urlparse(url)
        if parsed.scheme == "file":
            return True
        key = parsed.netloc
        async with self._lock:
            if key not in self._cache:
                robots_url = urljoin(f"{parsed.scheme}://{parsed.netloc}", "/robots.txt")
                parser = RobotFileParser()
                try:
                    async with httpx.AsyncClient(timeout=self._timeout) as client:
                        response = await client.get(robots_url)
                        if response.status_code >= 400:
                            parser.parse([])
                        else:
                            parser.parse(response.text.splitlines())
                except httpx.HTTPError:
                    parser.parse([])
                self._cache[key] = parser
            parser = self._cache[key]
        return parser.can_fetch(self._user_agent, url)
