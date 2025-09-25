"""Pagination discovery primitives."""
from __future__ import annotations

from typing import List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup


def _parse_selector(selector: str) -> tuple[str, Optional[str]]:
    expr = selector.strip().replace(" @", "@")
    attr = None
    if expr.endswith("[]"):
        expr = expr[:-2]
    if "@" in expr:
        expr, attr = expr.split("@", 1)
    elif "::attr(" in expr:
        expr, attr = expr.split("::attr(", 1)
        attr = attr.rstrip(")")
    return expr.strip(), attr.strip() if attr else attr


def discover_next_urls(
    html: str,
    base_url: str,
    *,
    selector: Optional[str],
    max_pages: int,
    month_grid: bool = False,
) -> List[str]:
    if max_pages <= 1:
        return []
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []
    if selector:
        css, attr = _parse_selector(selector)
        elements = soup.select(css)
        for element in elements:
            href = element.get(attr or "href") if attr or element.has_attr("href") else None
            if href:
                urls.append(urljoin(base_url, href))
                break
    if month_grid:
        month_links = soup.select("a[rel='next'], a.month-next")
        for element in month_links:
            href = element.get("href")
            if href:
                urls.append(urljoin(base_url, href))
        urls = urls[: max_pages - 1]
    return urls[: max_pages - 1]
