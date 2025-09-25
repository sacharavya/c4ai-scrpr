"""Tracing helpers for fetch and parse stages."""
from __future__ import annotations

import contextlib
import logging
import time
from typing import Iterator, Optional

try:
    import structlog
    from structlog.contextvars import bind_contextvars, clear_contextvars
except ImportError:  # pragma: no cover - fallback when structlog missing
    structlog = None  # type: ignore

def _logger() -> logging.Logger:
    return logging.getLogger("app.trace")


def set_context(*, run_id: str, job_id: str, source_id: str) -> None:
    if structlog is not None:
        bind_contextvars(run_id=run_id, job_id=job_id, source_id=source_id)
    _logger().debug("trace_context", run_id=run_id, job_id=job_id, source_id=source_id)


def clear_context() -> None:
    if structlog is not None:
        clear_contextvars()


@contextlib.contextmanager
def span(*, name: str, url: Optional[str] = None) -> Iterator[None]:
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        _logger().info("trace_span", span=name, url=url, elapsed_ms=elapsed_ms)


def log_retry(attempt: int, *, url: str, reason: str) -> None:
    _logger().warning("fetch_retry", attempt=attempt, url=url, reason=reason)


def log_fetch_result(*, url: str, status: int, bytes_read: int, elapsed_ms: int) -> None:
    _logger().info(
        "fetch_result",
        url=url,
        status=status,
        bytes=bytes_read,
        elapsed_ms=elapsed_ms,
    )
