"""Structured logging initialisation utilities."""
from __future__ import annotations

import logging
import logging.config
from pathlib import Path
from typing import Any, Dict

import yaml

try:
    import structlog
except ImportError:  # pragma: no cover - structlog optional in tests
    structlog = None


def configure_logging(config_path: Path) -> None:
    """Configure stdlib and structlog logging using the YAML definition."""
    if not config_path.exists() or structlog is None:
        logging.basicConfig(level=logging.INFO)
        if structlog is not None:
            structlog.configure(processors=[structlog.processors.JSONRenderer()])
        return

    with config_path.open("r", encoding="utf-8") as handle:
        config: Dict[str, Any] = yaml.safe_load(handle)
    logging.config.dictConfig(config)
    if structlog is not None:
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.add_log_level,
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(),
            ],
            cache_logger_on_first_use=True,
        )
