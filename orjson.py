"""Lightweight fallback implementation of a subset of orjson."""
from __future__ import annotations

import json
from typing import Any

OPT_INDENT_2 = "OPT_INDENT_2"


def dumps(obj: Any, option: str | None = None) -> bytes:
    indent = 2 if option == OPT_INDENT_2 else None
    return json.dumps(obj, indent=indent).encode("utf-8")


def loads(data: str | bytes) -> Any:
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    return json.loads(data)
