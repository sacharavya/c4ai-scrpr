"""JSON Schema validation pipeline stage."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import jsonschema
import orjson


@dataclass
class ValidationResult:
    """Outcome of validating a single entity."""

    ok: bool
    errors: List[str]


class SchemaRegistry:
    """Lazily loads JSON Schemas per entity type."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._cache: Dict[str, Dict] = {}
        self._allowed: Dict[str, set[str]] = {}

    def _load(self, entity_type: str) -> Dict:
        if entity_type not in self._cache:
            path = self._root / f"{entity_type.rstrip('s')}.schema.json"
            if not path.exists():
                raise FileNotFoundError(f"Schema not found for {entity_type}: {path}")
            self._cache[entity_type] = orjson.loads(path.read_text(encoding="utf-8"))
            self._allowed[entity_type] = set(self._cache[entity_type].get("properties", {}).keys())
        return self._cache[entity_type]

    def validate(self, entity_type: str, payload: Dict[str, object]) -> ValidationResult:
        schema = self._load(entity_type)
        validator = jsonschema.Draft202012Validator(schema)
        errors = [f"{error.json_path}: {error.message}" for error in validator.iter_errors(payload)]
        return ValidationResult(ok=not errors, errors=errors)

    def prune(self, entity_type: str, payload: Dict[str, object]) -> Dict[str, object]:
        """Return a copy containing only fields permitted by the schema."""
        self._load(entity_type)
        allowed = self._allowed[entity_type]
        return {key: value for key, value in payload.items() if key in allowed}
