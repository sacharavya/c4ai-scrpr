"""Rule-based fallback parsers for HTML listings."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml
from bs4 import BeautifulSoup


@dataclass
class RuleSpec:
    """Declarative instructions for parsing list pages with CSS selectors."""

    list_item: str
    fields: Dict[str, str]
    pagination_next: Optional[str]
    pagination_month_grid: bool
    pagination_max_pages: int
    timezone: Optional[str]


def load_rule(path: Path) -> RuleSpec:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    selectors = data.get("selectors", {})
    pagination = data.get("pagination", {})
    date_scopes = data.get("date_scopes", {})
    return RuleSpec(
        list_item=selectors.get("list_item", "body"),
        fields=data.get("fields", {}),
        pagination_next=pagination.get("next_selector"),
        pagination_month_grid=bool(pagination.get("month_grid", False)),
        pagination_max_pages=int(pagination.get("max_pages", 1)),
        timezone=date_scopes.get("timezone"),
    )


def _parse_expression(expression: str) -> Tuple[str, Optional[str], bool, bool]:
    expr = expression.strip()
    multi = False
    if expr.endswith("[]"):
        multi = True
        expr = expr[:-2]
    text_fallback = False
    if expr.endswith("|text"):
        text_fallback = True
        expr = expr[:-5]
    expr = expr.replace(" @", "@")
    if "@" in expr:
        selector, attr = expr.split("@", 1)
        return selector.strip(), attr.strip(), multi, text_fallback
    if "::attr(" in expr:
        selector, attr = expr.split("::attr(", 1)
        return selector.strip(), attr.rstrip(")"), multi, text_fallback
    return expr.strip(), None, multi, text_fallback


def _value_from_element(element, attr: Optional[str], text_fallback: bool) -> Optional[str]:
    if attr:
        value = element.get(attr)
        if value:
            return value.strip()
        if text_fallback:
            text = element.get_text(strip=True)
            return text or None
        return None
    text = element.get_text(strip=True)
    return text or None


def extract_with_rules(html: str, spec: RuleSpec) -> List[Dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select(spec.list_item)
    results: List[Dict[str, object]] = []
    for item in items:
        payload: Dict[str, object] = {}
        for field, expression in spec.fields.items():
            selector, attr, multi, text_fallback = _parse_expression(expression)
            if multi:
                values: List[str] = []
                for element in item.select(selector):
                    value = _value_from_element(element, attr, text_fallback)
                    if value:
                        values.append(value)
                payload[field] = values
            elif field == "time_slots":
                slots: List[str] = []
                for element in item.select(selector):
                    value = _value_from_element(element, attr, text_fallback)
                    if value:
                        slots.append(value)
                payload[field] = slots
            else:
                element = item.select_one(selector)
                if element is None:
                    payload[field] = None
                else:
                    payload[field] = _value_from_element(element, attr, text_fallback)
        if spec.timezone and "timezone" not in payload:
            payload["timezone"] = spec.timezone
        results.append(payload)
    return results
