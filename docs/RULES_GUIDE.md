# Selector & Pagination Rule Guide

This guide explains how to author CSS/XPath rule files used by the platform. The YAML rule format mirrors what the extractor expects in `app/parse/rules.py`.

## Structure
```yaml
selectors:
  list_item: "ul.events > li"
fields:
  title: "h3"
  start: "time.start@datetime|text"
  detail_url: "a@href"
pagination:
  next_selector: "a.next@href"
  max_pages: 5
date_scopes:
  timezone: "America/Toronto"
```

- **selectors.list_item** points to the repeated container node.
- **fields** specifies mappings to individual attributes. Suffixes determine behaviour:
  - `@attr` extracts an attribute.
  - `@attr[]` collects attribute values from all matches.
  - `|text` appended after `@datetime` or `@date` hints the parser to fall back to text when no attribute is present.
- **pagination** instructs how to discover further pages. Supported keys:
  - `next_selector`: CSS selector returning an `<a>` for *rel=next* style pagination.
  - `numbered_links`: use when recurrence sits in paginated nav lists; provide `selector` and optional `stop_after`.
  - `month_grid: true`: walk month calendars discovered via `.active + a` semantics; the parser stops at `max_pages` or when dates exceed the configured fences.
- **date_scopes.timezone** enforces a timezone during normalisation.

## Best Practices
- Prefer absolute selectors anchored on semantic classes or `data-*` attributes.
- Emit ISO-8601 datetimes whenever possible (`YYYY-MM-DDTHH:MM:SS±HH`).
- Include `country` and `city` in each rule set; schema validation requires them.
- Provide `detail_url` when event detail pages contain richer JSON-LD for later enrichment.
- When pagination is date-driven (e.g., a month grid), set `max_pages` to a safe bound (e.g., `3`) and rely on the scheduler to revisit the source frequently.

## Testing
- Place representative HTML under `tests/fixtures/html/` and write unit tests in `tests/test_parse_rules.py` or new files.
- Use `make test` to validate extraction and downstream schema enforcement before adding a new rule to production.

