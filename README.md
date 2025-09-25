# crawl4ai Event Platform

Production-ready, modular web data platform for harvesting events, festivals, and sports data using crawl4ai. Phase 2 adds CI gates, partitioned exports, resumable scheduling, and admin diagnostics while keeping the stack portable across CSV and SQLite (Postgres ready).

## Features
- Source registry driven scheduling with per-source QPS/concurrency caps and resumable checkpoints
- All crawl jobs are configured exclusively via `source_registry/sources.csv`
- crawl4ai-powered fetching with conditional ETag cache, robots.txt enforcement, tracing, and bronze snapshots
- JSON-LD fast path with CSS/XPath fallback parsing, month-grid pagination, and Schema.org hardening
- Normalisation for datetimes, contact signals, taxonomy tagging, geo stubs, and price extraction
- Strict JSON Schema validation, deterministic dedup keys, idempotent SQLite upserts, and quarantine for violations
- Silver JSONL per run and gold CSV/SQLite partitioned by execution date (`data/gold/YYYY-MM-DD/`) with manifests and metrics snapshots
- Structured JSON logging, request tracing, and admin CLI for status, reject inspection, and explainability
- CI workflow enforcing `ruff`, `black`, `mypy`, and coverage ≥ 85%

## Getting Started
1. **Environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -e .[dev]
   cp .env.example .env
   ```
2. **Seed sources (optional demo data)**
   ```bash
   python -m app.main seed-sources
   ```
3. **Run a crawl**
   ```bash
   python -m app.main crawl --type events --limit 5 --source-id=demo_events
   ```
4. **Inspect outputs**
   - Bronze snapshots: `data/bronze/`
   - Silver JSONL: `data/silver/{type}-{run_id}.jsonl`
   - Gold CSV partitions: `data/gold/YYYY-MM-DD/{events|festivals|sports}.csv`
   - SQLite BI store: `data/gold/events.db`
   - Metrics: `data/metrics/run_<run_id>.json`
   - Quarantine: `data/quarantine/`

### Makefile shortcuts

```bash
make venv      # create venv and install deps
make lint      # ruff + black
make type      # mypy
make test      # pytest suite
make cov       # pytest coverage (fails < 85%)
make run       # demo crawl
```

## CLI Commands
- `python -m app.main crawl --type events --limit 100 --concurrency 5`
- `python -m app.main crawl --type sports --source-id demo_sports`
- `python -m app.main schedule --ticks 2 --interval 5`
- `python -m app.main seed-sources`
- `python -m app.admin.cli status`
- `python -m app.admin.cli inspect-rejects --last 7 --source-id city_events_demo`
- `python -m app.admin.cli explain --url=https://example.org/events`

Common flags: `--qps`, `--timeout`, `--since`, `--until`.

## Configuration
- `config/settings.toml` holds fetch, layout, and scheduler defaults (`run_manifest_dir`, `job_checkpoint_dir`)
- `.env` / environment variables override user agent, timeouts, and data directories
- `source_registry/sources.csv` plus YAML rules per source govern extraction strategies
- `docs/RULES_GUIDE.md` documents the selector language and pagination hints
- `docs/RUNBOOK.md` covers operations, troubleshooting, and recovery flows

## Tests
```bash
pytest -q
```

## Docker
Build and run using the fixtures bundle:
```bash
docker build -t c4ai-scrpr .
docker-compose up
```
The compose service mounts `./data` so CSV/SQLite artefacts persist to the host.

## Data Flow
```
source_registry → scheduler → queue → fetch (crawl4ai) → parse (JSON-LD/rules)
  → normalise → validate/dedup → silver JSONL → gold CSV + SQLite partitions
```
Bronze stores raw snapshots, Silver stores normalised JSONL per run, Gold stores merged partitioned CSV + SQLite tables with manifests and metrics per run.

## Project Layout
- `app/orchestrator`: scheduler, queue, job state machine
- `app/fetch`: crawl4ai sessions, robots cache, conditional fetching, snapshots
- `app/parse`: JSON-LD parsing and CSS/XPath fallback extraction
- `app/normalize`: datetime, contact, geo stubs, and taxonomy mapping
- `app/quality`: validation, deduplication, merging, quarantine
- `app/storage`: pydantic models, partition writers, SQLite helpers
- `app/observability`: logging setup, tracing, metrics registry
- `app/admin`: status/inspection CLI utilities
- `tests/`: fixtures plus coverage for fetch, parse, scheduling, checkpoints, partitions, admin CLI

## Sample Output
See `data/gold/YYYY-MM-DD/events.csv`, `data/silver/events-<run_id>.jsonl`, and `data/metrics/run_<run_id>.json` produced from the bundled fixtures for reference.
