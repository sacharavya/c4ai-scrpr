# Runbook

## Overview
The platform ingests event calendars using crawl4ai, validates against strict JSON Schemas, and exports partitioned CSV/SQLite datasets. This runbook documents daily operations, troubleshooting, and recovery procedures.

## Start / Stop
- **Start scheduler loop**
  ```bash
  python -m app.main schedule
  ```
  This launches the asynchronous cron loop defined in `config/settings.toml`. A manifest is written per run to `data/manifests/`.

- **Graceful stop**
  Press `Ctrl+C`. The scheduler writes a final manifest with `exit_code != 0` if interrupted mid-run.

- **One-off crawl**
  ```bash
  python -m app.main crawl --source-id=city_events_demo --limit=30
  ```

## Adding a Source
1. Create a new row in `source_registry/sources.csv` with appropriate `source_id`, base URL, type, concurrency, and QPS caps.
2. Add a rule file under `source_registry/rules/<source_id>.yaml`. Follow `docs/RULES_GUIDE.md`.
3. Drop fixture HTML in `tests/fixtures/html/` and extend parser tests.
4. Run `make lint type test cov`.
5. Deploy by committing changes; the CI workflow enforces linting, typing, and 85% coverage.

## Monitoring & Diagnostics
- **Logs**: Structured JSON logs include `run_id`, `job_id`, `source_id`, and `elapsed_ms`. tail with `jq`.
- **Metrics**: Each run writes `data/metrics/run_<run_id>.json` summarising counters.
- **Admin commands**:
  - `python -m app.admin.cli status`: shows last run results per source.
  - `python -m app.admin.cli inspect-rejects --last 7d`: summarises schema violations.
  - `python -m app.admin.cli explain --url=https://example.org/events/foo`: prints fetch policy, robots decision, and selectors applied.

## Troubleshooting
| Symptom | Possible Causes | Resolution |
| --- | --- | --- |
| `robots_disallow` metric spikes | Source changed robots.txt | Run `python -m app.admin.cli explain --url` to confirm; update source or add exemptions per policy. |
| Entities quarantined for missing fields | Schema tightened; rule missing data | Inspect `data/quarantine/` JSON; adjust rules or fallback to detail page. |
| Scheduler stalled | Check `data/checkpoints/<run_id>.json` for stuck job id | Delete checkpoint to restart run or investigate job-specific failure logs. |
| Duplicate entries in gold | Dedup key missed variance | Update `app/quality/keys.py` to incorporate distinguishing fields; add regression test. |

## Recovery & Resumable Runs
- On crash, rerun the scheduler; it loads the last checkpoint and continues remaining pages.
- Clear checkpoints only when you intentionally wish to restart from scratch.
- Bronze snapshots are immutable; reprocess by rerunning the crawl with `--since` / `--until` parameters to narrow scope.

## Data Exports
- Gold exports are partitioned by execution date: `data/gold/YYYY-MM-DD/events.csv` etc.
- Silver JSONL contains run id in the filename (e.g., `data/silver/events-<run_id>.jsonl`).
- SQLite database `data/gold/events.db` maintains upserted tables for BI tools; ensure nightly vacuum via scheduled maintenance.

