"""Command-line entrypoints for the crawl4ai data platform."""
from __future__ import annotations

import argparse
import asyncio
import json
import hashlib
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import tomllib
from dotenv import load_dotenv

try:
    import uvloop
except ImportError:  # pragma: no cover - uvloop optional on some platforms
    uvloop = None

from app.admin.status import load_sources as admin_load_sources, summarise_runs
from app.fetch.fetcher import ConditionalCache, fetch_document
from app.fetch.robots import RobotsCache
from app.fetch.session import create_crawl_session
from app.normalize.fields import normalise_contacts, normalise_urls, normalize_datetimes, price_to_number
from app.normalize.taxonomy import map_taxonomy
from app.observability.log import configure_logging
from app.observability.metrics import MetricsRegistry, record_duration
from app.observability.tracing import clear_context, set_context
from app.orchestrator.jobs import Job
from app.orchestrator.queue import JobQueue
from app.orchestrator.scheduler import plan_jobs
from app.orchestrator.source_loader import load_sources as load_source_configs, validate_sources as validate_source_registry
from app.orchestrator.checkpoint import (
    JobCheckpoint,
    clear_checkpoint as remove_checkpoint,
    load_checkpoint,
    save_checkpoint,
)
from app.parse.extractor import extract_entities
from app.parse.pagination import discover_next_urls
from app.parse.rules import RuleSpec, load_rule
from app.quality.dedup import Deduplicator
from app.quality.merge import EntityMerger
from app.quality.quarantine import Quarantine
from app.quality.validate import SchemaRegistry
from app.storage.layout import DataLayout
from app.storage.writers import StorageWriter
from app.orchestrator.schedule_loop import run_schedule_loop

DEFAULT_SOURCES_CSV = Path("source_registry/sources.csv")


def load_settings(path: Path) -> Dict[str, object]:
    """Read the TOML configuration file."""
    with path.open("rb") as handle:
        return tomllib.load(handle)


def build_arg_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(prog="c4ai-scrpr", description="crawl4ai data platform")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("seed-sources", help="Populate the source registry with demo rows")

    crawl = sub.add_parser("crawl", help="Execute a crawl run")
    crawl.add_argument("--type", default="events", help="Entity type to crawl (events|festivals|sports|all)")
    crawl.add_argument("--limit", type=int, default=100, help="Maximum number of jobs to run")
    crawl.add_argument("--source-id", default="all", help="Filter to a particular source ID")
    crawl.add_argument("--concurrency", type=int, default=3, help="Maximum concurrent jobs")
    crawl.add_argument("--qps", type=int, default=2, help="Global requests per second budget")
    crawl.add_argument("--timeout", type=int, default=30, help="Per-request timeout in seconds")
    crawl.add_argument("--since", help="Optional lower bound ISO date")
    crawl.add_argument("--until", help="Optional upper bound ISO date")
    crawl.add_argument("--dry-run", action="store_true", help="Print planned jobs without executing")

    schedule = sub.add_parser("schedule", help="Run the scheduler loop")
    schedule.add_argument("--ticks", type=int, help="Number of iterations to execute")
    schedule.add_argument("--interval", type=int, default=60, help="Seconds between ticks")

    status = sub.add_parser("status", help="Summarise sources and manifests")
    status.add_argument("--sources", default="source_registry/sources.csv", help="Path to sources CSV")
    status.add_argument("--manifests", default="data/manifests", help="Manifest directory")

    sub.add_parser("validate-sources", help="Validate sources.csv and associated rule files")

    return parser


async def _process_job(
    *,
    job: Job,
    session,
    robots: RobotsCache,
    conditional_cache: ConditionalCache,
    rule_spec: RuleSpec,
    entity_type: str,
    layout: DataLayout,
    metrics: MetricsRegistry,
    schema_registry: SchemaRegistry,
    deduplicator: Deduplicator,
    quarantine: Quarantine,
    merger: EntityMerger,
    results: Dict[str, List[Dict[str, object]]],
    results_index: Dict[str, Dict[str, Dict[str, object]]],
    timeout: float,
    run_id: str,
    checkpoint_state: Optional[JobCheckpoint],
    source_stats: Dict[str, Dict[str, int]],
) -> None:
    """Process a single job through fetch, parse, normalise, validate and dedup stages."""
    job.mark_started()
    set_context(run_id=run_id, job_id=job.job_id, source_id=job.source_id)
    stats = source_stats.setdefault(
        job.source_id,
        {"rows_new": 0, "rows_updated": 0, "rejects": 0},
    )
    snapshot = await fetch_document(
        session=session,
        url=job.url,
        robots=robots,
        cache=conditional_cache,
        metrics=metrics,
        bronze_root=layout.bronze,
        timeout=timeout,
    )
    if snapshot is None:
        job.mark_succeeded()
        return

    try:
        pages = [snapshot]
        # Basic pagination discovery
        extra_urls = discover_next_urls(
            snapshot.html,
            job.url,
            selector=rule_spec.pagination_next,
            max_pages=rule_spec.pagination_max_pages,
            month_grid=rule_spec.pagination_month_grid,
        )
        for url in extra_urls:
            extra = await fetch_document(
                session=session,
                url=url,
                robots=robots,
                cache=conditional_cache,
                metrics=metrics,
                bronze_root=layout.bronze,
                timeout=timeout,
            )
            if extra:
                pages.append(extra)

        discovered_urls = [job.url] + extra_urls
        discovered_hash = hashlib.sha1("|".join(sorted(discovered_urls)).encode("utf-8")).hexdigest()
        start_page = 0
        if checkpoint_state and checkpoint_state.job_id == job.job_id and checkpoint_state.discovered_urls_hash == discovered_hash:
            start_page = checkpoint_state.page_idx + 1

        for idx, page in enumerate(pages):
            if idx < start_page:
                continue
            try:
                extracted = extract_entities(
                    html=page.html,
                    source_id=job.source_id,
                    entity_type=entity_type if entity_type != "all" else job.entity_type,
                    rule_spec=rule_spec,
                )
            except Exception:  # pragma: no cover - defensive
                metrics.incr("parse_failures")
                continue
            for entity in extracted:
                normalize_datetimes(entity)
                normalise_contacts(entity)
                price_to_number(entity)
                normalise_urls(entity)
                map_taxonomy(entity)
                entity_type_key = entity["type"]
                clean_entity = schema_registry.prune(entity_type_key, entity)
                if "timezone" not in clean_entity or not clean_entity["timezone"]:
                    clean_entity["timezone"] = entity.get("timezone")
                clean_entity = {k: v for k, v in clean_entity.items() if v is not None}
                validation = schema_registry.validate(entity_type_key, clean_entity)
                if not validation.ok:
                    quarantine.reject(entity=entity, reason=validation.errors)
                    metrics.incr("validates_failed")
                    metrics.incr("quarantine_rows")
                    stats["rejects"] += 1
                    continue
                if deduplicator.is_duplicate(clean_entity):
                    metrics.incr("duplicates")
                    continue
                key = deduplicator.key_for(clean_entity)
                deduplicator.remember(clean_entity)
                existing = results_index[entity_type_key].get(key)
                if existing:
                    merged, mutated = merger.merge(existing, clean_entity)
                    results_index[entity_type_key][key] = merged
                    if mutated:
                        metrics.incr("entities_updated")
                        stats["rows_updated"] += 1
                else:
                    results_index[entity_type_key][key] = clean_entity
                    results[entity_type_key].append(clean_entity)
                    metrics.incr("entities_new")
                    stats["rows_new"] += 1
                checkpoint = JobCheckpoint(
                    job_id=job.job_id,
                    url_cursor=page.url,
                    page_idx=idx,
                    discovered_urls_hash=discovered_hash,
                )
                save_checkpoint(layout.checkpoints, run_id, checkpoint)
        job.mark_succeeded()
        remove_checkpoint(layout.checkpoints, run_id)
    finally:
        clear_context()


async def run_crawl(args: argparse.Namespace, settings: Dict[str, object]) -> None:
    """Execute the crawl command end-to-end."""
    data_root = Path(settings["app"]["data_root"])

    try:
        sources = load_source_configs(DEFAULT_SOURCES_CSV)
    except ValueError as exc:
        raise SystemExit(f"Failed to load sources: {exc}")

    if args.source_id != "all":
        sources = [source for source in sources if source.source_id == args.source_id]

    if not sources:
        print("No matching sources found")
        return

    planned_jobs = plan_jobs(sources=sources, entity_type=args.type, limit=args.limit)

    if getattr(args, "dry_run", False):
        summary = [
            {
                "job_id": job.job_id,
                "source_id": job.source_id,
                "type": job.entity_type,
                "url": job.url,
                "css_rules_path": job.metadata.get("css_rules_path"),
            }
            for job in planned_jobs
        ]
        print(json.dumps(summary, indent=2))
        return

    if not planned_jobs:
        print("No jobs to execute after filtering")
        return

    data_root.mkdir(parents=True, exist_ok=True)
    layout = DataLayout(
        bronze=Path(settings["app"]["bronze_dir"]),
        silver=Path(settings["app"]["silver_dir"]),
        gold=Path(settings["app"]["gold_dir"]),
        manifests=Path(settings["scheduler"]["run_manifest_dir"]),
        checkpoints=Path(settings["scheduler"]["job_checkpoint_dir"]),
        metrics=Path(settings["app"]["metrics_dir"]),
    )
    metrics = MetricsRegistry()
    schema_registry = SchemaRegistry(Path("config/schemas"))
    deduplicator = Deduplicator()
    merger = EntityMerger()
    quarantine = Quarantine(Path(settings["app"]["quarantine_dir"]))
    storage_writer = StorageWriter(layout)

    queue = JobQueue(path=data_root / "queue" / f"jobs-{args.source_id}.jsonl")
    await queue.clear()
    for job in planned_jobs:
        await queue.enqueue(job)

    run_id = getattr(args, "run_id", datetime.utcnow().strftime("%Y%m%dT%H%M%S"))
    results: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    results_index: Dict[str, Dict[str, Dict[str, object]]] = defaultdict(dict)
    source_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"rows_new": 0, "rows_updated": 0, "rejects": 0})
    conditional_cache = ConditionalCache(layout.bronze / "conditional.json")
    robots = RobotsCache(user_agent=settings["fetch"]["user_agent"], timeout=5.0)

    with record_duration(metrics, "run_duration_ms"):
        async with create_crawl_session(
            user_agent=settings["fetch"]["user_agent"],
            timeout=settings["fetch"]["timeout_seconds"],
            max_connections=args.concurrency,
        ) as session:
            async def worker() -> None:
                while True:
                    try:
                        job = await asyncio.wait_for(queue.dequeue(), timeout=0.1)
                    except asyncio.TimeoutError:
                        if queue.empty():
                            break
                        continue
                try:
                    rule_spec = load_rule(Path(job.metadata["css_rules_path"]))
                    checkpoint_state = load_checkpoint(layout.checkpoints, run_id)
                    await _process_job(
                        job=job,
                        session=session,
                        robots=robots,
                        conditional_cache=conditional_cache,
                        rule_spec=rule_spec,
                        entity_type=args.type,
                        layout=layout,
                        metrics=metrics,
                        schema_registry=schema_registry,
                        deduplicator=deduplicator,
                        quarantine=quarantine,
                        merger=merger,
                        results=results,
                        results_index=results_index,
                        timeout=args.timeout,
                        run_id=run_id,
                        checkpoint_state=checkpoint_state,
                        source_stats=source_stats,
                    )
                except Exception as error:  # pragma: no cover - defensive logging
                    job.mark_failed(error)
                    if job.should_retry():
                        await queue.enqueue(job)
                finally:
                    await queue.task_done()

            workers = [asyncio.create_task(worker()) for _ in range(args.concurrency)]
            await asyncio.gather(*workers)

    manifest_records = {}
    for entity_type, entities in results.items():
        manifest_records[entity_type] = storage_writer.persist(
            entity_type=entity_type,
            entities=entities,
            run_id=run_id,
        )

    manifest_dir = layout.manifests
    manifest = manifest_dir / f"run-{run_id}.json"
    manifest.write_text(
        json.dumps({
            "run_id": run_id,
            "counts": {key: len(value) for key, value in results.items()},
            "paths": {
                etype: {kind: str(path) for kind, path in artifacts.items()}
                for etype, artifacts in manifest_records.items()
            },
            "source_stats": {key: dict(value) for key, value in source_stats.items()},
            "metrics": metrics.snapshot(),
            "exit_code": 0,
        }, indent=2),
        encoding="utf-8",
    )
    metrics.export(path=layout.metrics / f"run_{run_id}.json", run_id=run_id)


def main(argv: Optional[List[str]] = None) -> None:
    """Entry point for the CLI."""
    load_dotenv()
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    settings = load_settings(Path("config/settings.toml"))
    configure_logging(Path("config/logging.yaml"))

    if uvloop is not None:
        uvloop.install()

    if args.command == "seed-sources":
        from scripts.seed_sources import seed_sources

        seed_sources(Path("source_registry/sources.csv"))
        return

    if args.command == "schedule":
        layout = DataLayout(
            bronze=Path(settings["app"]["bronze_dir"]),
            silver=Path(settings["app"]["silver_dir"]),
            gold=Path(settings["app"]["gold_dir"]),
            manifests=Path(settings["scheduler"]["run_manifest_dir"]),
            checkpoints=Path(settings["scheduler"]["job_checkpoint_dir"]),
            metrics=Path(settings["app"]["metrics_dir"]),
        )
        asyncio.run(
            run_schedule_loop(
                settings,
                layout=layout,
                interval_seconds=getattr(args, "interval", 60),
                ticks=getattr(args, "ticks", None),
            )
        )
        return

    if args.command == "validate-sources":
        results = validate_source_registry(DEFAULT_SOURCES_CSV)
        report = []
        success = True
        for source_id, ok, detail in results:
            status = "OK"
            if detail == "disabled":
                status = "DISABLED"
            elif not ok:
                status = "FAIL"
                success = False
            report.append({
                "source_id": source_id,
                "status": status,
                "detail": detail if status != "OK" else "",
            })
        print(json.dumps(report, indent=2))
        if not success:
            raise SystemExit(1)
        return

    if args.command == "status":
        sources = admin_load_sources(Path(args.sources))
        manifests = summarise_runs(Path(args.manifests))
        print(json.dumps({"sources": sources, "manifests": manifests}, indent=2))
        return

    if args.command == "crawl":
        asyncio.run(run_crawl(args, settings))


if __name__ == "__main__":
    main()
