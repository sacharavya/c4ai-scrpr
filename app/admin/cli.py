"""Administrative CLI utilities."""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from app.admin.status import load_sources as load_sources_csv
from app.observability.log import configure_logging


def _load_manifests(manifest_dir: Path) -> List[Dict[str, object]]:
    manifests: List[Dict[str, object]] = []
    if not manifest_dir.exists():
        return manifests
    for path in sorted(manifest_dir.glob("run-*.json"), reverse=True):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        payload["__path__"] = str(path)
        manifests.append(payload)
    return manifests


def cmd_status(args: argparse.Namespace) -> None:
    sources = load_sources_csv(Path(args.sources))
    manifest_dir = Path(args.manifests)
    manifests = _load_manifests(manifest_dir)
    summary = []
    for source in sources:
        stats = {"rows_new": 0, "rows_updated": 0, "rejects": 0, "last_run": None}
        for manifest in manifests:
            source_stats = manifest.get("source_stats", {})
            if source["source_id"] in source_stats:
                stats.update({k: source_stats[source["source_id"]].get(k, 0) for k in stats if k in source_stats[source["source_id"]]})
                stats["last_run"] = manifest.get("run_id")
                break
        summary.append({"source_id": source["source_id"], **stats})
    print(json.dumps(summary, indent=2))


def _quarantine_reasons(quarantine_dir: Path, *, source_id: Optional[str], days: int) -> Dict[str, int]:
    if not quarantine_dir.exists():
        return {}
    cutoff = datetime.utcnow() - timedelta(days=days)
    counter: Counter[str] = Counter()
    for path in quarantine_dir.glob("reject_*.json"):
        ts = path.stem.split("_")[-1]
        try:
            dt = datetime.strptime(ts, "%Y%m%dT%H%M%S%f")
        except ValueError:
            dt = datetime.utcnow()
        if dt < cutoff:
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        if source_id and payload.get("entity", {}).get("source_id") != source_id:
            continue
        for reason in payload.get("reason", []):
            counter[reason] += 1
    return dict(counter)


def cmd_rejects(args: argparse.Namespace) -> None:
    reasons = _quarantine_reasons(Path(args.quarantine), source_id=args.source_id, days=args.last)
    print(json.dumps(reasons, indent=2))


def cmd_explain(args: argparse.Namespace) -> None:
    url = args.url
    sources = load_sources_csv(Path(args.sources))
    matched = next((s for s in sources if url.startswith(s["base_url"])) , None)
    if not matched:
        print(json.dumps({"url": url, "matched": False}))
        return
    explanation = {
        "url": url,
        "matched": True,
        "source_id": matched["source_id"],
        "type": matched["type"],
        "max_qps": matched.get("max_qps"),
        "rules_path": matched.get("css_rules_path"),
        "robots_ok": matched.get("robots_ok"),
    }
    print(json.dumps(explanation, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="app.admin.cli", description="Administration commands")
    sub = parser.add_subparsers(dest="command", required=True)

    status = sub.add_parser("status", help="Show per-source statistics")
    status.add_argument("--sources", default="source_registry/sources.csv")
    status.add_argument("--manifests", default="data/manifests")

    rejects = sub.add_parser("inspect-rejects", help="Summarise quarantine reasons")
    rejects.add_argument("--quarantine", default="data/quarantine")
    rejects.add_argument("--source-id")
    rejects.add_argument("--last", type=int, default=7, help="Lookback window in days")

    explain = sub.add_parser("explain", help="Explain fetch configuration for a URL")
    explain.add_argument("--url", required=True)
    explain.add_argument("--sources", default="source_registry/sources.csv")

    return parser


def main(argv: Optional[List[str]] = None) -> None:
    configure_logging(Path("config/logging.yaml"))
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "status":
        cmd_status(args)
        return
    if args.command == "inspect-rejects":
        cmd_rejects(args)
        return
    if args.command == "explain":
        cmd_explain(args)
        return


if __name__ == "__main__":
    main()
