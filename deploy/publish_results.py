#!/usr/bin/env python3
"""Publish benchmark run results into a per-benchmark/per-engine JSON store.

This script consumes a benchmark *run directory* produced by ``bench-run``
(e.g. ``results/20260425-003/``) and merges the data it finds into a
flat JSON store organised as::

    <output-dir>/
        index.json                          # catalog of benchmarks/engines
        <benchmark_name>/
            <engine_name>.json              # time-series per (benchmark, engine)

Each ``<benchmark>/<engine>.json`` document contains the common metadata
that identifies the run configuration plus an ``operations`` array. Every
element of that array is a timestamped data point for a single Locust
operation (or the synthetic ``__total__`` aggregate).

The publisher is **idempotent**: re-running it for the same source
directory does not change the on-disk file. New rows are deduplicated by
``(timestamp, name)``. If a new run uses a different configuration (e.g.
different ``users`` or ``workload_params``) than the existing file, it is
skipped with a warning rather than mixing incompatible data points.

Entries older than ``--retention-days`` (default 90) are pruned on every
run so the store stays bounded.

Usage::

    python deploy/publish_results.py \\
        --results-dir results/20260425-003 \\
        --output-dir gh-pages/data
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

DEFAULT_RETENTION_DAYS = 90
TOTAL_OP_NAME = "__total__"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _OperationKey:
    timestamp: str
    name: str


def _common_metadata(report: Dict[str, Any]) -> Dict[str, Any]:
    """Subset of metadata that must be identical across all rows in a file."""
    md = report.get("metadata", {})
    return {
        "benchmark_name": md.get("benchmark_name", ""),
        "run_label": md.get("run_label", ""),
        "database_engine": md.get("database_engine", ""),
        "users": md.get("users"),
        "spawn_rate": md.get("spawn_rate"),
        "run_time": md.get("run_time"),
        "workload_params": md.get("workload_params", {}),
    }


def _operation_rows(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten the per-op entries (and synthetic total) into time-series rows."""
    md = report.get("metadata", {})
    timestamp = md.get("start_time")
    if not timestamp:
        return []

    rows: List[Dict[str, Any]] = []

    for op in report.get("operations", []):
        name = op.get("name", "")
        if name == TOTAL_OP_NAME:
            continue
        rows.append(
            {
                "timestamp": timestamp,
                "name": name,
                "num_requests": op.get("num_requests", 0),
                "num_failures": op.get("num_failures", 0),
                "requests_per_sec": _round1(op.get("requests_per_sec")),
                "response_time_ms": _response_time_ms(op),
            }
        )

    return rows


_KEPT_PERCENTILES = ("p50", "p90", "p95", "p99")


def _response_time_ms(op: Dict[str, Any]) -> Dict[str, Any]:
    """Build the consolidated response-time object for a single operation."""
    percentiles = op.get("percentiles_ms") or {}
    if not isinstance(percentiles, dict):
        percentiles = {}
    result: Dict[str, Any] = {
        "min": _round1(op.get("min_response_time_ms")),
        "max": _round1(op.get("max_response_time_ms")),
        "avg": _round1(op.get("avg_response_time_ms")),
    }
    for key in _KEPT_PERCENTILES:
        if key in percentiles:
            result[key] = _round1(percentiles[key])
    return result


def _round1(value: Any) -> Any:
    """Round a numeric latency to a single decimal; pass through None/non-numeric."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return round(float(value), 1)
    return value


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def find_reports(results_dir: Path) -> List[Path]:
    """Return all ``*_report.json`` files under *results_dir*."""
    return sorted(results_dir.rglob("*_report.json"))


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r") as f:
        return json.load(f)


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as f:
        json.dump(data, f, indent=2, sort_keys=False)
        f.write("\n")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Merge / prune
# ---------------------------------------------------------------------------


def _parse_timestamp(value: str) -> Optional[datetime]:
    """Parse an ISO8601 timestamp; tolerate trailing 'Z' and naive values."""
    if not value:
        return None
    s = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def prune_operations(
    operations: Iterable[Dict[str, Any]],
    *,
    cutoff: datetime,
) -> List[Dict[str, Any]]:
    """Drop operation rows whose ``timestamp`` is older than *cutoff*."""
    kept: List[Dict[str, Any]] = []
    for row in operations:
        ts = _parse_timestamp(row.get("timestamp", ""))
        if ts is None or ts >= cutoff:
            kept.append(row)
    return kept


def _operation_keys(rows: Iterable[Dict[str, Any]]) -> set:
    return {_OperationKey(r.get("timestamp", ""), r.get("name", "")) for r in rows}


def merge_document(
    existing: Optional[Dict[str, Any]],
    new_meta: Dict[str, Any],
    new_rows: List[Dict[str, Any]],
    *,
    cutoff: datetime,
) -> Tuple[Dict[str, Any], int, Optional[str]]:
    """Merge *new_rows* into *existing*.

    Returns ``(document, added_count, warning)``. ``warning`` is non-None
    when the new run's common metadata is incompatible with *existing*;
    in that case the original document is returned unchanged.
    """
    if existing is None:
        document = dict(new_meta)
        document["operations"] = prune_operations(new_rows, cutoff=cutoff)
        return document, len(document["operations"]), None

    existing_meta = {k: existing.get(k) for k in new_meta.keys()}
    if existing_meta != new_meta:
        differences = [k for k in new_meta.keys() if existing_meta.get(k) != new_meta.get(k)]
        warning = "metadata mismatch; skipping merge. Differing keys: " + ", ".join(
            sorted(differences)
        )
        return existing, 0, warning

    seen = _operation_keys(existing.get("operations", []))
    appended = 0
    merged = list(existing.get("operations", []))
    for row in new_rows:
        key = _OperationKey(row.get("timestamp", ""), row.get("name", ""))
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)
        appended += 1

    document = dict(existing)
    document.update(new_meta)
    document["operations"] = prune_operations(merged, cutoff=cutoff)
    return document, appended, None


# ---------------------------------------------------------------------------
# Index regeneration
# ---------------------------------------------------------------------------


def regenerate_index(output_dir: Path) -> Dict[str, Any]:
    """Walk *output_dir* and rebuild ``index.json``."""
    benchmarks: Dict[str, Dict[str, Any]] = {}

    for engine_file in sorted(output_dir.glob("*/*.json")):
        if engine_file.name == "index.json":
            continue
        try:
            doc = load_json(engine_file)
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(doc, dict):
            continue

        benchmark_name = doc.get("benchmark_name") or engine_file.parent.name
        engine_name = doc.get("database_engine") or engine_file.stem

        timestamps = [
            _parse_timestamp(row.get("timestamp", "")) for row in doc.get("operations", [])
        ]
        timestamps = [t for t in timestamps if t is not None]
        latest = max(timestamps).isoformat() if timestamps else None
        run_count = len({t.isoformat() for t in timestamps})

        engines = benchmarks.setdefault(benchmark_name, {"engines": {}})["engines"]
        engines[engine_name] = {
            "path": f"{benchmark_name}/{engine_name}.json",
            "latest_timestamp": latest,
            "run_count": run_count,
            "operation_count": len(doc.get("operations", [])),
        }

    index = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "benchmarks": benchmarks,
    }
    write_json(output_dir / "index.json", index)
    return index


# ---------------------------------------------------------------------------
# Top-level publish
# ---------------------------------------------------------------------------


def publish(
    results_dir: Path,
    output_dir: Path,
    *,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Publish all reports in *results_dir* into *output_dir*.

    Returns a summary dict useful for logging and tests.
    """
    if not results_dir.is_dir():
        raise FileNotFoundError(f"results-dir not found: {results_dir}")

    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=retention_days)

    reports = find_reports(results_dir)
    summary: Dict[str, Any] = {
        "reports_found": len(reports),
        "files_written": 0,
        "rows_added": 0,
        "warnings": [],
    }

    for report_path in reports:
        try:
            report = load_json(report_path)
        except (OSError, json.JSONDecodeError) as exc:
            summary["warnings"].append(f"{report_path}: failed to read ({exc})")
            continue

        meta = _common_metadata(report)
        benchmark = meta["benchmark_name"]
        engine = meta["database_engine"]
        if not benchmark or not engine:
            summary["warnings"].append(f"{report_path}: missing benchmark_name or database_engine")
            continue

        rows = _operation_rows(report)
        if not rows:
            summary["warnings"].append(f"{report_path}: no operation rows")
            continue

        target = output_dir / benchmark / f"{engine}.json"
        existing = load_json(target) if target.exists() else None
        if existing is not None and not isinstance(existing, dict):
            summary["warnings"].append(f"{target}: existing file is not an object; overwriting")
            existing = None

        document, added, warning = merge_document(existing, meta, rows, cutoff=cutoff)
        if warning:
            summary["warnings"].append(f"{target}: {warning}")
            continue

        write_json(target, document)
        summary["files_written"] += 1
        summary["rows_added"] += added

    regenerate_index(output_dir)
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--results-dir",
        required=True,
        type=Path,
        help="Path to a benchmark run directory (e.g. results/20260425-003).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Path to the data store directory (e.g. gh-pages/data).",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Drop operation rows older than this (default: {DEFAULT_RETENTION_DAYS}).",
    )
    args = parser.parse_args(argv)

    try:
        summary = publish(
            args.results_dir,
            args.output_dir,
            retention_days=args.retention_days,
        )
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(
        f"Published {summary['files_written']} files "
        f"({summary['rows_added']} new rows) "
        f"from {summary['reports_found']} report(s)."
    )
    for warning in summary["warnings"]:
        print(f"WARN: {warning}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
