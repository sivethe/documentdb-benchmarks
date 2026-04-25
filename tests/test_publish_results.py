"""Tests for ``deploy/publish_results.py``."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import pytest

# Make ``deploy/`` importable for the tests.
DEPLOY_DIR = Path(__file__).resolve().parent.parent / "deploy"
sys.path.insert(0, str(DEPLOY_DIR))

import publish_results  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_report(
    *,
    benchmark: str = "insert_no_index",
    engine: str = "azure_documentdb",
    timestamp: str = "2026-04-25T00:00:00+00:00",
    users: int = 10,
    workload_params: Dict[str, Any] = None,
    operations: List[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    workload_params = workload_params if workload_params is not None else {"foo": "bar"}
    operations = (
        operations
        if operations is not None
        else [
            {
                "request_type": "mongodb",
                "name": "insert_one",
                "num_requests": 100,
                "num_failures": 0,
                "avg_response_time_ms": 5.0,
                "min_response_time_ms": 1.0,
                "max_response_time_ms": 50.0,
                "requests_per_sec": 100.0,
                "failures_per_sec": 0.0,
                "percentiles_ms": {"p50": 5, "p95": 20, "p99": 40},
            }
        ]
    )
    return {
        "metadata": {
            "benchmark_name": benchmark,
            "run_label": "default",
            "database_engine": engine,
            "database": "benchmark_db",
            "collection": "test",
            "users": users,
            "spawn_rate": 5,
            "run_time": "60s",
            "workload_params": workload_params,
            "start_time": timestamp,
            "end_time": timestamp,
            "duration_seconds": 60.0,
        },
        "summary": {
            "total_requests": 100,
            "total_failures": 0,
            "avg_response_time_ms": 5.0,
            "min_response_time_ms": 1.0,
            "max_response_time_ms": 50.0,
            "requests_per_sec": 100.0,
            "percentiles_ms": {"p50": 5, "p95": 20, "p99": 40},
        },
        "operations": operations,
        "failures": [],
    }


def _write_report(
    run_dir: Path,
    report: Dict[str, Any],
) -> Path:
    benchmark = report["metadata"]["benchmark_name"]
    engine = report["metadata"]["database_engine"]
    target_dir = run_dir / engine / benchmark
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{benchmark}_report.json"
    with target.open("w") as f:
        json.dump(report, f)
    return target


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_parse_timestamp_handles_z_suffix() -> None:
    dt = publish_results._parse_timestamp("2026-04-25T00:00:00Z")
    assert dt is not None
    assert dt.tzinfo is timezone.utc


def test_parse_timestamp_naive_assumes_utc() -> None:
    dt = publish_results._parse_timestamp("2026-04-25T00:00:00")
    assert dt is not None
    assert dt.tzinfo is timezone.utc


def test_parse_timestamp_invalid() -> None:
    assert publish_results._parse_timestamp("not-a-date") is None
    assert publish_results._parse_timestamp("") is None


def test_operation_rows_excludes_total() -> None:
    rows = publish_results._operation_rows(_make_report())
    names = sorted(r["name"] for r in rows)
    assert names == ["insert_one"]
    for row in rows:
        assert row["timestamp"] == "2026-04-25T00:00:00+00:00"
        assert "request_type" not in row


def test_operation_rows_filters_percentiles() -> None:
    report = _make_report(
        operations=[
            {
                "request_type": "mongodb",
                "name": "insert_one",
                "num_requests": 1,
                "num_failures": 0,
                "avg_response_time_ms": 1.04,
                "min_response_time_ms": 0.57,
                "max_response_time_ms": 2.06,
                "requests_per_sec": 1.04,
                "percentiles_ms": {
                    "p50": 5.58,
                    "p75": 7,
                    "p90": 10.04,
                    "p95": 20,
                    "p99": 40.49,
                    "p999": 80,
                    "p100": 100,
                },
            }
        ]
    )
    rows = publish_results._operation_rows(report)
    assert "avg_response_time_ms" not in rows[0]
    assert "percentiles_ms" not in rows[0]
    assert rows[0]["requests_per_sec"] == 1.0
    assert rows[0]["response_time_ms"] == {
        "min": 0.6,
        "max": 2.1,
        "avg": 1.0,
        "p50": 5.6,
        "p90": 10.0,
        "p95": 20.0,
        "p99": 40.5,
    }


def test_prune_operations_drops_old_rows() -> None:
    now = datetime(2026, 4, 25, tzinfo=timezone.utc)
    cutoff = now - timedelta(days=90)
    rows = [
        {"timestamp": "2026-04-20T00:00:00+00:00", "name": "a"},
        {"timestamp": "2025-01-01T00:00:00+00:00", "name": "b"},
        {"timestamp": "garbage", "name": "c"},
    ]
    kept = publish_results.prune_operations(rows, cutoff=cutoff)
    kept_names = sorted(r["name"] for r in kept)
    # The recent one is kept; the very-old one is dropped; unparseable
    # timestamps are kept (we don't know how old they are).
    assert kept_names == ["a", "c"]


# ---------------------------------------------------------------------------
# Merge logic
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def test_merge_creates_new_document() -> None:
    cutoff = _now() - timedelta(days=90)
    report = _make_report()
    meta = publish_results._common_metadata(report)
    rows = publish_results._operation_rows(report)

    doc, added, warning = publish_results.merge_document(None, meta, rows, cutoff=cutoff)

    assert warning is None
    assert added == len(rows)
    assert doc["benchmark_name"] == "insert_no_index"
    assert doc["database_engine"] == "azure_documentdb"
    assert len(doc["operations"]) == len(rows)


def test_merge_dedupes_by_timestamp_and_name() -> None:
    cutoff = _now() - timedelta(days=90)
    report = _make_report()
    meta = publish_results._common_metadata(report)
    rows = publish_results._operation_rows(report)

    doc, _, _ = publish_results.merge_document(None, meta, rows, cutoff=cutoff)
    # Re-merging the same rows should not add anything new.
    doc2, added, warning = publish_results.merge_document(doc, meta, rows, cutoff=cutoff)

    assert warning is None
    assert added == 0
    assert len(doc2["operations"]) == len(rows)


def test_merge_appends_new_timestamps() -> None:
    cutoff = _now() - timedelta(days=90)
    first = _make_report(timestamp="2026-04-20T00:00:00+00:00")
    second = _make_report(timestamp="2026-04-21T00:00:00+00:00")

    meta = publish_results._common_metadata(first)
    rows1 = publish_results._operation_rows(first)
    rows2 = publish_results._operation_rows(second)

    doc, _, _ = publish_results.merge_document(None, meta, rows1, cutoff=cutoff)
    doc, added, warning = publish_results.merge_document(doc, meta, rows2, cutoff=cutoff)

    assert warning is None
    assert added == len(rows2)
    timestamps = {row["timestamp"] for row in doc["operations"]}
    assert timestamps == {
        "2026-04-20T00:00:00+00:00",
        "2026-04-21T00:00:00+00:00",
    }


def test_merge_rejects_metadata_mismatch() -> None:
    cutoff = _now() - timedelta(days=90)
    first = _make_report(users=10)
    second = _make_report(users=50)

    meta1 = publish_results._common_metadata(first)
    rows1 = publish_results._operation_rows(first)
    doc, _, _ = publish_results.merge_document(None, meta1, rows1, cutoff=cutoff)

    meta2 = publish_results._common_metadata(second)
    rows2 = publish_results._operation_rows(second)
    doc2, added, warning = publish_results.merge_document(doc, meta2, rows2, cutoff=cutoff)

    assert warning is not None
    assert "users" in warning
    assert added == 0
    # Original document untouched.
    assert doc2 == doc


def test_merge_prunes_old_rows() -> None:
    cutoff = _now() - timedelta(days=90)
    old = _make_report(timestamp="2025-01-01T00:00:00+00:00")
    new = _make_report(timestamp="2026-04-24T00:00:00+00:00")

    meta = publish_results._common_metadata(old)
    rows_old = publish_results._operation_rows(old)
    rows_new = publish_results._operation_rows(new)

    doc, _, _ = publish_results.merge_document(None, meta, rows_old, cutoff=cutoff)
    # The first call already prunes old rows on insertion.
    assert doc["operations"] == []

    doc, added, _ = publish_results.merge_document(doc, meta, rows_new, cutoff=cutoff)
    assert added == len(rows_new)
    assert all(row["timestamp"] == "2026-04-24T00:00:00+00:00" for row in doc["operations"])


# ---------------------------------------------------------------------------
# End-to-end publish()
# ---------------------------------------------------------------------------


def test_publish_writes_files_and_index(tmp_path: Path) -> None:
    run_dir = tmp_path / "results" / "20260425-001"
    out_dir = tmp_path / "data"

    _write_report(run_dir, _make_report(benchmark="insert_no_index"))
    _write_report(
        run_dir,
        _make_report(benchmark="insert_no_index", engine="mongodb"),
    )
    _write_report(run_dir, _make_report(benchmark="count_no_filter"))

    summary = publish_results.publish(run_dir, out_dir, now=_now())

    assert summary["reports_found"] == 3
    assert summary["files_written"] == 3
    assert summary["warnings"] == []

    assert (out_dir / "insert_no_index" / "azure_documentdb.json").exists()
    assert (out_dir / "insert_no_index" / "mongodb.json").exists()
    assert (out_dir / "count_no_filter" / "azure_documentdb.json").exists()

    index = json.loads((out_dir / "index.json").read_text())
    assert "insert_no_index" in index["benchmarks"]
    assert "azure_documentdb" in index["benchmarks"]["insert_no_index"]["engines"]
    assert "mongodb" in index["benchmarks"]["insert_no_index"]["engines"]


def test_publish_is_idempotent(tmp_path: Path) -> None:
    run_dir = tmp_path / "results" / "20260425-001"
    out_dir = tmp_path / "data"
    _write_report(run_dir, _make_report())

    publish_results.publish(run_dir, out_dir, now=_now())
    target = out_dir / "insert_no_index" / "azure_documentdb.json"
    before = json.loads(target.read_text())

    summary = publish_results.publish(run_dir, out_dir, now=_now())
    after = json.loads(target.read_text())

    assert summary["rows_added"] == 0
    assert before["operations"] == after["operations"]


def test_publish_appends_second_run(tmp_path: Path) -> None:
    out_dir = tmp_path / "data"

    run1 = tmp_path / "results" / "20260424-001"
    _write_report(run1, _make_report(timestamp="2026-04-24T00:00:00+00:00"))
    publish_results.publish(run1, out_dir, now=_now())

    run2 = tmp_path / "results" / "20260425-001"
    _write_report(run2, _make_report(timestamp="2026-04-25T00:00:00+00:00"))
    summary = publish_results.publish(run2, out_dir, now=_now())

    assert summary["rows_added"] > 0
    target = out_dir / "insert_no_index" / "azure_documentdb.json"
    doc = json.loads(target.read_text())
    timestamps = {row["timestamp"] for row in doc["operations"]}
    assert timestamps == {
        "2026-04-24T00:00:00+00:00",
        "2026-04-25T00:00:00+00:00",
    }


def test_publish_warns_on_metadata_mismatch(tmp_path: Path) -> None:
    out_dir = tmp_path / "data"

    run1 = tmp_path / "results" / "20260424-001"
    _write_report(run1, _make_report(users=10))
    publish_results.publish(run1, out_dir, now=_now())

    run2 = tmp_path / "results" / "20260425-001"
    _write_report(run2, _make_report(users=50))
    summary = publish_results.publish(run2, out_dir, now=_now())

    assert summary["files_written"] == 0
    assert any("metadata mismatch" in w for w in summary["warnings"])


def test_publish_missing_results_dir_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        publish_results.publish(tmp_path / "missing", tmp_path / "out")


def test_cli_main_returns_zero(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "results" / "20260425-001"
    out_dir = tmp_path / "data"
    _write_report(run_dir, _make_report())

    rc = publish_results.main(
        [
            "--results-dir",
            str(run_dir),
            "--output-dir",
            str(out_dir),
        ]
    )
    captured = capsys.readouterr()
    assert rc == 0
    assert "Published" in captured.out
