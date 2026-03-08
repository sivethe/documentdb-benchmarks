"""
Report loader - reads Locust CSV output and run metadata files.

Parses the CSV stats files and JSON metadata produced by the benchmark
runner into structured data for analysis and comparison.
"""

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class RequestStats:
    """Statistics for a single request type from a benchmark run."""
    request_type: str
    name: str
    num_requests: int
    num_failures: int
    median_response_time: float
    average_response_time: float
    min_response_time: float
    max_response_time: float
    average_content_size: float
    requests_per_sec: float
    failures_per_sec: float
    # Percentiles
    p50: float = 0.0
    p66: float = 0.0
    p75: float = 0.0
    p80: float = 0.0
    p90: float = 0.0
    p95: float = 0.0
    p98: float = 0.0
    p99: float = 0.0
    p999: float = 0.0
    p100: float = 0.0


@dataclass
class RunResult:
    """Complete results from a single benchmark run."""
    # From metadata
    benchmark_name: str = ""
    run_label: str = ""
    database_engine: str = ""
    database: str = ""
    collection: str = ""
    users: int = 0
    spawn_rate: int = 0
    run_time: str = ""
    start_time: str = ""
    end_time: str = ""
    workload_params: Dict[str, Any] = field(default_factory=dict)

    # From CSV
    stats: List[RequestStats] = field(default_factory=list)
    total_stats: Optional[RequestStats] = None

    # Source paths
    result_dir: str = ""
    csv_prefix: str = ""


def _safe_float(value: str, default: float = 0.0) -> float:
    """Safely convert a string to float."""
    try:
        return float(value) if value and value != "N/A" else default
    except (ValueError, TypeError):
        return default


def _safe_int(value: str, default: int = 0) -> int:
    """Safely convert a string to int."""
    try:
        return int(value) if value and value != "N/A" else default
    except (ValueError, TypeError):
        return default


def load_metadata(path: Path) -> Dict[str, Any]:
    """Load run metadata JSON file."""
    with open(path, "r") as f:
        return json.load(f)


def load_stats_csv(path: Path) -> List[RequestStats]:
    """
    Load a Locust stats CSV file.

    Locust generates a file like: {prefix}_stats.csv
    with columns: Type, Name, Request Count, Failure Count, Median Response Time,
    Average Response Time, Min Response Time, Max Response Time, Average Content Size,
    Requests/s, Failures/s, 50%, 66%, 75%, 80%, 90%, 95%, 98%, 99%, 99.9%, 99.99%, 100%
    """
    stats = []
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stat = RequestStats(
                request_type=row.get("Type", ""),
                name=row.get("Name", ""),
                num_requests=_safe_int(row.get("Request Count", "0")),
                num_failures=_safe_int(row.get("Failure Count", "0")),
                median_response_time=_safe_float(row.get("Median Response Time", "0")),
                average_response_time=_safe_float(row.get("Average Response Time", "0")),
                min_response_time=_safe_float(row.get("Min Response Time", "0")),
                max_response_time=_safe_float(row.get("Max Response Time", "0")),
                average_content_size=_safe_float(row.get("Average Content Size", "0")),
                requests_per_sec=_safe_float(row.get("Requests/s", "0")),
                failures_per_sec=_safe_float(row.get("Failures/s", "0")),
                p50=_safe_float(row.get("50%", "0")),
                p66=_safe_float(row.get("66%", "0")),
                p75=_safe_float(row.get("75%", "0")),
                p80=_safe_float(row.get("80%", "0")),
                p90=_safe_float(row.get("90%", "0")),
                p95=_safe_float(row.get("95%", "0")),
                p98=_safe_float(row.get("98%", "0")),
                p99=_safe_float(row.get("99%", "0")),
                p999=_safe_float(row.get("99.9%", "0")),
                p100=_safe_float(row.get("100%", "0")),
            )
            stats.append(stat)

    return stats


def load_run_result(result_dir: Path, csv_prefix: str) -> RunResult:
    """
    Load a complete run result from a directory.

    Expects:
        {result_dir}/{csv_prefix}_metadata.json
        {result_dir}/{csv_prefix}_stats.csv

    Args:
        result_dir: Directory containing the result files
        csv_prefix: The CSV prefix used during the run

    Returns:
        A RunResult with metadata and stats populated
    """
    result = RunResult(result_dir=str(result_dir), csv_prefix=csv_prefix)

    # Load metadata
    metadata_path = result_dir / f"{csv_prefix}_metadata.json"
    if metadata_path.exists():
        meta = load_metadata(metadata_path)
        result.benchmark_name = meta.get("benchmark_name", "")
        result.run_label = meta.get("run_label", "")
        result.database_engine = meta.get("database_engine", "")
        result.database = meta.get("database", "")
        result.collection = meta.get("collection", "")
        result.users = meta.get("users", 0)
        result.spawn_rate = meta.get("spawn_rate", 0)
        result.run_time = meta.get("run_time", "")
        result.start_time = meta.get("start_time", "")
        result.end_time = meta.get("end_time", "")
        result.workload_params = meta.get("workload_params", {})

    # Load stats CSV
    stats_path = result_dir / f"{csv_prefix}_stats.csv"
    if stats_path.exists():
        all_stats = load_stats_csv(stats_path)
        for stat in all_stats:
            if stat.name == "Aggregated":
                result.total_stats = stat
            else:
                result.stats.append(stat)

    return result


def discover_runs(base_dir: Path) -> List[RunResult]:
    """
    Discover all benchmark run results under a directory.

    Scans for *_metadata.json files and loads the corresponding results.

    Args:
        base_dir: Root directory to scan (searched recursively)

    Returns:
        List of RunResult objects found
    """
    results = []
    for metadata_file in sorted(base_dir.rglob("*_metadata.json")):
        meta = load_metadata(metadata_file)
        csv_prefix = meta.get("csv_prefix", "")
        if csv_prefix:
            result = load_run_result(metadata_file.parent, csv_prefix)
            results.append(result)

    return results
