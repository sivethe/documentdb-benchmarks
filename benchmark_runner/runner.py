"""
Benchmark runner - orchestrates Locust benchmark execution.

Loads configuration, discovers the benchmark module, runs Locust
programmatically, and saves results (CSV + Markdown reports) along with
run metadata for later analysis.
"""

import importlib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import locust.env
import locust.stats
import gevent

from benchmark_runner.config import (
    BenchmarkConfig,
    build_config,
    load_config,
    parse_args,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _mask_url(url: str) -> str:
    """Mask credentials in a MongoDB connection string for safe logging.

    Replaces the ``userinfo`` component (``user:password@``) of the URL with
    ``***:***@`` so that passwords are never written to logs or console output.
    If the URL contains no credentials the original string is returned unchanged.
    """
    return re.sub(r"://[^/@]+@", "://***:***@", url)


def _wait_for_setup_complete(user_classes: list, timeout: int = 600) -> float:
    """Wait for all benchmark user classes to finish setup.

    Polls ``_setup_done`` on each user class until all report ``True``
    or the *timeout* is reached.  Returns the number of seconds spent
    waiting so callers can log how long setup took.

    Args:
        user_classes: Locust User subclasses discovered for this run.
        timeout: Maximum seconds to wait before giving up.

    Returns:
        Elapsed seconds spent waiting for setup.
    """
    logger.info("Waiting for benchmark setup (seeding / warmup) to complete...")
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if all(
            getattr(cls, "_setup_done", True)
            for cls in user_classes
        ):
            elapsed = time.monotonic() - start
            logger.info("Benchmark setup complete (%.1fs)", elapsed)
            return elapsed
        gevent.sleep(1)
    elapsed = time.monotonic() - start
    logger.warning("Timed out waiting for benchmark setup after %ds", int(elapsed))
    return elapsed


def _generate_run_dir(base_output_dir: Path, benchmark_name: str) -> Path:
    """
    Generate a unique run directory under base_output_dir.

    Uses the benchmark name as the directory name (e.g. ``aggregation_single_node``).
    If a directory with that name already exists, appends ``_2``, ``_3``, etc.

    Args:
        base_output_dir: The base output directory (e.g. results/).
        benchmark_name: The benchmark name used as the subdirectory name.

    Returns:
        Path to the new unique run directory.
    """
    run_dir = base_output_dir / benchmark_name
    if not run_dir.exists():
        return run_dir

    counter = 2
    while (base_output_dir / f"{benchmark_name}_{counter}").exists():
        counter += 1
    return base_output_dir / f"{benchmark_name}_{counter}"


def discover_user_classes(module_path: str):
    """
    Import a benchmark module and return all Locust User subclasses.

    Args:
        module_path: Dotted Python module path, e.g. "benchmarks.insert_benchmark"

    Returns:
        List of User subclass types found in the module
    """
    from locust import User

    module = importlib.import_module(f"benchmark_runner.{module_path}")
    user_classes = []
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if (
            isinstance(attr, type)
            and issubclass(attr, User)
            and attr is not User
            and not getattr(attr, "abstract", False)
        ):
            user_classes.append(attr)

    if not user_classes:
        raise ValueError(
            f"No Locust User subclasses found in module '{module_path}'. "
            f"Make sure your benchmark defines a class that inherits from MongoUser."
        )

    return user_classes


def save_run_metadata(
    config: BenchmarkConfig, output_dir: Path, start_time: datetime, end_time: datetime
):
    """
    Save run metadata as JSON for the analyzer to consume.

    The metadata file includes all configuration, labels, and timing
    information needed to group and compare runs.
    """
    metadata = {
        "benchmark_name": config.benchmark_name,
        "benchmark_module": config.benchmark_module,
        "run_label": config.run_label,
        "database_engine": config.database_engine,
        "database": config.database,
        "collection": config.collection,
        "users": config.users,
        "spawn_rate": config.spawn_rate,
        "run_time": config.run_time,
        "workload_params": config.workload_params,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "csv_prefix": config.csv_prefix,
        "report_file": config.report_file,
        "json_report_file": config.json_report_file,
    }

    metadata_path = output_dir / f"{config.csv_prefix}_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"Run metadata saved to {metadata_path}")


def run_benchmark(config: BenchmarkConfig):
    """
    Execute a benchmark run using Locust programmatically.

    1. Discovers User classes from the benchmark module
    2. Creates a Locust environment with the config attached
    3. Runs the benchmark for the configured duration
    4. Saves CSV stats, HTML report, and run metadata
    """
    logger.info(f"Starting benchmark: {config.benchmark_name}")
    logger.info(f"  Module: {config.benchmark_module}")
    logger.info(f"  MongoDB URL: {_mask_url(config.mongodb_url)}")
    logger.info(f"  Database: {config.database}.{config.collection}")
    logger.info(f"  Users: {config.users}, Spawn rate: {config.spawn_rate}")
    logger.info(f"  Duration: {config.run_time}")
    logger.info(f"  Run label: {config.run_label}")
    logger.info(f"  Database engine: {config.database_engine}")

    # Generate unique run directory under the configured output path
    base_output_dir = config.output_path
    output_dir = _generate_run_dir(base_output_dir, config.csv_prefix)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"  Run output directory: {output_dir}")

    # Discover benchmark User classes
    user_classes = discover_user_classes(config.benchmark_module)
    logger.info(f"  Found user classes: {[c.__name__ for c in user_classes]}")

    # Create Locust environment
    env = locust.env.Environment(
        user_classes=user_classes,
        host=config.host,
    )

    # Attach our config to the environment so MongoUser can access it
    env.benchmark_config = config

    # Attach a save_json callback so benchmarks can write JSON files
    # during setup without knowing the output path.
    def _save_json(filename: str, data) -> None:
        """Write *data* as JSON to ``output_dir / csv_prefix_filename``."""
        path = output_dir / f"{config.csv_prefix}_{filename}"
        try:
            with open(path, "w") as f:
                json.dump(data, f, indent=2, default=str)
            logger.info("Saved %s", path)
        except Exception:
            logger.warning("Failed to save %s", path, exc_info=True)

    env.save_json = _save_json

    # Enable CSV stats writing
    csv_path = str(output_dir / config.csv_prefix)
    locust.stats.CSV_STATS_INTERVAL_SEC = 5
    stats_csv_writer = locust.stats.StatsCSVFileWriter(
        environment=env,
        percentiles_to_report=[0.5, 0.75, 0.9, 0.95, 0.99, 1.0],
        base_filepath=csv_path,
        full_history=True,
    )

    # Parse run_time to seconds
    run_time_seconds = _parse_duration(config.run_time)

    # Start the Locust runner
    runner = env.create_local_runner()

    # Start CSV writer greenlet
    csv_greenlet = gevent.spawn(stats_csv_writer.stats_writer)

    # Start the test
    runner.start(config.users, spawn_rate=config.spawn_rate)

    # Wait for seeding/setup to complete before starting the benchmark timer.
    # Seeding happens inside on_start() and can take a long time for large
    # collections with indexes.  Without this wait the run_time budget is
    # consumed by setup, potentially leaving zero time for actual tasks.
    _wait_for_setup_complete(user_classes)

    # Reset stats so the setup phase is not counted in results
    env.stats.reset_all()

    start_time = datetime.now(timezone.utc)
    logger.info(f"Benchmark running for {config.run_time} ({run_time_seconds}s)...")

    # Wait for the run duration
    gevent.sleep(run_time_seconds)

    # Stop the runner
    runner.quit()

    end_time = datetime.now(timezone.utc)

    # Kill the CSV writer greenlet and do a final flush so that
    # failures and exceptions captured near the end are persisted.
    csv_greenlet.kill(block=True)
    _final_flush_csv(stats_csv_writer)

    logger.info("Benchmark complete. Saving results...")

    # Save reports
    report_path = str(output_dir / config.report_file)
    _save_markdown_report(env, report_path)

    json_report_path = str(output_dir / config.json_report_file)
    _save_json_report(env, config, json_report_path, start_time, end_time)

    # Save run metadata for analyzer
    save_run_metadata(config, output_dir, start_time, end_time)

    # Print summary
    _print_summary(env)

    logger.info(f"Results saved to {output_dir}/")
    logger.info(f"  CSV: {csv_path}_stats.csv")
    logger.info(f"  Markdown report: {report_path}")
    logger.info(f"  JSON report: {json_report_path}")
    logger.info(f"  Metadata: {csv_path}_metadata.json")


def _final_flush_csv(stats_csv_writer: locust.stats.StatsCSVFileWriter) -> None:
    """Write final CSV data rows and close all file handles.

    The periodic ``stats_writer`` greenlet may be killed before its last
    iteration flushes data to disk. This function performs one last write
    of requests, failures, and exceptions so nothing is lost.
    """
    try:
        # Re-write the requests (seek-truncate) file with final data
        stats_csv_writer.requests_csv_filehandle.seek(0)
        stats_csv_writer.requests_csv_writer.writerow(stats_csv_writer.requests_csv_columns)
        stats_csv_writer._requests_data_rows(stats_csv_writer.requests_csv_writer)
        stats_csv_writer.requests_csv_filehandle.truncate()
        stats_csv_writer.requests_flush()

        # Re-write failures
        stats_csv_writer.failures_csv_filehandle.seek(0)
        stats_csv_writer.failures_csv_writer.writerow(stats_csv_writer.failures_columns)
        stats_csv_writer._failures_data_rows(stats_csv_writer.failures_csv_writer)
        stats_csv_writer.failures_csv_filehandle.truncate()
        stats_csv_writer.failures_flush()

        # Re-write exceptions
        stats_csv_writer.exceptions_csv_filehandle.seek(0)
        stats_csv_writer.exceptions_csv_writer.writerow(stats_csv_writer.exceptions_columns)
        stats_csv_writer._exceptions_data_rows(stats_csv_writer.exceptions_csv_writer)
        stats_csv_writer.exceptions_csv_filehandle.truncate()
        stats_csv_writer.exceptions_flush()

        # Flush history (append-only, just needs a flush)
        stats_csv_writer.stats_history_flush()
    except Exception:
        logger.warning("Failed to perform final CSV flush", exc_info=True)
    finally:
        stats_csv_writer.close_files()


def _parse_duration(duration_str: str) -> int:
    """Parse a duration string like '60s', '5m', '1h' to seconds."""
    duration_str = duration_str.strip().lower()
    if duration_str.endswith("h"):
        return int(duration_str[:-1]) * 3600
    elif duration_str.endswith("m"):
        return int(duration_str[:-1]) * 60
    elif duration_str.endswith("s"):
        return int(duration_str[:-1])
    else:
        return int(duration_str)


def _save_json_report(
    env,
    config: BenchmarkConfig,
    path: str,
    start_time: datetime,
    end_time: datetime,
) -> None:
    """Generate and save a JSON report optimised for machine parsing.

    The JSON structure contains run metadata, aggregate totals, and
    per-operation statistics with all available percentiles so that
    downstream tools can consume benchmark results without parsing
    Markdown or CSV.
    """
    report = _generate_json_report(env, config, start_time, end_time)
    with open(path, "w") as f:
        json.dump(report, f, indent=2)


def _generate_json_report(
    env,
    config: BenchmarkConfig,
    start_time: datetime,
    end_time: datetime,
) -> dict:
    """Build the JSON report dict from Locust stats."""
    stats = env.runner.stats
    total = stats.total

    def _percentiles(entry) -> dict:
        """Return a dict of common percentile values for *entry*."""
        return {
            "p50": entry.get_response_time_percentile(0.50),
            "p75": entry.get_response_time_percentile(0.75),
            "p90": entry.get_response_time_percentile(0.90),
            "p95": entry.get_response_time_percentile(0.95),
            "p99": entry.get_response_time_percentile(0.99),
            "p999": entry.get_response_time_percentile(0.999),
            "p100": entry.get_response_time_percentile(1.0),
        }

    def _entry_dict(entry) -> dict:
        return {
            "request_type": entry.method or "",
            "name": entry.name,
            "num_requests": entry.num_requests,
            "num_failures": entry.num_failures,
            "avg_response_time_ms": entry.avg_response_time,
            "min_response_time_ms": entry.min_response_time,
            "max_response_time_ms": entry.max_response_time,
            "requests_per_sec": entry.total_rps,
            "failures_per_sec": entry.fail_ratio,
            "percentiles_ms": _percentiles(entry),
        }

    operations = [_entry_dict(entry) for entry in stats.entries.values()]

    failures = []
    for fail in stats.errors.values():
        failures.append(
            {
                "method": fail.method,
                "name": fail.name,
                "error": str(fail.error),
                "occurrences": fail.occurrences,
            }
        )

    return {
        "metadata": {
            "benchmark_name": config.benchmark_name,
            "run_label": config.run_label,
            "database_engine": config.database_engine,
            "database": config.database,
            "collection": config.collection,
            "users": config.users,
            "spawn_rate": config.spawn_rate,
            "run_time": config.run_time,
            "workload_params": config.workload_params,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds(),
        },
        "summary": {
            "total_requests": total.num_requests,
            "total_failures": total.num_failures,
            "avg_response_time_ms": total.avg_response_time,
            "min_response_time_ms": total.min_response_time,
            "max_response_time_ms": total.max_response_time,
            "requests_per_sec": total.total_rps,
            "percentiles_ms": _percentiles(total),
        },
        "operations": operations,
        "failures": failures,
    }


def _save_markdown_report(env, path: str):
    """Generate and save a Markdown report from Locust stats."""
    md = _generate_markdown_report(env)
    with open(path, "w") as f:
        f.write(md)


def _generate_markdown_report(env) -> str:
    """Generate a Markdown report from Locust stats."""
    stats = env.runner.stats
    lines = []
    lines.append("# Benchmark Report\n")
    lines.append("## Summary\n")

    def _fmt(val, fmt=".1f"):
        """Format a value, returning 'N/A' if None."""
        if val is None:
            return "N/A"
        return f"{val:{fmt}}"

    total = stats.total
    lines.append(f"- **Total Requests:** {total.num_requests:,}")
    lines.append(f"- **Total Failures:** {total.num_failures:,}")
    lines.append(f"- **Avg Response Time:** {_fmt(total.avg_response_time)} ms")
    if total.num_requests > 0:
        lines.append(f"- **Min Response Time:** {_fmt(total.min_response_time)} ms")
        lines.append(f"- **Max Response Time:** {_fmt(total.max_response_time)} ms")
        lines.append(f"- **P50:** {_fmt(total.get_response_time_percentile(0.5))} ms")
        lines.append(f"- **P95:** {_fmt(total.get_response_time_percentile(0.95))} ms")
        lines.append(f"- **P99:** {_fmt(total.get_response_time_percentile(0.99))} ms")
    lines.append(f"- **Requests/sec:** {_fmt(total.total_rps)}")
    lines.append("")

    lines.append("## Detailed Results\n")
    lines.append(
        "| Type | Name | Requests | Failures | Avg (ms) | Min (ms) | Max (ms) | P50 (ms) | P95 (ms) | P99 (ms) | RPS |"
    )
    lines.append(
        "|------|------|----------|----------|----------|----------|----------|----------|----------|----------|-----|"
    )

    for entry in stats.entries.values():
        method = entry.method or ""
        row = (
            f"| {method} | {entry.name} "
            f"| {entry.num_requests:,} | {entry.num_failures:,} "
            f"| {_fmt(entry.avg_response_time)} "
            f"| {_fmt(entry.min_response_time)} "
            f"| {_fmt(entry.max_response_time)} "
            f"| {_fmt(entry.get_response_time_percentile(0.5))} "
            f"| {_fmt(entry.get_response_time_percentile(0.95))} "
            f"| {_fmt(entry.get_response_time_percentile(0.99))} "
            f"| {_fmt(entry.total_rps)} |"
        )
        lines.append(row)

    # Aggregated row
    agg_row = (
        f"| | **Aggregated** "
        f"| **{total.num_requests:,}** | **{total.num_failures:,}** "
        f"| **{_fmt(total.avg_response_time)}** "
        f"| **{_fmt(total.min_response_time)}** "
        f"| **{_fmt(total.max_response_time)}** "
        f"| **{_fmt(total.get_response_time_percentile(0.5))}** "
        f"| **{_fmt(total.get_response_time_percentile(0.95))}** "
        f"| **{_fmt(total.get_response_time_percentile(0.99))}** "
        f"| **{_fmt(total.total_rps)}** |"
    )
    lines.append(agg_row)
    lines.append("")

    return "\n".join(lines)


def _print_summary(env):
    """Print a summary of the benchmark run to stdout."""
    stats = env.runner.stats
    total = stats.total
    print("\n" + "=" * 80)
    print("BENCHMARK SUMMARY")
    print("=" * 80)
    print(f"  Total requests:  {total.num_requests}")
    print(f"  Total failures:  {total.num_failures}")
    print(f"  Avg response:    {total.avg_response_time:.1f} ms")
    if total.num_requests > 0:
        print(f"  Min response:    {total.min_response_time:.1f} ms")
        print(f"  Max response:    {total.max_response_time:.1f} ms")
        print(f"  P50:             {total.get_response_time_percentile(0.5):.1f} ms")
        print(f"  P95:             {total.get_response_time_percentile(0.95):.1f} ms")
        print(f"  P99:             {total.get_response_time_percentile(0.99):.1f} ms")
    print(f"  Requests/sec:    {total.total_rps:.1f}")
    print("=" * 80 + "\n")


def main():
    """CLI entry point."""
    args = parse_args()

    # Load config file if provided
    config_dict = {}
    if args.config:
        logger.info(f"Loading config from {args.config}")
        config_dict = load_config(args.config)

    config = build_config(args, config_dict)

    if not config.benchmark_module:
        logger.error(
            "No benchmark module specified. Use --benchmark-module or set "
            "'benchmark_module' in your config file."
        )
        sys.exit(1)

    if not config.database_engine:
        logger.error(
            "No database engine specified. Use --database-engine to identify the "
            "target engine (e.g. 'mongodb', 'atlas', 'azure-documentdb')."
        )
        sys.exit(1)

    run_benchmark(config)


if __name__ == "__main__":
    main()
