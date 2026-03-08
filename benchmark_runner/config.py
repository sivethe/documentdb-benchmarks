"""
Configuration loading for benchmark runs.

Supports YAML config files with CLI overrides. Config files are the primary
mechanism since benchmarks have many parameters (connection strings, workload
tuning, duration, concurrency, etc.) that are cumbersome on the command line.

CLI arguments override config file values for quick iteration.

Config files support inheritance via the ``inherits`` key. A child config
specifies ``inherits: parent_config.yaml`` (relative to the child's directory)
and only overrides the values it needs. Deep merging is applied so that nested
dicts like ``workload_params`` are merged rather than replaced.
"""

import argparse
import copy
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


import yaml


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep-merge *override* into a copy of *base*.

    For nested dicts the merge recurses; all other types are replaced
    by the override value.
    """
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


@dataclass
class BenchmarkConfig:
    """Configuration for a single benchmark run."""

    # Connection (defaults to MONGODB_URL env var if set)
    mongodb_url: str = field(
        default_factory=lambda: os.environ.get("MONGODB_URL", "mongodb://localhost:27017")
    )
    database: str = "benchmark_db"
    collection: str = "benchmark_collection"

    # Benchmark identity
    benchmark_name: str = ""
    benchmark_module: str = ""  # Python module path, e.g. "benchmarks.insert_benchmark"

    # Run labels (used by the analyzer to group/compare results)
    run_label: str = ""  # e.g. "single-node-unsharded", "Atlas", "Azure DocumentDB"
    database_engine: str = ""  # e.g. "mongodb", "atlas", "azure-documentdb", "aws-documentdb"

    # Locust settings
    users: int = 10
    spawn_rate: int = 5
    run_time: str = "60s"
    host: str = ""  # Locust host (set to mongodb_url if empty)

    # Output
    output_dir: str = "results"
    csv_prefix: str = ""  # Auto-generated if empty
    report_file: str = ""  # Auto-generated if empty (Markdown)
    json_report_file: str = ""  # Auto-generated if empty (JSON)

    # Workload-specific parameters (passed through to the benchmark)
    workload_params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.host:
            self.host = self.mongodb_url
        if not self.csv_prefix:
            self.csv_prefix = self.benchmark_name or "benchmark"
        if not self.report_file:
            self.report_file = f"{self.csv_prefix}_report.md"
        if not self.json_report_file:
            self.json_report_file = f"{self.csv_prefix}_report.json"
        if not self.run_label:
            self.run_label = self.database_engine or "default"

    @property
    def output_path(self) -> Path:
        return Path(self.output_dir)

    @property
    def csv_full_prefix(self) -> str:
        return str(self.output_path / self.csv_prefix)

    @property
    def report_full_path(self) -> str:
        return str(self.output_path / self.report_file)


def load_config(config_path: str, _seen: Optional[List[str]] = None) -> Dict[str, Any]:
    """Load a YAML config file, resolving ``inherits`` chains.

    If the config contains an ``inherits`` key, the referenced parent config
    is loaded first (path resolved relative to the child file's directory),
    and the child's values are deep-merged on top. Inheritance chains are
    supported (grandparent -> parent -> child). Circular references are
    detected and raise ``ValueError``.

    Args:
        config_path: Path to the YAML config file.

    Returns:
        Merged configuration dictionary.
    """
    config_path = str(Path(config_path).resolve())

    # Circular-reference detection
    if _seen is None:
        _seen = []
    if config_path in _seen:
        chain = " -> ".join(_seen + [config_path])
        raise ValueError(f"Circular config inheritance detected: {chain}")
    _seen.append(config_path)

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f) or {}

    parent_ref = raw.pop("inherits", None)
    if parent_ref is not None:
        parent_path = str((Path(config_path).parent / parent_ref).resolve())
        parent_data = load_config(parent_path, _seen)
        return _deep_merge(parent_data, raw)

    return raw


def build_config(
    args: Optional[argparse.Namespace] = None, config_dict: Optional[Dict[str, Any]] = None
) -> BenchmarkConfig:
    """
    Build a BenchmarkConfig from a config dict (from YAML) with optional
    CLI argument overrides.
    """
    merged: Dict[str, Any] = {}

    if config_dict:
        # Flatten: top-level keys go to BenchmarkConfig fields,
        # anything under "workload_params" stays nested
        for key, value in config_dict.items():
            merged[key] = value

    # CLI overrides (only set values that were explicitly provided)
    if args:
        args_dict = vars(args)
        for key, value in args_dict.items():
            if value is not None and key != "config":
                merged[key] = value

    # Extract workload_params separately
    workload_params = merged.pop("workload_params", {})

    # Build config with known fields; remaining keys go into workload_params
    known_fields = {f.name for f in BenchmarkConfig.__dataclass_fields__.values()}
    config_kwargs = {}
    for key, value in merged.items():
        if key in known_fields:
            config_kwargs[key] = value
        else:
            workload_params[key] = value

    config_kwargs["workload_params"] = workload_params
    return BenchmarkConfig(**config_kwargs)


def parse_args(argv=None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run a MongoDB benchmark using Locust",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with a config file
  python -m benchmark_runner.runner --config config/insert_benchmark.yaml \\
      --database-engine mongodb

  # Run with config file and CLI overrides
  python -m benchmark_runner.runner --config config/insert_benchmark.yaml \\
      --database-engine mongodb \\
      --mongodb-url "mongodb://myhost:27017" --users 20 --run-time 120s

  # Run specifying everything on CLI
  python -m benchmark_runner.runner \\
      --database-engine mongodb \\
      --benchmark-module benchmarks.insert_benchmark \\
      --benchmark-name insert_test \\
      --mongodb-url "mongodb://localhost:27017" \\
      --users 10 --run-time 60s
        """,
    )

    parser.add_argument("--config", "-c", type=str, default=None, help="Path to YAML config file")
    parser.add_argument("--mongodb-url", type=str, default=None, help="MongoDB connection string")
    parser.add_argument("--database", type=str, default=None, help="Database name")
    parser.add_argument("--collection", type=str, default=None, help="Collection name")
    parser.add_argument(
        "--benchmark-name", type=str, default=None, help="Name for this benchmark run"
    )
    parser.add_argument(
        "--benchmark-module",
        type=str,
        default=None,
        help="Python module path for the benchmark (e.g. benchmarks.insert_benchmark)",
    )
    parser.add_argument(
        "--run-label",
        type=str,
        default=None,
        help="Label for this run (e.g. 'single-node', 'Atlas')",
    )
    parser.add_argument(
        "--database-engine",
        type=str,
        required=True,
        help="Database engine name (e.g. 'mongodb', 'atlas', 'azure-documentdb')",
    )
    parser.add_argument("--users", "-u", type=int, default=None, help="Number of concurrent users")
    parser.add_argument(
        "--spawn-rate", "-r", type=int, default=None, help="User spawn rate per second"
    )
    parser.add_argument(
        "--run-time", "-t", type=str, default=None, help="Run duration (e.g. '60s', '5m', '1h')"
    )
    parser.add_argument(
        "--output-dir", "-o", type=str, default=None, help="Output directory for results"
    )

    # Convert hyphens to underscores for dataclass compatibility
    args = parser.parse_args(argv)
    # argparse stores as mongodb_url already when using dest or underscored names
    return args
