"""
Comparator - combines and compares results across multiple benchmark runs.

Supports two main comparison modes:
1. Cross-scenario: Same benchmark, same database, different configurations
   (e.g., single-node vs sharded vs replicated)
2. Cross-database: Same benchmark, same config, different database engines
   (e.g., MongoDB vs Atlas vs Azure DocumentDB vs AWS DocumentDB)

When runs span multiple benchmarks or labels, results are segmented into
sections so that operations from different benchmarks are never combined.

Produces structured comparison data that can be rendered as tables,
charts, or HTML reports.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from benchmark_analyzer.report_loader import RunResult, RequestStats


@dataclass
class OperationComparison:
    """Comparison data for a single operation across runs."""

    operation_name: str
    request_type: str
    # Keyed by run_label or database_engine
    stats_by_label: Dict[str, RequestStats] = field(default_factory=dict)


@dataclass
class ComparisonSection:
    """Results for one segment of the comparison (a specific benchmark + context)."""

    title: str
    operations: List[OperationComparison] = field(default_factory=list)
    totals: Dict[str, Optional[RequestStats]] = field(default_factory=dict)


@dataclass
class ComparisonReport:
    """Complete comparison across multiple runs."""

    benchmark_name: str
    comparison_mode: str  # "cross-scenario" or "cross-database"
    labels: List[str] = field(default_factory=list)  # ordered list of compared labels
    sections: List[ComparisonSection] = field(default_factory=list)
    runs: List[RunResult] = field(default_factory=list)


def compare_runs(runs: List[RunResult], group_by: str = "run_label") -> ComparisonReport:
    """
    Compare multiple benchmark runs.

    Runs are compared along the *group_by* dimension (e.g. ``database_engine``).
    The other dimension (``run_label`` or ``database_engine``) is used to
    segment runs into separate :class:`ComparisonSection` objects so that
    results from different benchmarks or configurations are never mixed.

    Args:
        runs: List of RunResult objects to compare
        group_by: Field to group by - either "run_label" (cross-scenario)
                  or "database_engine" (cross-database)

    Returns:
        ComparisonReport with sections, each containing operations compared
        across labels.
    """
    if not runs:
        return ComparisonReport(benchmark_name="", comparison_mode=group_by)

    mode = "cross-database" if group_by == "database_engine" else "cross-scenario"
    segment_by = "run_label" if group_by == "database_engine" else "database_engine"

    # Collect all labels (the compared dimension)
    labels = sorted({getattr(run, group_by, run.run_label) or "unknown" for run in runs})

    # Group runs into segments: (benchmark_name, segment_value) -> [runs]
    segment_runs: Dict[tuple, List[RunResult]] = defaultdict(list)
    for run in runs:
        seg_val = getattr(run, segment_by, "unknown") or "unknown"
        key = (run.benchmark_name, seg_val)
        segment_runs[key].append(run)

    # Determine if all segments share the same benchmark name
    unique_benchmarks = sorted({k[0] for k in segment_runs})
    single_benchmark = len(unique_benchmarks) == 1

    sections: List[ComparisonSection] = []
    for (bench_name, seg_val), seg_run_list in sorted(segment_runs.items()):
        ops_by_name: Dict[str, OperationComparison] = {}
        totals: Dict[str, Optional[RequestStats]] = {}

        for run in seg_run_list:
            label = getattr(run, group_by, run.run_label) or "unknown"

            for stat in run.stats:
                op_key = f"{stat.request_type}::{stat.name}"
                if op_key not in ops_by_name:
                    ops_by_name[op_key] = OperationComparison(
                        operation_name=stat.name,
                        request_type=stat.request_type,
                    )
                ops_by_name[op_key].stats_by_label[label] = stat

            if run.total_stats:
                totals[label] = run.total_stats

        title = seg_val if single_benchmark else f"{bench_name} ({seg_val})"
        sections.append(
            ComparisonSection(
                title=title,
                operations=sorted(ops_by_name.values(), key=lambda op: op.operation_name),
                totals=totals,
            )
        )

    report_name = unique_benchmarks[0] if single_benchmark else ", ".join(unique_benchmarks)

    return ComparisonReport(
        benchmark_name=report_name,
        comparison_mode=mode,
        labels=labels,
        sections=sections,
        runs=runs,
    )


def filter_runs(
    runs: List[RunResult],
    benchmark_name: Optional[str] = None,
    database_engine: Optional[str] = None,
    run_label: Optional[str] = None,
) -> List[RunResult]:
    """
    Filter runs by criteria.

    Args:
        runs: List of all available RunResult objects
        benchmark_name: Filter by benchmark name (exact match)
        database_engine: Filter by database engine
        run_label: Filter by run label

    Returns:
        Filtered list of RunResult objects
    """
    filtered = runs
    if benchmark_name:
        filtered = [r for r in filtered if r.benchmark_name == benchmark_name]
    if database_engine:
        filtered = [r for r in filtered if r.database_engine == database_engine]
    if run_label:
        filtered = [r for r in filtered if r.run_label == run_label]
    return filtered


def group_runs_by_benchmark(runs: List[RunResult]) -> Dict[str, List[RunResult]]:
    """Group runs by benchmark name."""
    groups: Dict[str, List[RunResult]] = defaultdict(list)
    for run in runs:
        groups[run.benchmark_name].append(run)
    return dict(groups)
