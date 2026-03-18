"""
Report generator - produces human-readable comparison reports.

Generates:
- Console table output (for quick viewing)
- Markdown reports (for VS Code preview and documentation)
- CSV export (for further analysis in spreadsheets)
"""

import csv
import json
from io import StringIO
from pathlib import Path
from typing import List, Optional

from benchmark_analyzer.comparator import (
    ComparisonReport,
    ComparisonSection,
    OperationComparison,
)
from benchmark_analyzer.report_loader import RequestStats


def format_console_report(report: ComparisonReport) -> str:
    """
    Generate a formatted console table comparing benchmark runs.

    Returns a string with aligned columns showing key metrics
    across all compared labels, with each operation as a row.
    Sections are used when multiple benchmarks or labels are present.
    """
    lines = []
    lines.append("=" * 110)
    lines.append(f"BENCHMARK COMPARISON: {report.benchmark_name}")
    lines.append(f"Mode: {report.comparison_mode}")
    lines.append(f"Comparing: {', '.join(report.labels)}")
    lines.append("=" * 110)

    if not report.sections or not any(s.operations for s in report.sections):
        lines.append("No operations to compare.")
        return "\n".join(lines)

    multi_label = len(report.labels) > 1
    multi_section = len(report.sections) > 1

    # Build header template
    if multi_label:
        header = (
            f"  {'Operation':<30} {'Label':<20} {'Requests':>10} "
            f"{'Avg (ms)':>10} {'P50 (ms)':>10} "
            f"{'P95 (ms)':>10} {'P99 (ms)':>10} {'RPS':>10} {'Fail %':>8}"
        )
    else:
        header = (
            f"  {'Operation':<35} {'Requests':>10} "
            f"{'Avg (ms)':>10} {'P50 (ms)':>10} "
            f"{'P95 (ms)':>10} {'P99 (ms)':>10} {'RPS':>10} {'Fail %':>8}"
        )

    for section in report.sections:
        if not section.operations:
            continue

        lines.append("")
        if multi_section:
            lines.append(f"--- {section.title} ---")
            lines.append("")

        lines.append(header)
        lines.append("  " + "-" * (len(header) - 2))

        for op in section.operations:
            for label in report.labels:
                stat = op.stats_by_label.get(label)
                if stat is None:
                    continue
                if multi_label:
                    fail_pct = (stat.num_failures / max(stat.num_requests, 1)) * 100
                    row = (
                        f"  {op.operation_name:<30} {label:<20} "
                        f"{stat.num_requests:>10,} "
                        f"{stat.average_response_time:>10.1f} "
                        f"{stat.p50:>10.1f} {stat.p95:>10.1f} "
                        f"{stat.p99:>10.1f} "
                        f"{stat.requests_per_sec:>10.1f} "
                        f"{fail_pct:>7.1f}%"
                    )
                else:
                    fail_pct = (stat.num_failures / max(stat.num_requests, 1)) * 100
                    row = (
                        f"  {op.operation_name:<35} "
                        f"{stat.num_requests:>10,} "
                        f"{stat.average_response_time:>10.1f} "
                        f"{stat.p50:>10.1f} {stat.p95:>10.1f} "
                        f"{stat.p99:>10.1f} "
                        f"{stat.requests_per_sec:>10.1f} "
                        f"{fail_pct:>7.1f}%"
                    )
                lines.append(row)

        # Totals
        if section.totals:
            lines.append("  " + "-" * (len(header) - 2))
            for label in report.labels:
                stat = section.totals.get(label)
                if stat is None:
                    continue
                fail_pct = (stat.num_failures / max(stat.num_requests, 1)) * 100
                if multi_label:
                    row = (
                        f"  {'TOTAL':<30} {label:<20} "
                        f"{stat.num_requests:>10,} "
                        f"{stat.average_response_time:>10.1f} "
                        f"{stat.p50:>10.1f} {stat.p95:>10.1f} "
                        f"{stat.p99:>10.1f} "
                        f"{stat.requests_per_sec:>10.1f} "
                        f"{fail_pct:>7.1f}%"
                    )
                else:
                    row = (
                        f"  {'TOTAL':<35} "
                        f"{stat.num_requests:>10,} "
                        f"{stat.average_response_time:>10.1f} "
                        f"{stat.p50:>10.1f} {stat.p95:>10.1f} "
                        f"{stat.p99:>10.1f} "
                        f"{stat.requests_per_sec:>10.1f} "
                        f"{fail_pct:>7.1f}%"
                    )
                lines.append(row)

    lines.append("")
    lines.append("=" * 110)
    return "\n".join(lines)


def generate_markdown_report(report: ComparisonReport) -> str:
    """Generate a Markdown comparison report with sections.

    Produces a report suitable for VS Code preview, GitHub rendering,
    and documentation. When multiple benchmarks or configurations are
    present, results are split into separate sections so that operations
    from different contexts are never combined.
    """
    lines = []
    lines.append(f"# Benchmark Comparison: {report.benchmark_name}\n")
    lines.append(f"- **Mode:** {report.comparison_mode}")
    lines.append(f"- **Comparing:** {', '.join(report.labels)}")
    lines.append("")

    # Run details
    if report.runs:
        lines.append("## Run Details\n")
        lines.append("| Benchmark | Label | Engine | Users | Duration | Database |")
        lines.append("|-----------|-------|--------|-------|----------|----------|")
        for run in report.runs:
            label = getattr(run, "run_label", "unknown")
            row = (
                f"| {run.benchmark_name} | {label} | {run.database_engine}"
                f" | {run.users} | {run.run_time} | {run.database}.{run.collection} |"
            )
            lines.append(row)
        lines.append("")

    multi_label = len(report.labels) > 1
    multi_section = len(report.sections) > 1
    has_any_ops = any(s.operations for s in report.sections)

    if not has_any_ops:
        lines.append("No operations to compare.")
        return "\n".join(lines)

    for section in report.sections:
        if not section.operations:
            continue

        # Section heading
        if multi_section:
            lines.append(f"## Results: {section.title}\n")
        else:
            lines.append("## Results\n")

        lines.append(_operations_flat_md_table(section.operations, report.labels, multi_label))
        lines.append("")

    return "\n".join(lines)


def _fmt_val(val, fmt: str = "{:.1f}") -> str:
    """Format a value, returning 'N/A' if None."""
    if val is None:
        return "N/A"
    return fmt.format(val)


_FLAT_TABLE_METRICS = [
    ("Requests", "num_requests", "{:,}"),
    ("Failures", "num_failures", "{:,}"),
    ("Avg (ms)", "average_response_time", "{:.1f}"),
    ("P50 (ms)", "p50", "{:.1f}"),
    ("P95 (ms)", "p95", "{:.1f}"),
    ("P99 (ms)", "p99", "{:.1f}"),
    ("RPS", "requests_per_sec", "{:.1f}"),
]


def _operations_flat_md_table(
    operations: List[OperationComparison],
    labels: List[str],
    multi_label: bool,
) -> str:
    """Generate a single Markdown table with all operations as rows."""
    # Build header
    hdr_cols = ["Operation"]
    if multi_label:
        hdr_cols.append("Label")
    hdr_cols.extend(name for name, _, _ in _FLAT_TABLE_METRICS)
    header = "| " + " | ".join(hdr_cols) + " |"
    sep = "|" + "|".join("---" for _ in hdr_cols) + "|"
    rows = [header, sep]

    for op in operations:
        for label in labels:
            stat = op.stats_by_label.get(label)
            if stat is None:
                continue
            cells = [op.operation_name]
            if multi_label:
                cells.append(label)
            for _, attr, fmt in _FLAT_TABLE_METRICS:
                cells.append(_fmt_val(getattr(stat, attr, None), fmt))
            rows.append("| " + " | ".join(cells) + " |")

    return "\n".join(rows)


def _totals_flat_md_table(totals, labels: List[str], multi_label: bool) -> str:
    """Generate a Markdown totals table matching the flat layout."""
    hdr_cols = ["Label" if multi_label else ""]
    hdr_cols.extend(name for name, _, _ in _FLAT_TABLE_METRICS)
    header = "| " + " | ".join(hdr_cols) + " |"
    sep = "|" + "|".join("---" for _ in hdr_cols) + "|"
    rows = [header, sep]

    for label in labels:
        stat = totals.get(label)
        cells = [label if multi_label else "Total"]
        for _, attr, fmt in _FLAT_TABLE_METRICS:
            if stat:
                cells.append(_fmt_val(getattr(stat, attr, None), fmt))
            else:
                cells.append("N/A")
        rows.append("| " + " | ".join(cells) + " |")

    return "\n".join(rows)


def export_comparison_csv(report: ComparisonReport, output_path: Path):
    """
    Export comparison data as a CSV file for spreadsheet analysis.

    Each row represents one metric for one operation, with columns
    for each compared label. When multiple sections are present, a
    Section column is included to distinguish them.
    """
    multi_section = len(report.sections) > 1

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)

        # Header
        header = []
        if multi_section:
            header.append("Section")
        header.extend(["Operation", "Type", "Metric"])
        header.extend(report.labels)
        writer.writerow(header)

        metrics = [
            ("Requests", "num_requests"),
            ("Failures", "num_failures"),
            ("Avg (ms)", "average_response_time"),
            ("P50 (ms)", "p50"),
            ("P95 (ms)", "p95"),
            ("P99 (ms)", "p99"),
            ("RPS", "requests_per_sec"),
        ]

        for section in report.sections:
            for op in section.operations:
                for metric_name, attr in metrics:
                    row = []
                    if multi_section:
                        row.append(section.title)
                    row.extend([op.operation_name, op.request_type, metric_name])
                    for label in report.labels:
                        stat = op.stats_by_label.get(label)
                        if stat:
                            row.append(getattr(stat, attr, ""))
                        else:
                            row.append("")
                    writer.writerow(row)

            # Totals
            if section.totals:
                for metric_name, attr in metrics:
                    row = []
                    if multi_section:
                        row.append(section.title)
                    row.extend(["TOTAL", "", metric_name])
                    for label in report.labels:
                        stat = section.totals.get(label)
                        if stat:
                            row.append(getattr(stat, attr, ""))
                        else:
                            row.append("")
                    writer.writerow(row)
