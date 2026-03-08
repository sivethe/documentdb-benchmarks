"""
Analyzer CLI - command-line interface for analyzing and comparing benchmark results.

Usage:
  # Compare all runs of a specific benchmark
  python -m benchmark_analyzer.analyzer --results-dir results/insert --benchmark-name insert_single_node

  # Compare across database engines
  python -m benchmark_analyzer.analyzer --results-dir results/ --group-by database_engine

  # Compare across scenarios (configurations)
  python -m benchmark_analyzer.analyzer --results-dir results/ --group-by run_label

  # Export to CSV
  python -m benchmark_analyzer.analyzer --results-dir results/insert --output comparison.csv --format csv
"""

import argparse
import logging
import sys
from pathlib import Path

from benchmark_analyzer.report_loader import discover_runs
from benchmark_analyzer.comparator import compare_runs, filter_runs, group_runs_by_benchmark
from benchmark_analyzer.report_generator import (
    format_console_report,
    generate_markdown_report,
    export_comparison_csv,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze and compare benchmark results across runs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # View comparison on console
  python -m benchmark_analyzer.analyzer -d results/insert

  # Compare different databases for the insert benchmark
  python -m benchmark_analyzer.analyzer -d results/insert \\
      --group-by database_engine --output insert_db_comparison.md

  # Compare different configurations
  python -m benchmark_analyzer.analyzer -d results/insert \\
      --group-by run_label --benchmark-name insert_single_node

  # Export to CSV for spreadsheet analysis
  python -m benchmark_analyzer.analyzer -d results/ \\
      --format csv --output comparison.csv
        """,
    )

    parser.add_argument("--results-dir", "-d", type=str, required=True,
                        help="Directory containing benchmark results (searched recursively)")
    parser.add_argument("--benchmark-name", "-b", type=str, default=None,
                        help="Filter by benchmark name")
    parser.add_argument("--database-engine", type=str, default=None,
                        help="Filter by database engine")
    parser.add_argument("--run-label", type=str, default=None,
                        help="Filter by run label")
    parser.add_argument("--group-by", "-g", type=str, default="run_label",
                        choices=["run_label", "database_engine"],
                        help="How to group runs for comparison (default: run_label)")
    parser.add_argument("--output", "-o", type=str, default=None,
                        help="Output file path (default: print to console)")
    parser.add_argument("--format", "-f", type=str, default="auto",
                        choices=["auto", "console", "md", "csv"],
                        help="Output format (default: auto-detect from --output extension)")
    parser.add_argument("--list-runs", action="store_true",
                        help="List all discovered runs and exit")

    return parser.parse_args(argv)


def list_discovered_runs(runs):
    """Print a summary of all discovered runs."""
    print(f"\nDiscovered {len(runs)} benchmark run(s):\n")
    print(f"  {'Benchmark':<30} {'Label':<25} {'Engine':<20} {'Users':>6} {'Duration':>10}")
    print("  " + "-" * 95)
    for run in runs:
        print(f"  {run.benchmark_name:<30} {run.run_label:<25} "
              f"{run.database_engine:<20} {run.users:>6} {run.run_time:>10}")
    print()


def main():
    args = parse_args()
    results_dir = Path(args.results_dir)

    if not results_dir.exists():
        logger.error(f"Results directory not found: {results_dir}")
        sys.exit(1)

    # Discover all runs
    logger.info(f"Scanning {results_dir} for benchmark results...")
    all_runs = discover_runs(results_dir)

    if not all_runs:
        logger.error(f"No benchmark results found in {results_dir}")
        sys.exit(1)

    logger.info(f"Found {len(all_runs)} run(s)")

    # List runs mode
    if args.list_runs:
        list_discovered_runs(all_runs)
        return

    # Filter runs
    filtered = filter_runs(
        all_runs,
        benchmark_name=args.benchmark_name,
        database_engine=args.database_engine,
        run_label=args.run_label,
    )

    if not filtered:
        logger.error("No runs match the specified filters")
        sys.exit(1)

    logger.info(f"Comparing {len(filtered)} run(s) grouped by {args.group_by}")

    # Generate comparison
    report = compare_runs(filtered, group_by=args.group_by)

    # Determine output format
    output_format = args.format
    if output_format == "auto":
        if args.output:
            ext = Path(args.output).suffix.lower()
            output_format = {".md": "md", ".csv": "csv"}.get(ext, "console")
        else:
            output_format = "console"

    # Generate output
    if output_format == "console":
        text = format_console_report(report)
        if args.output:
            with open(args.output, "w") as f:
                f.write(text)
            logger.info(f"Report saved to {args.output}")
        else:
            print(text)

    elif output_format == "md":
        md = generate_markdown_report(report)
        output_path = args.output or "comparison_report.md"
        with open(output_path, "w") as f:
            f.write(md)
        logger.info(f"Markdown report saved to {output_path}")

    elif output_format == "csv":
        output_path = Path(args.output or "comparison_report.csv")
        export_comparison_csv(report, output_path)
        logger.info(f"CSV report saved to {output_path}")


if __name__ == "__main__":
    main()
