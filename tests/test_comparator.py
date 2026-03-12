"""Tests for benchmark_analyzer.comparator — sectioned comparison logic."""

from benchmark_analyzer.comparator import compare_runs, ComparisonSection
from benchmark_analyzer.report_loader import RequestStats, RunResult


def _make_stat(
    name: str,
    req_type: str = "custom",
    num_requests: int = 100,
    avg: float = 10.0,
    rps: float = 50.0,
) -> RequestStats:
    """Create a minimal RequestStats for testing."""
    return RequestStats(
        request_type=req_type,
        name=name,
        num_requests=num_requests,
        num_failures=0,
        median_response_time=avg,
        average_response_time=avg,
        min_response_time=avg * 0.5,
        max_response_time=avg * 2,
        average_content_size=0,
        requests_per_sec=rps,
        failures_per_sec=0,
        p50=avg,
        p95=avg * 1.5,
        p99=avg * 2,
    )


def _make_run(
    benchmark_name: str,
    run_label: str,
    database_engine: str,
    op_names=("insert_one", "insert_many"),
    rps: float = 100.0,
) -> RunResult:
    """Create a minimal RunResult for testing."""
    stats = [_make_stat(name, rps=rps) for name in op_names]
    total = _make_stat("Aggregated", num_requests=sum(s.num_requests for s in stats), rps=rps * 2)
    return RunResult(
        benchmark_name=benchmark_name,
        run_label=run_label,
        database_engine=database_engine,
        database="bench_db",
        collection="test_col",
        users=10,
        spawn_rate=5,
        run_time="60s",
        stats=stats,
        total_stats=total,
    )


class TestCompareRunsSections:
    """Tests that compare_runs produces correct sections."""

    def test_single_benchmark_single_engine(self):
        """One benchmark, one engine -> one section, no splitting needed."""
        runs = [_make_run("insert", "default", "mongodb")]
        report = compare_runs(runs, group_by="database_engine")

        assert len(report.sections) == 1
        assert report.labels == ["mongodb"]
        section = report.sections[0]
        assert len(section.operations) == 2
        assert "mongodb" in section.operations[0].stats_by_label

    def test_single_benchmark_two_engines(self):
        """One benchmark, two engines -> one section comparing engines."""
        runs = [
            _make_run("insert", "unsharded", "atlas", rps=100),
            _make_run("insert", "unsharded", "azure", rps=80),
        ]
        report = compare_runs(runs, group_by="database_engine")

        assert len(report.sections) == 1
        assert report.labels == ["atlas", "azure"]

        section = report.sections[0]
        assert len(section.operations) == 2
        for op in section.operations:
            assert "atlas" in op.stats_by_label
            assert "azure" in op.stats_by_label

    def test_two_benchmarks_two_engines_grouped_by_engine(self):
        """Two benchmarks x two engines -> two sections (one per label/segment)."""
        runs = [
            _make_run("insert_sharded", "sharded", "atlas", rps=90),
            _make_run("insert_sharded", "sharded", "azure", rps=80),
            _make_run("insert_single", "unsharded", "atlas", rps=100),
            _make_run("insert_single", "unsharded", "azure", rps=85),
        ]
        report = compare_runs(runs, group_by="database_engine")

        # Segments merge by run_label, so "sharded" and "unsharded" each get one section
        assert len(report.sections) == 2
        assert report.labels == ["atlas", "azure"]

        titles = [s.title for s in report.sections]
        assert "sharded" in titles
        assert "unsharded" in titles

        # Each section compares atlas vs azure independently
        for section in report.sections:
            for op in section.operations:
                assert "atlas" in op.stats_by_label
                assert "azure" in op.stats_by_label

    def test_two_benchmarks_two_engines_grouped_by_label(self):
        """Two benchmarks x two engines, grouped by run_label -> two sections.

        segment_by=database_engine, so each engine gets one section
        comparing labels (sharded vs unsharded).
        """
        runs = [
            _make_run("insert_sharded", "sharded", "atlas"),
            _make_run("insert_sharded", "sharded", "azure"),
            _make_run("insert_single", "unsharded", "atlas"),
            _make_run("insert_single", "unsharded", "azure"),
        ]
        report = compare_runs(runs, group_by="run_label")

        # Segments merge by database_engine: atlas and azure
        assert len(report.sections) == 2
        assert report.labels == ["sharded", "unsharded"]

        titles = [s.title for s in report.sections]
        assert "atlas" in titles
        assert "azure" in titles

    def test_no_cross_contamination_of_operations(self):
        """Operations with the same name in different benchmarks merge into one section.

        When grouped by database_engine and sharing the same segment value (label),
        operations with the same name from different benchmarks merge into one
        OperationComparison with stats keyed by engine.
        """
        runs = [
            _make_run("bench_a", "label_a", "atlas", op_names=["op_x"], rps=100),
            _make_run("bench_a", "label_a", "azure", op_names=["op_x"], rps=50),
            _make_run("bench_b", "label_b", "atlas", op_names=["op_x"], rps=200),
            _make_run("bench_b", "label_b", "azure", op_names=["op_x"], rps=150),
        ]
        report = compare_runs(runs, group_by="database_engine")

        # label_a and label_b each get one section
        assert len(report.sections) == 2

        for section in report.sections:
            assert len(section.operations) == 1
            op = section.operations[0]
            assert "atlas" in op.stats_by_label
            assert "azure" in op.stats_by_label

    def test_totals_per_section(self):
        """Each section gets its own totals, not shared across benchmarks."""
        runs = [
            _make_run("bench_a", "la", "atlas", rps=100),
            _make_run("bench_a", "la", "azure", rps=80),
            _make_run("bench_b", "lb", "atlas", rps=200),
            _make_run("bench_b", "lb", "azure", rps=160),
        ]
        report = compare_runs(runs, group_by="database_engine")

        # la and lb each get one section
        assert len(report.sections) == 2
        for section in report.sections:
            assert "atlas" in section.totals
            assert "azure" in section.totals

    def test_single_benchmark_uses_simple_title(self):
        """When all runs share the same benchmark, section title is just the segment value."""
        runs = [
            _make_run("insert", "sharded", "atlas"),
            _make_run("insert", "sharded", "azure"),
        ]
        report = compare_runs(runs, group_by="database_engine")
        assert report.sections[0].title == "sharded"

    def test_multi_benchmark_uses_segment_value_title(self):
        """Section titles use the segment value, even with multiple benchmarks."""
        runs = [
            _make_run("bench_a", "la", "atlas"),
            _make_run("bench_b", "lb", "atlas"),
        ]
        report = compare_runs(runs, group_by="database_engine")
        titles = [s.title for s in report.sections]
        # Both benchmarks share engine "atlas", segments are by run_label
        assert "la" in titles
        assert "lb" in titles

    def test_empty_runs(self):
        report = compare_runs([], group_by="database_engine")
        assert report.sections == []
        assert report.labels == []

    def test_benchmark_name_in_report(self):
        """Report.benchmark_name reflects single or multiple benchmarks."""
        # Single benchmark
        runs = [_make_run("insert", "default", "atlas")]
        report = compare_runs(runs, group_by="database_engine")
        assert report.benchmark_name == "insert"

        # Multiple benchmarks
        runs = [
            _make_run("insert", "a", "atlas"),
            _make_run("find", "b", "atlas"),
        ]
        report = compare_runs(runs, group_by="database_engine")
        assert "insert" in report.benchmark_name
        assert "find" in report.benchmark_name

    def test_multiple_benchmarks_same_engine_one_section(self):
        """Multiple benchmarks on the same engine should produce one section."""
        runs = [
            _make_run("count_group_sum", "group-sum", "mongodb", op_names=["group_sum"], rps=4.0),
            _make_run(
                "count_group_count", "group-count", "mongodb", op_names=["group_count"], rps=3.7
            ),
            _make_run("count_stage", "count-stage", "mongodb", op_names=["count_stage"], rps=5.0),
        ]
        report = compare_runs(runs, group_by="run_label")

        # All runs share the same engine -> one section titled "mongodb"
        assert len(report.sections) == 1
        assert report.sections[0].title == "mongodb"

        # All 3 operations appear in the single section
        op_names = [op.operation_name for op in report.sections[0].operations]
        assert "count_stage" in op_names
        assert "group_count" in op_names
        assert "group_sum" in op_names

        # Each operation has stats only for its own label
        for op in report.sections[0].operations:
            assert len(op.stats_by_label) == 1
