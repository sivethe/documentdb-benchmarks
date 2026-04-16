"""Tests for benchmark_runner.runner — JSON report generation."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from benchmark_runner.config import BenchmarkConfig
from benchmark_runner.runner import (
    _generate_json_report,
    _mask_url,
    _wait_for_setup_complete,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**overrides) -> BenchmarkConfig:
    defaults = dict(
        mongodb_url="mongodb://localhost:27017",
        database="test_db",
        collection="test_col",
        benchmark_name="test_bench",
        run_label="test_label",
        database_engine="mongodb",
        users=1,
        spawn_rate=1,
        run_time="10s",
        workload_params={},
    )
    defaults.update(overrides)
    return BenchmarkConfig(**defaults)


def _make_stats_entry(name="op", method="mongodb", num_requests=10, num_failures=0):
    entry = MagicMock()
    entry.name = name
    entry.method = method
    entry.num_requests = num_requests
    entry.num_failures = num_failures
    entry.avg_response_time = 5.0
    entry.min_response_time = 1.0
    entry.max_response_time = 20.0
    entry.total_rps = 100.0
    entry.fail_ratio = 0.0
    entry.get_response_time_percentile = MagicMock(return_value=5.0)
    return entry


def _make_error_entry(method="mongodb", name="op", error="some error", occurrences=1):
    err = MagicMock()
    err.method = method
    err.name = name
    err.error = error
    err.occurrences = occurrences
    return err


def _make_env(entries=None, errors=None):
    """Build a mock Locust env with runner.stats populated."""
    env = MagicMock()
    stats = MagicMock()

    entries = entries or {}
    errors = errors or {}

    stats.entries = entries
    stats.errors = errors
    stats.total = _make_stats_entry(name="Aggregated", num_requests=100)
    env.runner.stats = stats
    return env


_START = datetime(2026, 3, 7, 12, 0, 0, tzinfo=timezone.utc)
_END = datetime(2026, 3, 7, 12, 1, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# JSON report serialisation
# ---------------------------------------------------------------------------


class TestGenerateJsonReport:
    """Verify _generate_json_report produces valid, serialisable JSON."""

    def test_basic_report_is_json_serialisable(self):
        env = _make_env(
            entries={"op": _make_stats_entry()},
        )
        report = _generate_json_report(env, _make_config(), _START, _END)
        # Must not raise
        output = json.dumps(report)
        parsed = json.loads(output)
        assert parsed["metadata"]["benchmark_name"] == "test_bench"
        assert len(parsed["operations"]) == 1

    def test_string_error_is_serialisable(self):
        """Failures whose .error is a plain string should serialise."""
        env = _make_env(
            errors={"e1": _make_error_entry(error="timeout")},
        )
        report = _generate_json_report(env, _make_config(), _START, _END)
        output = json.dumps(report)
        parsed = json.loads(output)
        assert parsed["failures"][0]["error"] == "timeout"

    def test_exception_error_is_serialisable(self):
        """Failures whose .error is a RuntimeError must still serialise.

        This is the regression case: Locust stores the raw exception
        object when fired through ``events.request.fire(exception=...)``.
        """
        exc = RuntimeError("shardCollection failed for db.col: command not found")
        env = _make_env(
            errors={"e1": _make_error_entry(error=exc)},
        )
        report = _generate_json_report(env, _make_config(), _START, _END)
        # json.dumps must not raise TypeError
        output = json.dumps(report)
        parsed = json.loads(output)
        assert "shardCollection failed" in parsed["failures"][0]["error"]

    def test_various_exception_types_are_serialisable(self):
        """All common exception types should survive serialisation."""
        exceptions = [
            ConnectionError("connection refused"),
            ValueError("bad value"),
            OSError("network unreachable"),
            Exception("generic"),
        ]
        errors = {f"e{i}": _make_error_entry(error=exc) for i, exc in enumerate(exceptions)}
        env = _make_env(errors=errors)
        report = _generate_json_report(env, _make_config(), _START, _END)
        output = json.dumps(report)  # must not raise
        parsed = json.loads(output)
        assert len(parsed["failures"]) == len(exceptions)
        for f in parsed["failures"]:
            assert isinstance(f["error"], str)

    def test_report_with_no_failures(self):
        env = _make_env(entries={"op": _make_stats_entry()})
        report = _generate_json_report(env, _make_config(), _START, _END)
        output = json.dumps(report)
        parsed = json.loads(output)
        assert parsed["failures"] == []

    def test_none_min_response_time(self):
        """Operations with zero requests may have None min/max — still serialisable."""
        entry = _make_stats_entry(num_requests=0)
        entry.min_response_time = None
        entry.max_response_time = None
        env = _make_env(entries={"op": entry})
        report = _generate_json_report(env, _make_config(), _START, _END)
        output = json.dumps(report)
        parsed = json.loads(output)
        assert parsed["operations"][0]["min_response_time_ms"] is None


class TestMaskUrl:
    """Tests for _mask_url credential redaction."""

    def test_srv_with_credentials(self):
        url = "mongodb+srv://user:pass@cluster.example.net/?appName=test"
        assert _mask_url(url) == "mongodb+srv://***:***@cluster.example.net/?appName=test"

    def test_standard_with_credentials(self):
        url = "mongodb://user:pass@host1:27017,host2:27017/db?ssl=true"
        assert _mask_url(url) == "mongodb://***:***@host1:27017,host2:27017/db?ssl=true"

    def test_no_credentials(self):
        url = "mongodb://localhost:27017"
        assert _mask_url(url) == "mongodb://localhost:27017"

    def test_no_password_only_user(self):
        url = "mongodb://user@host:27017"
        assert _mask_url(url) == "mongodb://***:***@host:27017"


# ---------------------------------------------------------------------------
# _wait_for_setup_complete
# ---------------------------------------------------------------------------


class TestWaitForSetupComplete:
    """Verify _wait_for_setup_complete checks _setup_done."""

    @patch("benchmark_runner.runner.gevent.sleep")
    def test_returns_immediately_when_done(self, mock_sleep):
        cls = MagicMock()
        cls._setup_done = True

        elapsed = _wait_for_setup_complete([cls], timeout=5)

        assert elapsed >= 0
        mock_sleep.assert_not_called()

    @patch("benchmark_runner.runner.gevent.sleep")
    def test_waits_when_setup_not_done(self, mock_sleep):
        """Runner must wait until setup is complete."""
        cls = MagicMock()
        cls._setup_done = False

        # Simulate setup completing after first sleep
        def complete_setup(seconds):
            cls._setup_done = True

        mock_sleep.side_effect = complete_setup

        elapsed = _wait_for_setup_complete([cls], timeout=5)

        assert elapsed >= 0
        mock_sleep.assert_called_once()

    @patch("benchmark_runner.runner.gevent.sleep")
    def test_defaults_to_true_when_attrs_missing(self, mock_sleep):
        """Classes without _setup_done are treated as ready."""
        cls = MagicMock(spec=[])  # no attributes

        elapsed = _wait_for_setup_complete([cls], timeout=5)

        assert elapsed >= 0
        mock_sleep.assert_not_called()
