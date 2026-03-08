"""Tests for the four insert benchmark variants.

Covers:
- insert_no_index_benchmark
- insert_single_path_index_benchmark
- insert_wildcard_index_benchmark
- insert_composite_index_benchmark
"""

from unittest.mock import MagicMock, patch

import pymongo
from locust.env import Environment

from benchmark_runner.benchmarks.insert.insert_no_index_benchmark import InsertNoIndexBenchmarkUser
from benchmark_runner.benchmarks.insert.insert_single_path_index_benchmark import (
    InsertSinglePathIndexBenchmarkUser,
)
from benchmark_runner.benchmarks.insert.insert_wildcard_index_benchmark import (
    InsertWildcardIndexBenchmarkUser,
)
from benchmark_runner.benchmarks.insert.insert_composite_index_benchmark import (
    InsertCompositeIndexBenchmarkUser,
)
from benchmark_runner.config import BenchmarkConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_environment(workload_params=None, database_engine=""):
    """Create a minimal Locust Environment with a mock BenchmarkConfig."""
    env = Environment()
    config = BenchmarkConfig(
        mongodb_url="mongodb://localhost:27017",
        database="test_db",
        collection="test_col",
        workload_params=workload_params or {},
        database_engine=database_engine,
    )
    env.benchmark_config = config
    return env


def _reset_class(cls):
    """Reset class-level seed flags so tests are isolated."""
    cls._seed_done = False
    cls._sharding_error = None


def _make_user(cls, env):
    """Instantiate a benchmark user with class-level state reset."""
    _reset_class(cls)
    return cls(env)


def _setup_mock_mongo():
    """Return (mock_client_cls, mock_client, mock_db, mock_collection)."""
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    mock_collection = MagicMock()
    mock_collection.name = "test_col"
    mock_collection.estimated_document_count.return_value = 0
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)
    return mock_client, mock_db, mock_collection


# ===========================================================================
# InsertNoIndexBenchmarkUser
# ===========================================================================


class TestInsertNoIndexOnStart:
    """Verify on_start for the no-index variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_default_params(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()

        assert user.document_size == 256
        assert user.batch_size == 100

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_custom_params(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"document_size": 1024, "batch_size": 50})
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()

        assert user.document_size == 1024
        assert user.batch_size == 50

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_no_index_created(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_not_called()


class TestInsertNoIndexTasks:
    """Verify task methods for the no-index variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_one(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"document_size": 256, "batch_size": 5})
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()
        user.insert_one()

        mock_collection.insert_one.assert_called_once()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_many(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"document_size": 256, "batch_size": 5})
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()
        user.insert_many()

        mock_collection.insert_many.assert_called_once()
        docs = mock_collection.insert_many.call_args.args[0]
        assert len(docs) == 5

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_one_sharding_error(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()
        InsertNoIndexBenchmarkUser._sharding_error = "shard failed"
        user.insert_one()

        mock_collection.insert_one.assert_not_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_many_sharding_error(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()
        InsertNoIndexBenchmarkUser._sharding_error = "shard failed"
        user.insert_many()

        mock_collection.insert_many.assert_not_called()


class TestInsertNoIndexWeights:
    """Verify Locust task weights for the no-index variant."""

    def test_task_weights(self):
        tasks = InsertNoIndexBenchmarkUser.tasks
        if isinstance(tasks, dict):
            weights = tasks
        else:
            from collections import Counter

            weights = Counter(tasks)

        name_weights = {func.__name__: weight for func, weight in weights.items()}
        assert name_weights.get("insert_one") == 3
        assert name_weights.get("insert_many") == 1


class TestInsertNoIndexWeightParams:
    """Verify insert_one_weight / insert_many_weight config params."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_one_skipped_when_weight_zero(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"insert_one_weight": 0})
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()
        user.insert_one()

        mock_collection.insert_one.assert_not_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_many_skipped_when_weight_zero(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"insert_many_weight": 0})
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()
        user.insert_many()

        mock_collection.insert_many.assert_not_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_one_runs_when_weight_nonzero(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"insert_one_weight": 1, "insert_many_weight": 0})
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()
        user.insert_one()

        mock_collection.insert_one.assert_called_once()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_many_runs_when_weight_nonzero(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"insert_one_weight": 0, "insert_many_weight": 5})
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()
        user.insert_many()

        mock_collection.insert_many.assert_called_once()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_default_weights(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()

        assert user.insert_one_weight == 3
        assert user.insert_many_weight == 1


# ===========================================================================
# InsertSinglePathIndexBenchmarkUser
# ===========================================================================


class TestInsertSinglePathIndexOnStart:
    """Verify on_start for the single-path-index variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_timestamp_index(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("timestamp", pymongo.ASCENDING)],
            name="idx_timestamp_asc",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_timestamp_index_with_ordered_index_on_azure(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment(database_engine="azure_documentdb")
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("timestamp", pymongo.ASCENDING)],
            name="idx_timestamp_asc",
            storageEngine={"enableOrderedIndex": True},
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_seed_runs_only_once(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user1 = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user1.on_start()

        mock_collection.reset_mock()
        user2 = InsertSinglePathIndexBenchmarkUser(env)
        user2.on_start()

        assert mock_collection.create_index.call_count == 0


class TestInsertSinglePathIndexTasks:
    """Verify tasks for the single-path-index variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_one(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"document_size": 256, "batch_size": 5})
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user.on_start()
        user.insert_one_singlePathIndex()

        mock_collection.insert_one.assert_called_once()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_many(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"document_size": 256, "batch_size": 5})
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user.on_start()
        user.insert_many_singlePathIndex()

        mock_collection.insert_many.assert_called_once()
        docs = mock_collection.insert_many.call_args.args[0]
        assert len(docs) == 5


class TestInsertSinglePathIndexWeights:
    """Verify task weights for the single-path-index variant."""

    def test_task_weights(self):
        tasks = InsertSinglePathIndexBenchmarkUser.tasks
        if isinstance(tasks, dict):
            weights = tasks
        else:
            from collections import Counter

            weights = Counter(tasks)

        name_weights = {func.__name__: weight for func, weight in weights.items()}
        assert name_weights.get("insert_one_singlePathIndex") == 3
        assert name_weights.get("insert_many_singlePathIndex") == 1


# ===========================================================================
# InsertWildcardIndexBenchmarkUser
# ===========================================================================


class TestInsertWildcardIndexOnStart:
    """Verify on_start for the wildcard-index variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_wildcard_index(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertWildcardIndexBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("$**", 1)],
            name="idx_wildcard",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_wildcard_index_with_ordered_index_on_azure(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment(database_engine="azure_documentdb")
        user = _make_user(InsertWildcardIndexBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("$**", 1)],
            name="idx_wildcard",
            storageEngine={"enableOrderedIndex": True},
        )


class TestInsertWildcardIndexTasks:
    """Verify tasks for the wildcard-index variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_one(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"document_size": 256, "batch_size": 5})
        user = _make_user(InsertWildcardIndexBenchmarkUser, env)
        user.on_start()
        user.insert_one_wildcardIndex()

        mock_collection.insert_one.assert_called_once()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_many(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"document_size": 256, "batch_size": 5})
        user = _make_user(InsertWildcardIndexBenchmarkUser, env)
        user.on_start()
        user.insert_many_wildcardIndex()

        mock_collection.insert_many.assert_called_once()
        docs = mock_collection.insert_many.call_args.args[0]
        assert len(docs) == 5


class TestInsertWildcardIndexWeights:
    """Verify task weights for the wildcard-index variant."""

    def test_task_weights(self):
        tasks = InsertWildcardIndexBenchmarkUser.tasks
        if isinstance(tasks, dict):
            weights = tasks
        else:
            from collections import Counter

            weights = Counter(tasks)

        name_weights = {func.__name__: weight for func, weight in weights.items()}
        assert name_weights.get("insert_one_wildcardIndex") == 3
        assert name_weights.get("insert_many_wildcardIndex") == 1


# ===========================================================================
# InsertCompositeIndexBenchmarkUser
# ===========================================================================


class TestInsertCompositeIndexOnStart:
    """Verify on_start for the composite-index variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_composite_index(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertCompositeIndexBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("category", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)],
            name="idx_category_timestamp_asc",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_composite_index_with_ordered_index_on_azure(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment(database_engine="azure_documentdb")
        user = _make_user(InsertCompositeIndexBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("category", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)],
            name="idx_category_timestamp_asc",
            storageEngine={"enableOrderedIndex": True},
        )


class TestInsertCompositeIndexTasks:
    """Verify tasks for the composite-index variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_one(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"document_size": 256, "batch_size": 5})
        user = _make_user(InsertCompositeIndexBenchmarkUser, env)
        user.on_start()
        user.insert_one_compositeIndex()

        mock_collection.insert_one.assert_called_once()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_many(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"document_size": 256, "batch_size": 5})
        user = _make_user(InsertCompositeIndexBenchmarkUser, env)
        user.on_start()
        user.insert_many_compositeIndex()

        mock_collection.insert_many.assert_called_once()
        docs = mock_collection.insert_many.call_args.args[0]
        assert len(docs) == 5


class TestInsertCompositeIndexWeights:
    """Verify task weights for the composite-index variant."""

    def test_task_weights(self):
        tasks = InsertCompositeIndexBenchmarkUser.tasks
        if isinstance(tasks, dict):
            weights = tasks
        else:
            from collections import Counter

            weights = Counter(tasks)

        name_weights = {func.__name__: weight for func, weight in weights.items()}
        assert name_weights.get("insert_one_compositeIndex") == 3
        assert name_weights.get("insert_many_compositeIndex") == 1
