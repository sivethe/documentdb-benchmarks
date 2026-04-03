"""Tests for the five insert benchmark variants.

Covers:
- insert_no_index_benchmark
- insert_single_path_index_benchmark
- insert_wildcard_index_benchmark
- insert_composite_index_benchmark
- insert_unique_index_benchmark
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
from benchmark_runner.benchmarks.insert.insert_unique_index_benchmark import (
    InsertUniqueIndexBenchmarkUser,
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
    cls._explain_done = False
    cls._explain_result = None
    cls._indexes_result = None
    cls._warmup_done = False


def _make_user(cls, env):
    """Instantiate a benchmark user with class-level state reset."""
    _reset_class(cls)
    return cls(env)


def _setup_mock_mongo():
    """Return (mock_client_cls, mock_client, mock_db, mock_collection)."""
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_db.name = "test_db"
    mock_db.current_op.return_value = {"inprog": []}
    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    mock_collection = MagicMock()
    mock_collection.name = "test_col"
    mock_collection.estimated_document_count.return_value = 0
    mock_collection.list_indexes.return_value = iter([{"v": 2, "key": {"_id": 1}, "name": "_id_"}])
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
    def test_creates_createdAt_index(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("createdAt", pymongo.ASCENDING)],
            name="idx_createdAt_asc",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_createdAt_index_with_ordered_index_on_azure(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment(database_engine="azure_documentdb")
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("createdAt", pymongo.ASCENDING)],
            name="idx_createdAt_asc",
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
            [("category", pymongo.ASCENDING), ("createdAt", pymongo.ASCENDING)],
            name="idx_category_createdAt_asc",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_composite_index_with_ordered_index_on_azure(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment(database_engine="azure_documentdb")
        user = _make_user(InsertCompositeIndexBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("category", pymongo.ASCENDING), ("createdAt", pymongo.ASCENDING)],
            name="idx_category_createdAt_asc",
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


# ===========================================================================
# InsertUniqueIndexBenchmarkUser
# ===========================================================================


class TestInsertUniqueIndexOnStart:
    """Verify on_start for the unique-index variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_unique_index(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("createdAt", pymongo.ASCENDING)],
            name="idx_createdAt_unique",
            unique=True,
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_unique_index_with_ordered_index_on_azure(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment(database_engine="azure_documentdb")
        user = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("createdAt", pymongo.ASCENDING)],
            name="idx_createdAt_unique",
            unique=True,
            storageEngine={"enableOrderedIndex": True},
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_seed_runs_only_once(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user1 = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user1.on_start()

        mock_collection.reset_mock()
        user2 = InsertUniqueIndexBenchmarkUser(env)
        user2.on_start()

        assert mock_collection.create_index.call_count == 0


class TestInsertUniqueIndexTasks:
    """Verify tasks for the unique-index variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_one(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"document_size": 256, "batch_size": 5})
        user = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user.on_start()
        user.insert_one_uniqueIndex()

        mock_collection.insert_one.assert_called_once()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_many(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"document_size": 256, "batch_size": 5})
        user = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user.on_start()
        user.insert_many_uniqueIndex()

        mock_collection.insert_many.assert_called_once()
        docs = mock_collection.insert_many.call_args.args[0]
        assert len(docs) == 5

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_one_sharding_error(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user.on_start()
        InsertUniqueIndexBenchmarkUser._sharding_error = "shard failed"
        user.insert_one_uniqueIndex()

        mock_collection.insert_one.assert_not_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_many_sharding_error(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user.on_start()
        InsertUniqueIndexBenchmarkUser._sharding_error = "shard failed"
        user.insert_many_uniqueIndex()

        mock_collection.insert_many.assert_not_called()


class TestInsertUniqueIndexWeights:
    """Verify task weights for the unique-index variant."""

    def test_task_weights(self):
        tasks = InsertUniqueIndexBenchmarkUser.tasks
        if isinstance(tasks, dict):
            weights = tasks
        else:
            from collections import Counter

            weights = Counter(tasks)

        name_weights = {func.__name__: weight for func, weight in weights.items()}
        assert name_weights.get("insert_one_uniqueIndex") == 3
        assert name_weights.get("insert_many_uniqueIndex") == 1


class TestInsertUniqueIndexWeightParams:
    """Verify insert_one_weight / insert_many_weight config params for unique-index."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_one_skipped_when_weight_zero(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"insert_one_weight": 0})
        user = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user.on_start()
        user.insert_one_uniqueIndex()

        mock_collection.insert_one.assert_not_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_many_skipped_when_weight_zero(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"insert_many_weight": 0})
        user = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user.on_start()
        user.insert_many_uniqueIndex()

        mock_collection.insert_many.assert_not_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_one_runs_when_weight_nonzero(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"insert_one_weight": 1, "insert_many_weight": 0})
        user = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user.on_start()
        user.insert_one_uniqueIndex()

        mock_collection.insert_one.assert_called_once()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_insert_many_runs_when_weight_nonzero(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"insert_one_weight": 0, "insert_many_weight": 5})
        user = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user.on_start()
        user.insert_many_uniqueIndex()

        mock_collection.insert_many.assert_called_once()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_default_weights(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user.on_start()

        assert user.insert_one_weight == 3
        assert user.insert_many_weight == 1


# ===========================================================================
# Index Build Verification (base_benchmark features)
# ===========================================================================


class TestWaitForIndexBuilds:
    """Verify _wait_for_index_builds polls currentOp correctly."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_returns_immediately_when_no_index_ops(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.current_op.return_value = {"inprog": []}

        env = _make_mock_environment()
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user.on_start()

        mock_db.current_op.assert_called()

    @patch("benchmark_runner.base_benchmark.time.sleep")
    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_polls_until_index_build_finishes(self, mock_client_cls, mock_sleep):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        # First call: index build in progress, second call: done
        mock_db.current_op.side_effect = [
            {"inprog": [{"msg": "Index Build: scanning", "command": {}}]},
            {"inprog": []},
        ]

        env = _make_mock_environment()
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user.on_start()

        assert mock_db.current_op.call_count == 2
        mock_sleep.assert_called_once_with(2.0)

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_detects_createIndexes_command(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        # Operation with createIndexes command matching collection name
        mock_db.current_op.side_effect = [
            {"inprog": [{"command": {"createIndexes": "test_col"}, "msg": ""}]},
            {"inprog": []},
        ]

        env = _make_mock_environment()
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)

        with patch("benchmark_runner.base_benchmark.time.sleep"):
            user.on_start()

        assert mock_db.current_op.call_count == 2

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_skips_when_current_op_not_supported(self, mock_client_cls):
        """Engines that don't support currentOp should not block."""
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.current_op.side_effect = Exception("command not supported")

        env = _make_mock_environment()
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user.on_start()  # Should not raise

        mock_db.current_op.assert_called_once()


class TestCaptureIndexes:
    """Verify _capture_indexes stores the index list on the class."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_captures_index_list(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        expected_indexes = [
            {"v": 2, "key": {"_id": 1}, "name": "_id_"},
            {"v": 2, "key": {"createdAt": 1}, "name": "idx_createdAt_asc"},
        ]
        mock_collection.list_indexes.return_value = iter(expected_indexes)

        env = _make_mock_environment()
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user.on_start()

        assert InsertSinglePathIndexBenchmarkUser._indexes_result == expected_indexes

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_captures_indexes_for_no_index_benchmark(self, mock_client_cls):
        """Even benchmarks without custom indexes should capture the default _id index."""
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        expected_indexes = [{"v": 2, "key": {"_id": 1}, "name": "_id_"}]
        mock_collection.list_indexes.return_value = iter(expected_indexes)

        env = _make_mock_environment()
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()

        assert InsertNoIndexBenchmarkUser._indexes_result == expected_indexes

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_handles_list_indexes_failure(self, mock_client_cls):
        """If list_indexes fails, _indexes_result stays None."""
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_collection.list_indexes.side_effect = Exception("not supported")

        env = _make_mock_environment()
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user.on_start()

        assert InsertSinglePathIndexBenchmarkUser._indexes_result is None


# ===========================================================================
# Warmup phase — insert benchmarks
# ===========================================================================


class TestInsertWarmupPhase:
    """Verify run_warmup() sets _warmup_done for insert benchmarks."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_warmup_sets_done_flag_no_index(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertNoIndexBenchmarkUser, env)
        user.on_start()

        assert InsertNoIndexBenchmarkUser._warmup_done is True

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_warmup_sets_done_flag_single_path(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertSinglePathIndexBenchmarkUser, env)
        user.on_start()

        assert InsertSinglePathIndexBenchmarkUser._warmup_done is True

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_warmup_sets_done_flag_wildcard(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertWildcardIndexBenchmarkUser, env)
        user.on_start()

        assert InsertWildcardIndexBenchmarkUser._warmup_done is True

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_warmup_sets_done_flag_composite(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertCompositeIndexBenchmarkUser, env)
        user.on_start()

        assert InsertCompositeIndexBenchmarkUser._warmup_done is True

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_warmup_sets_done_flag_unique(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(InsertUniqueIndexBenchmarkUser, env)
        user.on_start()

        assert InsertUniqueIndexBenchmarkUser._warmup_done is True

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_warmup_runs_only_once(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user1 = _make_user(InsertNoIndexBenchmarkUser, env)
        user1.on_start()

        mock_collection.list_indexes.reset_mock()
        user2 = InsertNoIndexBenchmarkUser(env)
        user2.on_start()

        # list_indexes should not be called again for the second user
        mock_collection.list_indexes.assert_not_called()
