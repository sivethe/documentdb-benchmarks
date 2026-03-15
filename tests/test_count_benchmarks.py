"""Tests for the count/aggregation benchmark variants.

Covers:
- count_common (seeding, index creation)
- count_group_sum_benchmark ($group + $sum:1)
- count_group_count_benchmark ($group + $count:{})
- count_stage_benchmark ($count stage)
"""

from unittest.mock import MagicMock, call, patch

import pymongo
from locust.env import Environment

from benchmark_runner.benchmarks.count.count_common import (
    create_indexes,
    seed_count_collection,
)
from benchmark_runner.benchmarks.count.count_group_sum_benchmark import (
    CountGroupSumBenchmarkUser,
)
from benchmark_runner.benchmarks.count.count_group_count_benchmark import (
    CountGroupCountBenchmarkUser,
)
from benchmark_runner.benchmarks.count.count_stage_benchmark import (
    CountStageBenchmarkUser,
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
    """Return (mock_client, mock_db, mock_collection)."""
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
# count_common — seed_count_collection
# ===========================================================================


class TestSeedCountCollection:
    """Verify seed_count_collection inserts documents correctly."""

    def test_inserts_correct_total(self):
        mock_collection = MagicMock()
        mock_collection.name = "test_col"

        seed_count_collection(mock_collection, num_docs=100, batch_size=50)

        assert mock_collection.insert_many.call_count == 2
        total_docs = sum(len(c.args[0]) for c in mock_collection.insert_many.call_args_list)
        assert total_docs == 100

    def test_small_batch(self):
        mock_collection = MagicMock()
        mock_collection.name = "test_col"

        seed_count_collection(mock_collection, num_docs=25, batch_size=10)

        assert mock_collection.insert_many.call_count == 3
        total_docs = sum(len(c.args[0]) for c in mock_collection.insert_many.call_args_list)
        assert total_docs == 25

    def test_single_batch(self):
        mock_collection = MagicMock()
        mock_collection.name = "test_col"

        seed_count_collection(mock_collection, num_docs=5, batch_size=10000)

        assert mock_collection.insert_many.call_count == 1
        docs = mock_collection.insert_many.call_args.args[0]
        assert len(docs) == 5

    def test_documents_have_expected_fields(self):
        mock_collection = MagicMock()
        mock_collection.name = "test_col"

        seed_count_collection(mock_collection, num_docs=3, batch_size=10)

        docs = mock_collection.insert_many.call_args.args[0]
        for doc in docs:
            assert "_id" in doc
            assert "category" in doc
            assert "value" in doc
            assert doc["category"] in ["A", "B", "C", "D", "E"]

    def test_ordered_false(self):
        mock_collection = MagicMock()
        mock_collection.name = "test_col"

        seed_count_collection(mock_collection, num_docs=10, batch_size=10)

        _, kwargs = mock_collection.insert_many.call_args
        assert kwargs.get("ordered") is False

    def test_returns_total_inserted(self):
        mock_collection = MagicMock()
        mock_collection.name = "test_col"

        result = seed_count_collection(mock_collection, num_docs=50, batch_size=20)
        assert result == 50


# ===========================================================================
# count_common — create_indexes
# ===========================================================================


class TestCreateIndexes:
    """Verify index creation for count benchmarks."""

    def test_none_creates_no_indexes(self):
        mock_collection = MagicMock()
        result = create_indexes(mock_collection, "none")
        assert result == []
        mock_collection.create_index.assert_not_called()

    def test_category_index(self):
        mock_collection = MagicMock()
        mock_collection.name = "test_col"
        mock_collection.create_index.return_value = "idx_category_asc"

        result = create_indexes(mock_collection, "category")

        mock_collection.create_index.assert_called_once_with(
            [("category", pymongo.ASCENDING)],
            name="idx_category_asc",
        )
        assert result == ["idx_category_asc"]

    def test_category_value_index(self):
        mock_collection = MagicMock()
        mock_collection.name = "test_col"
        mock_collection.create_index.return_value = "idx_category_value_asc"

        result = create_indexes(mock_collection, "category_value")

        mock_collection.create_index.assert_called_once_with(
            [("category", pymongo.ASCENDING), ("value", pymongo.ASCENDING)],
            name="idx_category_value_asc",
        )
        assert result == ["idx_category_value_asc"]

    def test_wildcard_index(self):
        mock_collection = MagicMock()
        mock_collection.name = "test_col"
        mock_collection.create_index.return_value = "idx_wildcard"

        result = create_indexes(mock_collection, "wildcard")

        mock_collection.create_index.assert_called_once_with(
            [("$**", 1)],
            name="idx_wildcard",
        )
        assert result == ["idx_wildcard"]

    def test_category_index_azure(self):
        mock_collection = MagicMock()
        mock_collection.name = "test_col"
        mock_collection.create_index.return_value = "idx_category_asc"

        create_indexes(mock_collection, "category", database_engine="azure_documentdb")

        mock_collection.create_index.assert_called_once_with(
            [("category", pymongo.ASCENDING)],
            name="idx_category_asc",
            storageEngine={"enableOrderedIndex": True},
        )

    def test_category_value_index_azure(self):
        mock_collection = MagicMock()
        mock_collection.name = "test_col"
        mock_collection.create_index.return_value = "idx_category_value_asc"

        create_indexes(mock_collection, "category_value", database_engine="azure_documentdb")

        mock_collection.create_index.assert_called_once_with(
            [("category", pymongo.ASCENDING), ("value", pymongo.ASCENDING)],
            name="idx_category_value_asc",
            storageEngine={"enableOrderedIndex": True},
        )

    def test_wildcard_index_no_storage_engine(self):
        """Wildcard indexes don't pass storageEngine even on Azure."""
        mock_collection = MagicMock()
        mock_collection.name = "test_col"
        mock_collection.create_index.return_value = "idx_wildcard"

        create_indexes(mock_collection, "wildcard", database_engine="azure_documentdb")

        mock_collection.create_index.assert_called_once_with(
            [("$**", 1)],
            name="idx_wildcard",
        )

    def test_unknown_index_type(self):
        mock_collection = MagicMock()
        result = create_indexes(mock_collection, "unknown_type")
        assert result == []
        mock_collection.create_index.assert_not_called()


# ===========================================================================
# CountGroupSumBenchmarkUser
# ===========================================================================


class TestCountGroupSumOnStart:
    """Verify on_start for the group_sum variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_default_params(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()

        assert user.seed_docs == 1000000
        assert user.document_size == 256
        assert user.index_type == "none"
        assert user.match_filter is None

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_custom_params(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment(
            {
                "seed_docs": 5000,
                "document_size": 512,
                "index_type": "category",
            }
        )
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()

        assert user.seed_docs == 5000
        assert user.document_size == 512
        assert user.index_type == "category"

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_seeds_collection(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()

        mock_collection.insert_many.assert_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_no_index_by_default(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_not_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_category_index(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10, "index_type": "category"})
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("category", pymongo.ASCENDING)],
            name="idx_category_asc",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_seed_runs_only_once(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user1 = _make_user(CountGroupSumBenchmarkUser, env)
        user1.on_start()

        mock_collection.reset_mock()
        user2 = CountGroupSumBenchmarkUser(env)
        user2.on_start()

        mock_collection.insert_many.assert_not_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_drops_collection_by_default(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()

        mock_collection.drop.assert_called_once()


class TestCountGroupSumTask:
    """Verify group_sum task execution."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_runs_aggregate_pipeline(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_collection.aggregate.return_value = iter(
            [
                {"_id": "A", "count": 10},
                {"_id": "B", "count": 20},
            ]
        )

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()
        user.group_sum()

        mock_collection.aggregate.assert_called_once_with(
            [{"$group": {"_id": "null", "count": {"$sum": 1}}}]
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_with_match_filter(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_collection.aggregate.return_value = iter(
            [
                {"_id": "A", "count": 10},
            ]
        )

        env = _make_mock_environment(
            {
                "seed_docs": 10,
                "match_filter": {"category": "A"},
            }
        )
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()
        user.group_sum()

        mock_collection.aggregate.assert_called_once_with(
            [
                {"$match": {"category": "A"}},
                {"$group": {"_id": "null", "count": {"$sum": 1}}},
            ]
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_sharding_error_skips_task(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()
        CountGroupSumBenchmarkUser._sharding_error = "shard failed"
        user.group_sum()

        mock_collection.aggregate.assert_not_called()


# ===========================================================================
# CountGroupCountBenchmarkUser
# ===========================================================================


class TestCountGroupCountOnStart:
    """Verify on_start for the group_count variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_default_params(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(CountGroupCountBenchmarkUser, env)
        user.on_start()

        assert user.seed_docs == 1000000
        assert user.document_size == 256
        assert user.index_type == "none"
        assert user.match_filter is None

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_custom_params(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment(
            {
                "seed_docs": 2000,
                "document_size": 1024,
                "index_type": "category_value",
            }
        )
        user = _make_user(CountGroupCountBenchmarkUser, env)
        user.on_start()

        assert user.seed_docs == 2000
        assert user.document_size == 1024
        assert user.index_type == "category_value"

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_seeds_collection(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupCountBenchmarkUser, env)
        user.on_start()

        mock_collection.insert_many.assert_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_category_value_index(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10, "index_type": "category_value"})
        user = _make_user(CountGroupCountBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("category", pymongo.ASCENDING), ("value", pymongo.ASCENDING)],
            name="idx_category_value_asc",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_seed_runs_only_once(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user1 = _make_user(CountGroupCountBenchmarkUser, env)
        user1.on_start()

        mock_collection.reset_mock()
        user2 = CountGroupCountBenchmarkUser(env)
        user2.on_start()

        mock_collection.insert_many.assert_not_called()


class TestCountGroupCountTask:
    """Verify group_count task execution."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_runs_aggregate_pipeline(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_collection.aggregate.return_value = iter(
            [
                {"_id": "A", "count": 10},
                {"_id": "B", "count": 20},
            ]
        )

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupCountBenchmarkUser, env)
        user.on_start()
        user.group_count()

        mock_collection.aggregate.assert_called_once_with(
            [{"$group": {"_id": "null", "count": {"$count": {}}}}]
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_with_match_filter(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_collection.aggregate.return_value = iter(
            [
                {"_id": "A", "count": 10},
            ]
        )

        env = _make_mock_environment(
            {
                "seed_docs": 10,
                "match_filter": {"category": "A"},
            }
        )
        user = _make_user(CountGroupCountBenchmarkUser, env)
        user.on_start()
        user.group_count()

        mock_collection.aggregate.assert_called_once_with(
            [
                {"$match": {"category": "A"}},
                {"$group": {"_id": "null", "count": {"$count": {}}}},
            ]
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_sharding_error_skips_task(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupCountBenchmarkUser, env)
        user.on_start()
        CountGroupCountBenchmarkUser._sharding_error = "shard failed"
        user.group_count()

        mock_collection.aggregate.assert_not_called()


# ===========================================================================
# CountStageBenchmarkUser
# ===========================================================================


class TestCountStageOnStart:
    """Verify on_start for the count_stage variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_default_params(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment()
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()

        assert user.seed_docs == 1000000
        assert user.document_size == 256
        assert user.index_type == "none"
        assert user.match_filter is None

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_custom_params(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment(
            {
                "seed_docs": 500,
                "document_size": 128,
                "index_type": "wildcard",
            }
        )
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()

        assert user.seed_docs == 500
        assert user.document_size == 128
        assert user.index_type == "wildcard"

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_seeds_collection(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()

        mock_collection.insert_many.assert_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_creates_wildcard_index(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10, "index_type": "wildcard"})
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()

        mock_collection.create_index.assert_called_once_with(
            [("$**", 1)],
            name="idx_wildcard",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_seed_runs_only_once(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user1 = _make_user(CountStageBenchmarkUser, env)
        user1.on_start()

        mock_collection.reset_mock()
        user2 = CountStageBenchmarkUser(env)
        user2.on_start()

        mock_collection.insert_many.assert_not_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_drops_collection_by_default(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()

        mock_collection.drop.assert_called_once()


class TestCountStageTask:
    """Verify count_stage task execution."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_runs_aggregate_pipeline(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_collection.aggregate.return_value = iter([{"total": 1000}])

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()
        user.count_stage()

        mock_collection.aggregate.assert_called_once_with([{"$count": "total"}])

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_with_match_filter(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_collection.aggregate.return_value = iter([{"total": 200}])

        env = _make_mock_environment(
            {
                "seed_docs": 10,
                "match_filter": {"category": "A"},
            }
        )
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()
        user.count_stage()

        mock_collection.aggregate.assert_called_once_with(
            [
                {"$match": {"category": "A"}},
                {"$count": "total"},
            ]
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_sharding_error_skips_task(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()
        CountStageBenchmarkUser._sharding_error = "shard failed"
        user.count_stage()

        mock_collection.aggregate.assert_not_called()


# ===========================================================================
# Task weight verification (all 3 benchmarks have exactly 1 task)
# ===========================================================================


class TestCountTaskWeights:
    """Verify each count benchmark has exactly one task."""

    def _get_task_names(self, cls):
        tasks = cls.tasks
        if isinstance(tasks, dict):
            return {func.__name__: weight for func, weight in tasks.items()}
        else:
            from collections import Counter

            return {func.__name__: weight for func, weight in Counter(tasks).items()}

    def test_group_sum_has_one_task(self):
        names = self._get_task_names(CountGroupSumBenchmarkUser)
        assert "group_sum" in names
        assert len(names) == 1

    def test_group_count_has_one_task(self):
        names = self._get_task_names(CountGroupCountBenchmarkUser)
        assert "group_count" in names
        assert len(names) == 1

    def test_count_stage_has_one_task(self):
        names = self._get_task_names(CountStageBenchmarkUser)
        assert "count_stage" in names
        assert len(names) == 1


# ===========================================================================
# CountGroupSumBenchmarkUser — explain plan
# ===========================================================================


class TestCountGroupSumExplain:
    """Verify explain plan capture for the group_sum variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_captures_result(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        explain_output = {"queryPlanner": {"winningPlan": "COLLSCAN"}}
        mock_db.command.return_value = explain_output

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()

        assert CountGroupSumBenchmarkUser._explain_result == explain_output

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_calls_db_command(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.command.return_value = {}

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()

        mock_db.command.assert_called_once_with(
            "explain",
            {
                "aggregate": "test_col",
                "pipeline": [{"$group": {"_id": "null", "count": {"$sum": 1}}}],
                "cursor": {},
            },
            verbosity="allPlansExecution",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_with_match_filter(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.command.return_value = {}

        env = _make_mock_environment({"seed_docs": 10, "match_filter": {"category": "A"}})
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()

        mock_db.command.assert_called_once_with(
            "explain",
            {
                "aggregate": "test_col",
                "pipeline": [
                    {"$match": {"category": "A"}},
                    {"$group": {"_id": "null", "count": {"$sum": 1}}},
                ],
                "cursor": {},
            },
            verbosity="allPlansExecution",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_runs_only_once(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.command.return_value = {"plan": "test"}

        env = _make_mock_environment({"seed_docs": 10})
        user1 = _make_user(CountGroupSumBenchmarkUser, env)
        user1.on_start()

        mock_db.command.reset_mock()
        user2 = CountGroupSumBenchmarkUser(env)
        user2.on_start()

        mock_db.command.assert_not_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_failure_does_not_block_benchmark(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.command.side_effect = Exception("explain not supported")

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupSumBenchmarkUser, env)
        # Should not raise — explain failure is logged and swallowed
        user.on_start()

        assert CountGroupSumBenchmarkUser._explain_done is True
        assert CountGroupSumBenchmarkUser._explain_result is None


# ===========================================================================
# CountGroupCountBenchmarkUser — explain plan
# ===========================================================================


class TestCountGroupCountExplain:
    """Verify explain plan capture for the group_count variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_captures_result(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        explain_output = {"queryPlanner": {"winningPlan": "COLLSCAN"}}
        mock_db.command.return_value = explain_output

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupCountBenchmarkUser, env)
        user.on_start()

        assert CountGroupCountBenchmarkUser._explain_result == explain_output

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_calls_db_command(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.command.return_value = {}

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupCountBenchmarkUser, env)
        user.on_start()

        mock_db.command.assert_called_once_with(
            "explain",
            {
                "aggregate": "test_col",
                "pipeline": [{"$group": {"_id": "null", "count": {"$count": {}}}}],
                "cursor": {},
            },
            verbosity="allPlansExecution",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_with_match_filter(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.command.return_value = {}

        env = _make_mock_environment({"seed_docs": 10, "match_filter": {"category": "A"}})
        user = _make_user(CountGroupCountBenchmarkUser, env)
        user.on_start()

        mock_db.command.assert_called_once_with(
            "explain",
            {
                "aggregate": "test_col",
                "pipeline": [
                    {"$match": {"category": "A"}},
                    {"$group": {"_id": "null", "count": {"$count": {}}}},
                ],
                "cursor": {},
            },
            verbosity="allPlansExecution",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_runs_only_once(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.command.return_value = {"plan": "test"}

        env = _make_mock_environment({"seed_docs": 10})
        user1 = _make_user(CountGroupCountBenchmarkUser, env)
        user1.on_start()

        mock_db.command.reset_mock()
        user2 = CountGroupCountBenchmarkUser(env)
        user2.on_start()

        mock_db.command.assert_not_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_failure_does_not_block_benchmark(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.command.side_effect = Exception("explain not supported")

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupCountBenchmarkUser, env)
        user.on_start()

        assert CountGroupCountBenchmarkUser._explain_done is True
        assert CountGroupCountBenchmarkUser._explain_result is None


# ===========================================================================
# CountStageBenchmarkUser — explain plan
# ===========================================================================


class TestCountStageExplain:
    """Verify explain plan capture for the count_stage variant."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_captures_result(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        explain_output = {"queryPlanner": {"winningPlan": "COUNT_SCAN"}}
        mock_db.command.return_value = explain_output

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()

        assert CountStageBenchmarkUser._explain_result == explain_output

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_calls_db_command(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.command.return_value = {}

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()

        mock_db.command.assert_called_once_with(
            "explain",
            {
                "aggregate": "test_col",
                "pipeline": [{"$count": "total"}],
                "cursor": {},
            },
            verbosity="allPlansExecution",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_with_match_filter(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.command.return_value = {}

        env = _make_mock_environment({"seed_docs": 10, "match_filter": {"category": "A"}})
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()

        mock_db.command.assert_called_once_with(
            "explain",
            {
                "aggregate": "test_col",
                "pipeline": [
                    {"$match": {"category": "A"}},
                    {"$count": "total"},
                ],
                "cursor": {},
            },
            verbosity="allPlansExecution",
        )

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_runs_only_once(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.command.return_value = {"plan": "test"}

        env = _make_mock_environment({"seed_docs": 10})
        user1 = _make_user(CountStageBenchmarkUser, env)
        user1.on_start()

        mock_db.command.reset_mock()
        user2 = CountStageBenchmarkUser(env)
        user2.on_start()

        mock_db.command.assert_not_called()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_explain_failure_does_not_block_benchmark(self, mock_client_cls):
        mock_client, mock_db, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client
        mock_db.command.side_effect = Exception("explain not supported")

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()

        assert CountStageBenchmarkUser._explain_done is True
        assert CountStageBenchmarkUser._explain_result is None


# ===========================================================================
# Warmup phase — all count benchmarks
# ===========================================================================


class TestCountWarmupPhase:
    """Verify run_warmup() sets _warmup_done and captures indexes."""

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_warmup_sets_done_flag_group_sum(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()

        assert CountGroupSumBenchmarkUser._warmup_done is True

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_warmup_sets_done_flag_group_count(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupCountBenchmarkUser, env)
        user.on_start()

        assert CountGroupCountBenchmarkUser._warmup_done is True

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_warmup_sets_done_flag_count_stage(self, mock_client_cls):
        mock_client, _, _ = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountStageBenchmarkUser, env)
        user.on_start()

        assert CountStageBenchmarkUser._warmup_done is True

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_warmup_captures_indexes(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user = _make_user(CountGroupSumBenchmarkUser, env)
        user.on_start()

        assert CountGroupSumBenchmarkUser._indexes_result is not None
        mock_collection.list_indexes.assert_called_once()

    @patch("benchmark_runner.base_benchmark.pymongo.MongoClient")
    def test_warmup_runs_only_once(self, mock_client_cls):
        mock_client, _, mock_collection = _setup_mock_mongo()
        mock_client_cls.return_value = mock_client

        env = _make_mock_environment({"seed_docs": 10})
        user1 = _make_user(CountGroupSumBenchmarkUser, env)
        user1.on_start()

        mock_collection.list_indexes.reset_mock()
        user2 = CountGroupSumBenchmarkUser(env)
        user2.on_start()

        # list_indexes should not be called again for the second user
        mock_collection.list_indexes.assert_not_called()
