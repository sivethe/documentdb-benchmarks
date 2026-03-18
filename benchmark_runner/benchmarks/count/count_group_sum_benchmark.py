"""
Count Benchmark — ``$group`` + ``$sum: 1`` aggregation pipeline.

Measures aggregation performance using the classic ``$group`` with
``$sum: 1`` counting pattern on a pre-seeded collection.  This is the
most widely used counting idiom across all MongoDB versions.

Pipeline: ``[{$group: {_id: "$category", count: {$sum: 1}}}]``

Workload parameters (set in config YAML under workload_params):
    seed_docs: int      - documents to seed (default: 1000000)
    document_size: int  - approximate document size in bytes (default: 256)
    index_type: str     - index to create: none, category, category_value, wildcard (default: none)
    drop_on_start: bool - drop collection before seeding (default: true)
    match_filter: dict   - optional $match filter prepended to the pipeline (default: none)
    sharded: bool       - enable sharding (default: false)
    shard_key: str      - shard key field (default: "_id")
"""

import logging

from locust import between, task

from benchmark_runner.base_benchmark import MongoUser
from benchmark_runner.benchmarks.count.count_common import (
    create_indexes,
    seed_count_collection,
)

logger = logging.getLogger(__name__)


class CountGroupSumBenchmarkUser(MongoUser):
    """Benchmark user measuring $group + $sum:1 aggregation performance."""

    wait_time = between(0.01, 0.05)

    def on_start(self):
        """Connect, seed the collection, and create indexes."""
        super().on_start()
        self.seed_docs = self.get_param("seed_docs", 1000000)
        self.document_size = self.get_param("document_size", 256)
        self.index_type = self.get_param("index_type", "none")
        self.match_filter = self.get_param("match_filter", None)
        self.seed_collection(self._seed_and_index, drop=self.get_param("drop_on_start", True))
        self.run_warmup(self._warmup)

    def _warmup(self):
        """Capture the explain plan during the warmup phase."""
        self.capture_explain_plan(self._explain_group_sum)

    def _seed_and_index(self):
        """Seed documents and create optional indexes."""
        seed_count_collection(
            self.collection,
            num_docs=self.seed_docs,
            document_size=self.document_size,
        )
        db_engine = self.config.database_engine if self.config else ""
        create_indexes(self.collection, self.index_type, database_engine=db_engine)

    def _build_pipeline(self):
        """Build the $group + $sum:1 aggregation pipeline."""
        pipeline = []
        if self.match_filter:
            pipeline.append({"$match": self.match_filter})
        pipeline.append({"$group": {"_id": "null", "count": {"$sum": 1}}})
        return pipeline

    def _explain_group_sum(self) -> dict:
        """Return the explain plan for the group_sum aggregation pipeline."""
        return self.db.command(
            "explain",
            {
                "aggregate": self.collection.name,
                "pipeline": self._build_pipeline(),
                "cursor": {},
            },
            verbosity="allPlansExecution",
        )

    @task
    def group_sum(self):
        """Run $group + $sum:1 aggregation and measure latency."""
        if self.fail_if_sharding_error("group_sum"):
            return
        with self.timed_operation("group_sum"):
            list(self.collection.aggregate(self._build_pipeline()))
