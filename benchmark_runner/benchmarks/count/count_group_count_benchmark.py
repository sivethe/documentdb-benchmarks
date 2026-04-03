"""
Count Benchmark — ``$group`` + ``$count: {}`` aggregation pipeline.

Measures aggregation performance using the ``$count`` accumulator
(introduced in MongoDB 5.0) within a ``$group`` stage.  Compares
whether ``$count: {}`` is optimized differently from ``$sum: 1``
across database engines.

Pipeline: ``[{$group: {_id: "$category", count: {$count: {}}}}]``

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

from benchmark_runner.benchmarks.count.count_common import CountBenchmarkUser

logger = logging.getLogger(__name__)


class CountGroupCountBenchmarkUser(CountBenchmarkUser):
    """Benchmark user measuring $group + $count:{} aggregation performance."""

    wait_time = between(0.01, 0.05)

    def _build_pipeline(self):
        """Build the $group + $count:{} aggregation pipeline."""
        pipeline = []
        if self.match_filter:
            pipeline.append({"$match": self.match_filter})
        pipeline.append({"$group": {"_id": "null", "count": {"$count": {}}}})
        return pipeline

    @task
    def group_count(self):
        """Run $group + $count:{} aggregation and measure latency."""
        if self.fail_if_sharding_error("group_count"):
            return
        with self.timed_operation("group_count"):
            list(self.collection.aggregate(self._build_pipeline()))
