"""
Count Benchmark — ``$group`` + ``$sum: 1`` aggregation pipeline.

Measures aggregation performance using the classic ``$group`` with
``$sum: 1`` counting pattern on a pre-seeded collection.  This is the
most widely used counting idiom across all MongoDB versions.

Pipeline: ``[{$group: {_id: "$category", count: {$sum: 1}}}]``

Workload parameters (set in config YAML under workload_params):
    seed_docs: int      - documents to seed (default: 1000000)
    document_size: int  - approximate document size in bytes (default: 256)
    indexSpec: dict     - index key spec, e.g. {"category": 1} (default: null / no index)
    drop_on_start: bool - drop collection before seeding (default: true)
    match_filter: dict   - optional $match filter prepended to the pipeline (default: none)
    sharded: bool       - enable sharding (default: false)
    shard_key: str      - shard key field (default: "_id")
"""

import logging

from locust import between, task

from benchmark_runner.benchmarks.count.count_common import CountBenchmarkUser

logger = logging.getLogger(__name__)


class CountGroupSumBenchmarkUser(CountBenchmarkUser):
    """Benchmark user measuring $group + $sum:1 aggregation performance."""

    wait_time = between(0.01, 0.05)

    def _build_pipeline(self):
        """Build the $group + $sum:1 aggregation pipeline."""
        pipeline = []
        if self.match_filter:
            pipeline.append({"$match": self.match_filter})
        pipeline.append({"$group": {"_id": "null", "count": {"$sum": 1}}})
        return pipeline

    @task
    def group_sum(self):
        """Run $group + $sum:1 aggregation and measure latency."""
        if self.fail_if_sharding_error("group_sum"):
            return
        with self.timed_operation("group_sum"):
            list(self.collection.aggregate(self._build_pipeline()))
