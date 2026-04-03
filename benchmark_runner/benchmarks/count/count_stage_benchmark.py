"""
Count Benchmark — ``$count`` stage aggregation pipeline.

Measures aggregation performance using the ``$count`` pipeline stage,
which returns the total number of documents without grouping.  This
serves as a baseline for comparing against ``$group``-based counting
strategies.

Pipeline: ``[{$count: "total"}]``

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


class CountStageBenchmarkUser(CountBenchmarkUser):
    """Benchmark user measuring $count stage aggregation performance."""

    wait_time = between(0.01, 0.05)

    def _build_pipeline(self):
        """Build the $count stage aggregation pipeline."""
        pipeline = []
        if self.match_filter:
            pipeline.append({"$match": self.match_filter})
        pipeline.append({"$count": "total"})
        return pipeline

    @task
    def count_stage(self):
        """Run $count stage aggregation and measure latency."""
        if self.fail_if_sharding_error("count_stage"):
            return
        with self.timed_operation("count_stage"):
            list(self.collection.aggregate(self._build_pipeline()))
