"""
TTL Query Impact Benchmark — concurrent queries during TTL deletion.

Extends the TTL deletion benchmark with concurrent aggregation tasks
that run while the TTL monitor deletes expired documents. This measures
the performance impact of background TTL deletion on query workloads.

Query tasks:
- ``match_eq_group``: ``$match`` with ``$eq`` on ``category`` → ``$group.$sum``
- ``match_range_group``: ``$match`` with ``$gte/$lte`` on ``value`` → ``$group.$sum``
- ``estimated_doc_count``: Lightweight count to observe the shrinking collection

These are reported as standard Locust request metrics (latency, RPS) so
the framework's existing reporting captures query degradation during TTL
deletion automatically.

Workload parameters (in addition to those from TTLDeletionBenchmarkUser):
    match_eq_group_weight: int    - weight for eq+group aggregation (default: 2, 0 to disable)
    match_range_group_weight: int - weight for range+group aggregation (default: 2, 0 to disable)
    estimated_count_weight: int   - weight for estimated count (default: 1, 0 to disable)
    poll_deletion_weight: int     - weight for deletion poll (default: 1, 0 to disable)
"""

import logging
import random

from locust import task, between

from benchmark_runner.benchmarks.ttl.ttl_deletion_benchmark import TTLDeletionBenchmarkUser

logger = logging.getLogger(__name__)

# Categories used in queries (must match document generator values).
_CATEGORIES = ["A", "B", "C", "D", "E"]


class TTLQueryImpactBenchmarkUser(TTLDeletionBenchmarkUser):
    """Benchmark user that runs concurrent queries during TTL deletion."""

    wait_time = between(0, 0.1)

    def on_start(self):
        super().on_start()
        self.match_eq_group_weight = self.get_param("match_eq_group_weight", 2)
        self.match_range_group_weight = self.get_param("match_range_group_weight", 2)
        self.estimated_count_weight = self.get_param("estimated_count_weight", 1)

    @task(2)
    def match_eq_group(self):
        """Run $match + $group aggregation with equality filter on category.

        Pipeline: ``[{$match: {category: <random>}}, {$group: {_id: null, total: {$sum: "$value"}}}]``
        """
        if self.match_eq_group_weight == 0:
            return
        if self.fail_if_sharding_error("match_eq_group"):
            return

        category = random.choice(_CATEGORIES)
        pipeline = [
            {"$match": {"category": category}},
            {"$group": {"_id": None, "total": {"$sum": "$value"}}},
        ]

        collections = self.__class__._collections or [self.collection]
        coll = random.choice(collections)

        with self.timed_operation("match_eq_group"):
            list(coll.aggregate(pipeline))

    @task(2)
    def match_range_group(self):
        """Run $match + $group aggregation with range filter on value.

        Pipeline: ``[{$match: {value: {$gte: lo, $lte: hi}}}, {$group: {_id: "$category", total: {$sum: 1}}}]``
        """
        if self.match_range_group_weight == 0:
            return
        if self.fail_if_sharding_error("match_range_group"):
            return

        lo = random.uniform(0, 800)
        hi = lo + random.uniform(50, 200)
        pipeline = [
            {"$match": {"value": {"$gte": lo, "$lte": hi}}},
            {"$group": {"_id": "$category", "total": {"$sum": 1}}},
        ]

        collections = self.__class__._collections or [self.collection]
        coll = random.choice(collections)

        with self.timed_operation("match_range_group"):
            list(coll.aggregate(pipeline))

    @task(1)
    def estimated_doc_count(self):
        """Run estimated_document_count to observe collection shrinking."""
        if self.estimated_count_weight == 0:
            return
        if self.fail_if_sharding_error("estimated_doc_count"):
            return

        collections = self.__class__._collections or [self.collection]
        coll = random.choice(collections)

        with self.timed_operation("estimated_doc_count"):
            coll.estimated_document_count()
