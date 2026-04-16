"""
Shared helpers for count/aggregation benchmarks.

Provides collection seeding utilities used by all count benchmark
variants (group_sum, group_count, count_stage).

The count benchmarks measure aggregation counting performance on a
pre-seeded collection using different pipeline strategies.
"""

import logging

from benchmark_runner.base_benchmark import MongoUser

logger = logging.getLogger(__name__)


class CountBenchmarkUser(MongoUser):
    """Base class for count/aggregation benchmarks.

    Provides shared ``on_start()`` lifecycle (param reading, seeding,
    index creation, warmup with explain plan capture).  Subclasses
    only need to implement ``_build_pipeline()`` and a ``@task`` method.
    """

    abstract = True

    def on_start(self):
        """Connect, seed the collection, and create indexes."""
        super().on_start()
        self.seed_docs = self.get_param("seed_docs", 1000000)
        self.document_size = self.get_param("document_size", 256)
        self.indexSpec = self.get_param("indexSpec", None)
        self.match_filter = self.get_param("match_filter", None)
        # run_once_across_all_users() ensures only one user performs
        # the setup sequence (drop, seed, index, warmup).
        self.run_once_across_all_users(self._setup)

    def _setup(self):
        # Drop collection if requested
        if self.get_param("drop_on_start", True):
            self.collection.drop()

        # Configure sharding if enabled
        self._setup_sharding()

        # Seed data and create indexes
        self.seed_collection(self.seed_docs, self.document_size)
        self.create_indexes(self.indexSpec)
        self._wait_for_index_builds()

        # Capture the current indexes for reporting purposes
        self._capture_indexes()

        # Capture the explain plan for the aggregation pipeline
        self.capture_explain_plan(lambda: self.explain_aggregation(self._build_pipeline()))

    def _build_pipeline(self):
        """Build the aggregation pipeline.  Must be overridden by subclasses."""
        raise NotImplementedError
