"""
Insert Benchmark — wildcard index (``$**``).

Measures insert throughput on a collection that has a root wildcard
index in addition to the default ``_id`` index.  Wildcard indexes must
update entries for every field in the document, so this benchmark
captures the worst-case index-maintenance overhead during writes.

Workload parameters (set in config YAML under workload_params):
    document_size: int  - approximate size of each document in bytes (default: 256)
    batch_size: int     - number of documents per insert_many call (default: 100)
    drop_on_start: bool - whether to drop the collection before starting (default: true)
    insert_one_weight: int - task weight for insert_one (default: 3, set 0 to disable)
    insert_many_weight: int - task weight for insert_many (default: 1, set 0 to disable)
"""

import logging

from locust import task, between

from benchmark_runner.base_benchmark import MongoUser
from benchmark_runner.data_generators.document_256byte import generate_document

logger = logging.getLogger(__name__)


class InsertWildcardIndexBenchmarkUser(MongoUser):
    """Benchmark user that measures insert performance with a ``$**`` wildcard index."""

    wait_time = between(0, 0.01)

    def on_start(self):
        super().on_start()
        self.document_size = self.get_param("document_size", 256)
        self.batch_size = self.get_param("batch_size", 100)
        self.insert_one_weight = self.get_param("insert_one_weight", 3)
        self.insert_many_weight = self.get_param("insert_many_weight", 1)
        self.seed_collection(self._create_index, drop=self.get_param("drop_on_start", True))
        self.run_warmup()

    def _create_index(self):
        """Create a root wildcard index on all fields."""
        logger.info(
            "Creating wildcard index '$**' for %s",
            self.collection.name,
        )
        kwargs = {}
        if self.config and self.config.database_engine == "azure_documentdb":
            kwargs["storageEngine"] = {"enableOrderedIndex": True}
        self.collection.create_index(
            [("$**", 1)],
            name="idx_wildcard",
            **kwargs,
        )

    @task(3)
    def insert_one_wildcardIndex(self):
        """Insert a single document and measure latency."""
        if self.insert_one_weight == 0:
            return
        if self.fail_if_sharding_error("insert_one_wildcardIndex"):
            return
        doc = generate_document(self.document_size)
        with self.timed_operation("insert_one_wildcardIndex"):
            self.collection.insert_one(doc)

    @task(1)
    def insert_many_wildcardIndex(self):
        """Insert a batch of documents and measure latency."""
        if self.insert_many_weight == 0:
            return
        if self.fail_if_sharding_error("insert_many_wildcardIndex"):
            return
        docs = [generate_document(self.document_size) for _ in range(self.batch_size)]
        with self.timed_operation("insert_many_wildcardIndex"):
            self.collection.insert_many(docs)
