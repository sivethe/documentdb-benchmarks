"""
Insert Benchmark — unique index on ``createdAt``.

Measures insert throughput on a collection that has a unique ascending
index on the ``createdAt`` field in addition to the default ``_id``
index.  Unique indexes add the overhead of a duplicate-key check on
every insert, so this benchmark captures the additional cost compared
to a plain (non-unique) secondary index.

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

logger = logging.getLogger(__name__)


class InsertUniqueIndexBenchmarkUser(MongoUser):
    """Benchmark user that measures insert performance with a unique index on createdAt."""

    wait_time = between(0, 0.01)

    def on_start(self):
        super().on_start()
        self.document_size = self.get_param("document_size", 256)
        self.batch_size = self.get_param("batch_size", 100)
        self.insert_one_weight = self.get_param("insert_one_weight", 3)
        self.insert_many_weight = self.get_param("insert_many_weight", 1)
        self.run_once_across_all_users(self._setup)

    def _setup(self):
        """Drop, configure sharding, create index, and capture indexes."""
        if self.get_param("drop_on_start", True):
            self.collection.drop()
        self._setup_sharding()
        self.create_indexes({"createdAt": 1}, name="idx_createdAt_unique", unique=True)
        self._wait_for_index_builds()
        self._capture_indexes()

    @task(3)
    def insert_one_uniqueIndex(self):
        """Insert a single document and measure latency."""
        if self.insert_one_weight == 0:
            return
        if self.fail_if_sharding_error("insert_one_uniqueIndex"):
            return
        doc = self.generate_document(self.document_size)
        with self.timed_operation("insert_one_uniqueIndex"):
            self.collection.insert_one(doc)

    @task(1)
    def insert_many_uniqueIndex(self):
        """Insert a batch of documents and measure latency."""
        if self.insert_many_weight == 0:
            return
        if self.fail_if_sharding_error("insert_many_uniqueIndex"):
            return
        docs = [self.generate_document(self.document_size) for _ in range(self.batch_size)]
        with self.timed_operation("insert_many_uniqueIndex"):
            self.collection.insert_many(docs)
