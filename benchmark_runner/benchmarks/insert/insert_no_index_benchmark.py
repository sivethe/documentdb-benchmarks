"""
Insert Benchmark — no secondary index.

Measures raw insert throughput on a collection with only the default
``_id`` index.  Runs ``insert_one`` and ``insert_many`` tasks so the
Locust report captures both single-document and batched latencies
without cross-contamination from other index configurations.

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


class InsertNoIndexBenchmarkUser(MongoUser):
    """Benchmark user that measures insert performance without secondary indexes."""

    wait_time = between(0, 0.01)

    def on_start(self):
        super().on_start()
        self.document_size = self.get_param("document_size", 256)
        self.batch_size = self.get_param("batch_size", 100)
        self.insert_one_weight = self.get_param("insert_one_weight", 3)
        self.insert_many_weight = self.get_param("insert_many_weight", 1)
        self.seed_collection(lambda: None, drop=self.get_param("drop_on_start", True))
        self.run_warmup()

    @task(3)
    def insert_one(self):
        """Insert a single document and measure latency."""
        if self.insert_one_weight == 0:
            return
        if self.fail_if_sharding_error("insert_one"):
            return
        doc = self.generate_document(self.document_size)
        with self.timed_operation("insert_one"):
            self.collection.insert_one(doc)

    @task(1)
    def insert_many(self):
        """Insert a batch of documents and measure latency."""
        if self.insert_many_weight == 0:
            return
        if self.fail_if_sharding_error("insert_many"):
            return
        docs = [self.generate_document(self.document_size) for _ in range(self.batch_size)]
        with self.timed_operation("insert_many"):
            self.collection.insert_many(docs)
