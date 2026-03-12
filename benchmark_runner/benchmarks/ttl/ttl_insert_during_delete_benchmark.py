"""
TTL Insert-During-Delete Benchmark — concurrent inserts during TTL deletion.

Extends the TTL deletion benchmark with concurrent insert tasks that
create additional expired documents while the TTL monitor is actively
deleting. This measures whether the database can keep up with deletion
when new expired documents are being added simultaneously.

Workload parameters (in addition to those from TTLDeletionBenchmarkUser):
    insert_batch_size: int         - docs per insert_many batch (default: 100)
    insert_expired_batch_weight: int - weight for insert task (default: 2, 0 to disable)
    poll_deletion_weight: int      - weight for deletion poll (default: 1, 0 to disable)
"""

import logging
import random

from locust import task, between

from benchmark_runner.benchmarks.ttl.ttl_common import generate_ttl_document
from benchmark_runner.benchmarks.ttl.ttl_deletion_benchmark import TTLDeletionBenchmarkUser

logger = logging.getLogger(__name__)


class TTLInsertDuringDeleteBenchmarkUser(TTLDeletionBenchmarkUser):
    """Benchmark user that inserts expired documents during TTL deletion."""

    wait_time = between(0, 0.1)

    def on_start(self):
        super().on_start()
        self.insert_batch_size = self.get_param("insert_batch_size", 100)
        self.insert_expired_batch_weight = self.get_param("insert_expired_batch_weight", 2)

    @task(2)
    def insert_expired_batch(self):
        """Insert a batch of already-expired documents.

        Uses ``insert_many(ordered=False)`` for maximum throughput.
        Documents are inserted with ``expireAt`` in the past so they
        are immediately eligible for TTL deletion.
        """
        if self.insert_expired_batch_weight == 0:
            return
        if self.fail_if_sharding_error("insert_expired_batch"):
            return

        docs = [
            generate_ttl_document(expired=True, size_bytes=self.document_size)
            for _ in range(self.insert_batch_size)
        ]

        collections = self.__class__._collections or [self.collection]
        coll = random.choice(collections)

        with self.timed_operation("insert_expired_batch"):
            coll.insert_many(docs, ordered=False)
