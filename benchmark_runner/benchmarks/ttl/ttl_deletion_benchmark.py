"""
TTL Deletion Benchmark — monitors TTL document deletion speed.

Measures how quickly a MongoDB-compatible database deletes expired
documents after a TTL index is created. This is the core TTL benchmark
that seeds a collection with a configurable mix of expired and alive
documents, creates a TTL index, and polls the collection count until
all expired documents are removed (or a timeout is reached).

Supports multi-collection mode (1–100 collections) and optional extra
indexes (simple, composite, unique, wildcard) that add write overhead
to the TTL deletion process.

Workload parameters (set in config YAML under workload_params):
    num_docs: int           - documents per collection (default: 100000)
    expired_pct: float      - percentage of docs that are expired (default: 10.0)
    document_size: int      - approximate document size in bytes (default: 256)
    num_collections: int    - number of collections to operate on (default: 1)
    with_extra_indexes: bool - create 4 extra indexes (default: false)
    expire_after_minutes: int - delay before docs expire (default: 0 = immediate)
    poll_interval: float    - seconds between count polls (default: 5.0)
    ttl_timeout: int        - max seconds to wait for deletion (default: 3600)
    drop_on_start: bool     - drop collections before seeding (default: true)
    poll_deletion_weight: int - task weight for poll task (default: 1, 0 to disable)
"""

import logging
import threading
import time
from typing import Any, List, Optional

from locust import task, constant

from benchmark_runner.base_benchmark import MongoUser
from benchmark_runner.benchmarks.ttl.ttl_common import (
    create_extra_indexes,
    create_ttl_index,
    seed_ttl_collection,
)

logger = logging.getLogger(__name__)


class TTLDeletionBenchmarkUser(MongoUser):
    """Benchmark user that monitors TTL deletion progress."""

    wait_time = constant(5)  # Overridden by poll_interval in on_start

    # Class-level state for TTL monitoring (shared across users).
    # Each subclass gets its own copy via __init_subclass__.
    _ttl_start_time: Optional[float] = None
    _ttl_first_delete_time: Optional[float] = None
    _initial_expired_count: int = 0
    _target_count: int = 0
    _ttl_complete: bool = False
    _collections: Optional[List[Any]] = None
    _extra_seed_done: bool = False

    def __init_subclass__(cls, **kwargs):
        """Give each subclass its own TTL monitoring state."""
        super().__init_subclass__(**kwargs)
        cls._ttl_start_time = None
        cls._ttl_first_delete_time = None
        cls._initial_expired_count = 0
        cls._target_count = 0
        cls._ttl_complete = False
        cls._collections = None
        cls._extra_seed_done = False

    def on_start(self):
        """Set up MongoDB connection, seed documents, create indexes."""
        super().on_start()

        # Read workload params
        self.num_docs = self.get_param("num_docs", 100000)
        self.expired_pct = self.get_param("expired_pct", 10.0)
        self.document_size = self.get_param("document_size", 256)
        self.num_collections = self.get_param("num_collections", 1)
        self.with_extra_indexes = self.get_param("with_extra_indexes", False)
        self.expire_after_minutes = self.get_param("expire_after_minutes", 0)
        self.poll_interval = self.get_param("poll_interval", 5.0)
        self.ttl_timeout = self.get_param("ttl_timeout", 3600)
        self.poll_deletion_weight = self.get_param("poll_deletion_weight", 1)

        # Override Locust wait_time with configured poll interval.
        # constant() returns a descriptor; assigning to self bypasses binding,
        # so wrap in a plain lambda to avoid the missing-argument error.
        poll = self.poll_interval
        self.wait_time = lambda: poll

        # Set up collections
        self._setup_collections()

        # Seed and create indexes (once across all users)
        self.seed_collection(self._seed_and_index, drop=self.get_param("drop_on_start", True))

    def _setup_collections(self):
        """Initialize the list of collections for multi-collection mode."""
        if self.__class__._collections is not None:
            return

        collections = []
        if self.num_collections <= 1:
            collections.append(self.collection)
        else:
            base_name = self.collection.name
            for i in range(self.num_collections):
                coll_name = f"{base_name}_{i}"
                collections.append(self.db[coll_name])

        self.__class__._collections = collections

    def _seed_and_index(self):
        """Seed all collections and create TTL + extra indexes."""
        db_engine = self.config.database_engine if self.config else ""
        collections = self.__class__._collections or [self.collection]

        total_expired = 0
        total_alive = 0

        for coll in collections:
            # Drop if needed (base seed_collection already drops self.collection,
            # but we need to handle extra collections)
            if coll.name != self.collection.name:
                try:
                    coll.drop()
                except Exception:
                    pass

            # Seed documents
            counts = seed_ttl_collection(
                collection=coll,
                num_docs=self.num_docs,
                expired_pct=self.expired_pct,
                document_size=self.document_size,
                expire_after_minutes=self.expire_after_minutes,
            )
            total_expired += counts["expired_count"]
            total_alive += counts["alive_count"]

            # Create extra indexes if enabled
            if self.with_extra_indexes:
                create_extra_indexes(coll, database_engine=db_engine)

            # Create TTL index — this starts the deletion clock
            create_ttl_index(coll, database_engine=db_engine)

        # Record TTL monitoring state
        self.__class__._ttl_start_time = time.monotonic()
        self.__class__._initial_expired_count = total_expired
        self.__class__._target_count = total_alive
        self.__class__._ttl_complete = False
        self.__class__._ttl_first_delete_time = None

        logger.info(
            "TTL monitoring started: %d expired docs across %d collection(s), "
            "target remaining = %d",
            total_expired,
            len(collections),
            total_alive,
        )

    def _signal_early_stop(self):
        """Signal the runner to stop early if a stop_event is available."""
        stop_event = getattr(self.environment, "stop_event", None)
        if stop_event is not None:
            stop_event.set()

    @task(1)
    def poll_deletion_progress(self):
        """Poll collection counts to track TTL deletion progress.

        Reports ``ttl_poll`` latency for the count operation and
        ``ttl_remaining_docs`` as a custom metric for tracking deletion
        progress over time.

        Detects the first deletion event and records the time-to-first-delete.
        When all expired docs are deleted (or timeout is reached), sets
        the ``_ttl_complete`` flag.
        """
        if self.poll_deletion_weight == 0:
            return
        if self.fail_if_sharding_error("ttl_poll"):
            return
        if self.__class__._ttl_complete:
            return

        collections = self.__class__._collections or [self.collection]
        total_count = 0

        with self.timed_operation("ttl_poll"):
            for coll in collections:
                total_count += coll.count_documents({})

        # Track remaining docs as a custom metric
        self.track_custom_metric("ttl_remaining_docs", total_count)

        target = self.__class__._target_count
        initial_total = self.__class__._initial_expired_count + target

        # Detect first deletion
        if self.__class__._ttl_first_delete_time is None and total_count < initial_total:
            self.__class__._ttl_first_delete_time = time.monotonic()
            elapsed = self.__class__._ttl_first_delete_time - self.__class__._ttl_start_time
            logger.info(
                "First TTL deletion detected after %.1fs (count: %d -> %d)",
                elapsed,
                initial_total,
                total_count,
            )
            self.track_custom_metric("ttl_time_to_first_delete_ms", elapsed * 1000)

        # Check completion
        if total_count <= target:
            self.__class__._ttl_complete = True
            elapsed = time.monotonic() - self.__class__._ttl_start_time
            logger.info(
                "TTL deletion complete: %d docs remaining (target: %d) after %.1fs",
                total_count,
                target,
                elapsed,
            )
            self.track_custom_metric("ttl_total_deletion_time_ms", elapsed * 1000)

            # Calculate active deletion time
            if self.__class__._ttl_first_delete_time is not None:
                active_time = time.monotonic() - self.__class__._ttl_first_delete_time
                self.track_custom_metric("ttl_active_deletion_time_ms", active_time * 1000)

            deleted = self.__class__._initial_expired_count
            if elapsed > 0 and deleted > 0:
                rate = deleted / elapsed
                self.track_custom_metric("ttl_deletion_rate_docs_per_sec", rate)
                logger.info("Deletion rate: %.0f docs/sec", rate)

            self._signal_early_stop()

        # Check timeout
        elif (time.monotonic() - self.__class__._ttl_start_time) > self.ttl_timeout:
            self.__class__._ttl_complete = True
            logger.warning(
                "TTL deletion timed out after %ds. Remaining: %d (target: %d)",
                self.ttl_timeout,
                total_count,
                target,
            )
            self.track_custom_failure(
                "ttl_timeout",
                RuntimeError(
                    f"TTL deletion timed out after {self.ttl_timeout}s. "
                    f"Remaining: {total_count}, target: {target}"
                ),
            )
            self._signal_early_stop()
