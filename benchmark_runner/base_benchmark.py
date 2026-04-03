"""
Base benchmark classes for MongoDB-compatible database benchmarks.

Provides a MongoUser base class that handles:
- MongoDB connection lifecycle (connect on start, close on stop)
- Access to the configured database and collection
- Custom metric tracking via Locust events
- Workload parameter access from config
- Thread-safe collection seeding across concurrent users
"""

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from gevent import GreenletExit
import pymongo
from locust import User, between

logger = logging.getLogger(__name__)


class MongoUser(User):
    """
    Base Locust User for MongoDB benchmarks.

    Subclass this and define @task methods to create a benchmark.
    The MongoDB client, database, and collection are available as
    instance attributes after on_start().

    Configuration is passed via the 'benchmark_config' attribute on
    the Locust environment (set by the runner).

    Example:
        class MyBenchmark(MongoUser):
            wait_time = between(0, 0.1)

            @task
            def my_operation(self):
                self.collection.insert_one({"key": "value"})
    """

    # Subclasses can override; default is minimal wait
    wait_time = between(0, 0.01)

    # Abstract: no host needed (we use mongodb_url from config)
    abstract = True

    # Class-level lock for coordinating seed operations across users.
    # Each concrete subclass gets its own lock via __init_subclass__.
    _seed_lock: threading.Lock
    _seed_done: bool = False

    def __init_subclass__(cls, **kwargs):
        """Give each benchmark subclass its own seed lock and flag."""
        super().__init_subclass__(**kwargs)
        cls._seed_lock = threading.Lock()
        cls._seed_done = False
        cls._sharding_error: Optional[str] = None
        cls._explain_lock = threading.Lock()
        cls._explain_done = False
        cls._explain_result: Optional[dict] = None
        cls._indexes_result: Optional[List[dict]] = None
        cls._warmup_lock = threading.Lock()
        cls._warmup_done = False

    def __init__(self, environment):
        super().__init__(environment)
        self.config = getattr(environment, "benchmark_config", None)
        self.client: Optional[pymongo.MongoClient] = None
        self.db = None
        self.collection = None
        self._workload_params: Dict[str, Any] = {}

    def on_start(self):
        """Connect to MongoDB when the user starts."""
        if self.config:
            mongodb_url = self.config.mongodb_url
            database = self.config.database
            collection = self.config.collection
            self._workload_params = self.config.workload_params
        else:
            # Fallback for standalone locust invocation
            mongodb_url = self.host or "mongodb://localhost:27017"
            database = "benchmark_db"
            collection = "benchmark_collection"

        self.client = pymongo.MongoClient(mongodb_url)
        self.db = self.client[database]
        self.collection = self.db[collection]

    def on_stop(self):
        """Close MongoDB connection when the user stops."""
        if self.client:
            self.client.close()

    @property
    def workload_params(self) -> Dict[str, Any]:
        """Access workload-specific parameters from config."""
        return self._workload_params

    def get_param(self, key: str, default: Any = None) -> Any:
        """Get a workload parameter by key with optional default."""
        return self._workload_params.get(key, default)

    def seed_collection(
        self,
        seed_func: Callable[[], None],
        drop: bool = True,
    ) -> None:
        """Run a seed function exactly once across all concurrent users.

        Uses a class-level lock so only the first user to call this
        method performs the drop + seed. All other users block until
        seeding is finished, then proceed directly.

        If workload_params include ``sharded: true``, the collection is
        sharded using the configured ``shard_key`` (default ``"_id"``).
        Supported shard keys: ``"_id"`` and ``"category"``.

        Args:
            seed_func: Callable that performs the actual inserts and
                       index creation. It should use ``ordered=False``
                       on ``insert_many`` calls for safety.
            drop: Whether to drop the collection before seeding.
        """
        with self.__class__._seed_lock:
            if self.__class__._seed_done:
                return

            if drop:
                try:
                    self.collection.drop()
                    logger.info("Dropped collection %s", self.collection.name)
                except Exception:
                    pass

            # Configure sharding if requested
            self._setup_sharding()

            seed_func()

            # Wait for any async index builds to finish.
            self._wait_for_index_builds()

            self.__class__._seed_done = True
            logger.info(
                "Seeding complete for %s (%s docs)",
                self.collection.name,
                self.collection.estimated_document_count(),
            )

    def _wait_for_index_builds(self, poll_interval: float = 2.0, timeout: int = 600) -> None:
        """Poll ``currentOp`` until no index builds are running on the collection.

        Some database engines return from ``createIndex`` before the
        index is fully built.  This method polls the server to make sure
        all background index builds for the current collection have
        finished before the benchmark starts measuring.

        Args:
            poll_interval: Seconds between ``currentOp`` polls.
            timeout: Maximum seconds to wait before giving up.
        """
        collection_name = self.collection.name
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            try:
                current_ops = self.db.current_op({"ns": self.db.name + "." + collection_name})
                ops = current_ops.get("inprog", [])
                index_ops = [
                    op
                    for op in ops
                    if op.get("msg", "").startswith("Index Build")
                    or op.get("command", {}).get("createIndexes") == collection_name
                ]
                if not index_ops:
                    logger.info(
                        "No index builds in progress on %s (waited %.1fs)",
                        collection_name,
                        time.monotonic() - start,
                    )
                    return
                logger.info(
                    "Waiting for %d index build(s) on %s...",
                    len(index_ops),
                    collection_name,
                )
            except Exception as exc:
                # Some engines don't support currentOp or restrict it.
                logger.debug(
                    "currentOp check unavailable, skipping index build wait: %s",
                    exc,
                )
                return
            time.sleep(poll_interval)
        logger.warning(
            "Timed out waiting for index builds on %s after %ds",
            collection_name,
            timeout,
        )

    def _capture_indexes(self) -> None:
        """Run ``list_indexes()`` and store the result on the class.

        The runner writes the captured list to
        ``<csv_prefix>_getIndexes.json`` in the output directory.
        """
        try:
            indexes = [idx for idx in self.collection.list_indexes()]
            self.__class__._indexes_result = indexes
            index_names = [idx.get("name", "?") for idx in indexes]
            logger.info(
                "Indexes on %s: %s",
                self.collection.name,
                index_names,
            )
        except Exception:
            logger.warning("Failed to capture index list", exc_info=True)

    def _setup_sharding(self) -> None:
        """Configure collection sharding if enabled in workload_params.

        Reads ``sharded`` (bool) and ``shard_key`` (str) from workload
        params. When ``sharded`` is true, enables sharding on the
        database and shards the collection using the specified key.

        Supported shard keys:
            - ``"_id"``  — hashed sharding on the default _id field
            - ``"category"`` — hashed sharding on the category field
                (creates a supporting index automatically)

        This is a no-op if ``sharded`` is false or not set, or if the
        database does not support sharding (e.g. standalone mongod).
        """
        sharded = self.get_param("sharded", False)
        if not sharded:
            return

        shard_key_field = self.get_param("shard_key", "_id")
        allowed_keys = {"_id", "category"}
        if shard_key_field not in allowed_keys:
            logger.warning(
                "Unsupported shard_key '%s'. Must be one of %s. Skipping sharding.",
                shard_key_field,
                allowed_keys,
            )
            return

        db_name = self.db.name
        collection_name = self.collection.name
        namespace = f"{db_name}.{collection_name}"

        try:
            # Enable sharding on the database (idempotent on most engines)
            self.client.admin.command("enableSharding", db_name)
            logger.info("Enabled sharding on database: %s", db_name)
        except Exception as exc:
            # Some engines (e.g. DocumentDB) auto-enable or don't need this
            logger.debug("enableSharding skipped or not needed: %s", exc)

        try:
            # Create an index on the shard key if it's not _id
            if shard_key_field != "_id":
                self.collection.create_index([(shard_key_field, "hashed")])
                logger.info("Created hashed index on '%s'", shard_key_field)

            # Shard the collection
            self.client.admin.command(
                "shardCollection",
                namespace,
                key={shard_key_field: "hashed"},
            )
            logger.info(
                "Sharded collection %s on key {%s: 'hashed'}",
                namespace,
                shard_key_field,
            )
        except Exception as exc:
            logger.error("Failed to shard collection %s: %s", namespace, exc)
            self.__class__._sharding_error = f"shardCollection failed for {namespace}: {exc}"

    def run_warmup(self, warmup_func: Optional[Callable[[], None]] = None) -> None:
        """Run warmup actions exactly once, after seeding, before measurement.

        The warmup phase executes after ``seed_collection()`` and before
        the runner resets Locust stats.  Any operations performed here
        (index snapshots, explain plans, cache-warming queries, etc.)
        are guaranteed to be excluded from measured results.

        The base implementation always captures the current index list
        via ``list_indexes()``.  Pass an optional *warmup_func* to
        perform additional one-time actions such as
        ``capture_explain_plan()``.

        Call this at the end of ``on_start()``, **after**
        ``seed_collection()``.

        Args:
            warmup_func: Optional callable that performs additional
                         warmup actions (e.g. explain plan capture).
                         Called exactly once across all concurrent
                         users of the same benchmark class.
        """
        with self.__class__._warmup_lock:
            if self.__class__._warmup_done:
                return
            self._capture_indexes()
            if warmup_func:
                warmup_func()
            self.__class__._warmup_done = True
            logger.info("Warmup complete for %s", self.__class__.__name__)

    def capture_explain_plan(self, explain_func: Callable[[], dict]) -> None:
        """Run an explain function exactly once and store its output.

        Uses a class-level lock so only the first user to call this
        method executes *explain_func*. The returned dict is stored on
        the class and later written to ``<name>_explain.json`` by the
        runner.

        Call this in ``on_start()`` **after** ``seed_collection()``.

        Args:
            explain_func: Callable that returns a dict containing the
                          explain plan output (e.g. from a
                          ``db.command("explain", ...)`` call).
        """
        with self.__class__._explain_lock:
            if self.__class__._explain_done:
                return
            try:
                result = explain_func()
                self.__class__._explain_result = result
                logger.info("Explain plan captured for %s", self.__class__.__name__)
            except Exception:
                logger.warning("Failed to capture explain plan", exc_info=True)
            self.__class__._explain_done = True

    def explain_aggregation(self, pipeline: list) -> dict:
        """Return the explain plan for an aggregation pipeline.

        Convenience wrapper around ``db.command("explain", ...)`` that
        uses the current database and collection.  Typically passed to
        ``capture_explain_plan`` during warmup::

            self.capture_explain_plan(
                lambda: self.explain_aggregation(self._build_pipeline())
            )

        Args:
            pipeline: The aggregation pipeline stages.

        Returns:
            The explain plan output as a dict.
        """
        return self.db.command(
            "explain",
            {
                "aggregate": self.collection.name,
                "pipeline": pipeline,
                "cursor": {},
            },
            verbosity="allPlansExecution",
        )

    def fail_if_sharding_error(self, operation_name: str) -> bool:
        """Check whether sharding setup failed and report a task failure if so.

        Call this at the top of every ``@task`` method in benchmarks
        that require sharding. When a previous ``shardCollection``
        command failed, the method fires a Locust failure event and
        returns ``True`` so the caller can bail out early.

        Args:
            operation_name: The Locust stat name for the task.

        Returns:
            ``True`` if sharding failed (task was recorded as a failure),
            ``False`` otherwise (proceed normally).
        """
        error = self.__class__._sharding_error
        if error is not None:
            self.environment.events.request.fire(
                request_type="mongodb",
                name=operation_name,
                response_time=0,
                response_length=0,
                exception=RuntimeError(error),
                context={},
            )
            return True
        return False

    def track_custom_metric(
        self, metric_name: str, value: float, request_type: str = "custom", response_length: int = 0
    ):
        """
        Report a custom metric to Locust's statistics.

        This integrates with Locust's built-in request tracking so
        custom metrics appear in the standard report alongside
        automatically tracked request timings.

        Args:
            metric_name: Name for the metric (appears in stats table)
            value: Response time in milliseconds
            request_type: Category for the metric (default: "custom")
            response_length: Response size in bytes (default: 0)
        """
        self.environment.events.request.fire(
            request_type=request_type,
            name=metric_name,
            response_time=value,
            response_length=response_length,
            exception=None,
            context={},
        )

    def track_custom_failure(
        self,
        metric_name: str,
        exception: Exception,
        response_time: float = 0,
        request_type: str = "custom",
    ):
        """
        Report a custom metric failure to Locust's statistics.

        Args:
            metric_name: Name for the metric
            exception: The exception that occurred
            response_time: Response time in milliseconds
            request_type: Category for the metric
        """
        self.environment.events.request.fire(
            request_type=request_type,
            name=metric_name,
            response_time=response_time,
            response_length=0,
            exception=exception,
            context={},
        )

    def timed_operation(self, operation_name: str, request_type: str = "mongodb"):
        """
        Context manager for timing MongoDB operations and reporting to Locust.

        Usage:
            with self.timed_operation("insert_one"):
                self.collection.insert_one(doc)

        Args:
            operation_name: Name for the operation (appears in stats)
            request_type: Category for the metric
        """
        return _TimedOperation(operation_name, request_type, self.environment.events)


class _TimedOperation:
    """Context manager that times an operation and reports to Locust."""

    def __init__(self, name: str, request_type: str, events_instance):
        self.name = name
        self.request_type = request_type
        self.events = events_instance
        self.start_time = 0.0

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # GreenletExit is raised when the runner kills user greenlets at
        # shutdown.  It is not a real benchmark failure — skip recording
        # and let it propagate silently.
        if exc_type is not None and issubclass(exc_type, GreenletExit):
            return False

        elapsed_ms = (time.perf_counter() - self.start_time) * 1000
        if exc_type is None:
            self.events.request.fire(
                request_type=self.request_type,
                name=self.name,
                response_time=elapsed_ms,
                response_length=0,
                exception=None,
                context={},
            )
        else:
            self.events.request.fire(
                request_type=self.request_type,
                name=self.name,
                response_time=elapsed_ms,
                response_length=0,
                exception=exc_val,
                context={},
            )
        # Don't suppress exceptions — let them propagate to Locust's
        # task runner which logs them to exceptions stats (with tracebacks).
        return False
