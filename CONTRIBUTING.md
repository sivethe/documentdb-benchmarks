# Contributing to DocumentDB Benchmarks

This guide covers how to write new benchmarks, configure them, and run the test suite.

## Writing Custom Benchmarks

### 1. Create a Benchmark Module

Create a new file under `benchmark_runner/benchmarks/`:

```python
# benchmark_runner/benchmarks/my_benchmark.py
from locust import task, between
from benchmark_runner.base_benchmark import MongoUser

class MyBenchmarkUser(MongoUser):
    wait_time = between(0, 0.01)

    def on_start(self):
        super().on_start()
        # Access workload params from config
        self.my_param = self.get_param("my_param", "default_value")

    @task
    def my_operation(self):
        # Use timed_operation to automatically track metrics
        with self.timed_operation("my_operation"):
            self.collection.find_one({"key": "value"})

    @task
    def another_operation(self):
        # Or track custom metrics manually
        import time
        start = time.perf_counter()
        result = self.collection.update_one(
            {"key": "value"},
            {"$set": {"updated": True}}
        )
        elapsed_ms = (time.perf_counter() - start) * 1000
        self.track_custom_metric("update_one", elapsed_ms)
```

### 2. Create a Config File

```yaml
# config/my_benchmark.yaml
benchmark_name: my_benchmark
benchmark_module: benchmarks.my_benchmark

run_label: single-node

# mongodb_url: "mongodb://localhost:27017"  # defaults to MONGODB_URL env var
database: benchmark_db
collection: my_collection

users: 10
spawn_rate: 5
run_time: "60s"

output_dir: results/my_benchmark

workload_params:
  my_param: "custom_value"
```

### Controlling Which Tasks Run (Task Weights)

When a benchmark defines multiple `@task` methods, each task exposes a
`<task_name>_weight` parameter under `workload_params`. Set a weight to **0** to
disable that task entirely — no code changes required.

For example, the insert benchmarks define `insert_one` (weight 3) and
`insert_many` (weight 1). To run **only** `insert_one`:

```yaml
workload_params:
  insert_many_weight: 0    # disables insert_many
```

To run **only** `insert_many`:

```yaml
workload_params:
  insert_one_weight: 0     # disables insert_one
```

The defaults match the `@task` decorator weights so behaviour is unchanged
unless you explicitly override them.

### Config Inheritance

Configs can inherit from a parent using the `inherits` key. The child only
needs to specify overrides — nested dicts like `workload_params` are
deep-merged rather than replaced:

```yaml
# config/my_benchmark_sharded.yaml
inherits: my_benchmark.yaml      # path relative to this file
benchmark_name: my_benchmark_sharded
run_label: sharded
workload_params:
  sharded: true                  # merged into parent's workload_params
```

Chained inheritance (grandparent → parent → child) is supported.

### 3. Run and Compare

```bash
# Run against MongoDB
python -m benchmark_runner --config config/my_benchmark.yaml \
    --database-engine mongodb

# Run against Azure DocumentDB (modify config or override on CLI)
python -m benchmark_runner --config config/my_benchmark.yaml \
    --database-engine azure-documentdb \
    --mongodb-url "mongodb://..." \
    --run-label azure-single-node

# Compare
python -m benchmark_analyzer \
    --results-dir results/my_benchmark \
    --group-by database_engine \
    --output my_benchmark_comparison.md
```

## The MongoUser Base Class

All benchmarks extend `MongoUser`, which provides:

| Feature | Description |
|---------|-------------|
| `self.collection` | Pre-connected PyMongo collection |
| `self.db` | Pre-connected PyMongo database |
| `self.client` | PyMongo MongoClient instance |
| `self.timed_operation(name)` | Context manager that times operations and reports to Locust stats |
| `self.track_custom_metric(name, ms)` | Report a custom metric manually |
| `self.track_custom_failure(name, exc)` | Report a failure for a custom metric |
| `self.get_param(key, default)` | Access workload parameters from config |
| `self.workload_params` | Full workload params dictionary |

### Task Weight Config Pattern

When a benchmark defines multiple `@task` methods, each task **must** expose a
configurable weight via `workload_params` so users can selectively enable or
disable individual tasks from the YAML config without editing Python code.

Pattern:
1. Define a `<task_name>_weight` workload param with a sensible non-zero default.
2. Read it in `on_start()`: `self.<task_name>_weight = self.get_param("<task_name>_weight", <default>)`.
3. At the top of the `@task` method, return early if weight is 0:
   ```python
   @task(3)
   def my_task(self):
       if self.my_task_weight == 0:
           return
       ...
   ```
4. Document the weight params in the base YAML config and in the module docstring.

Setting a weight to `0` effectively disables the task. This lets users run a
subset of tasks (e.g. only `insert_one` or only `insert_many`) purely via config.
See the insert benchmarks for the reference implementation.

## Running Tests

```bash
# Run the full test suite
pytest

# Run a specific test file
pytest tests/test_insert_benchmarks.py

# Run with verbose output
pytest -v
```

### Testing Guidelines

- **Always run the full test suite (`pytest`) after completing all proposed changes** to verify nothing is broken. Fix any failures before finishing.
- **Write tests for failure/error scenarios**, not just the happy path. Examples:
  - Operations that receive `Exception` objects instead of strings (e.g., Locust stats `.error` field may hold raw exception objects — ensure they are converted with `str()` before serialisation).
  - Setup steps that fail (e.g., `shardCollection` command not supported) — verify that downstream tasks handle the failure gracefully.
  - `None` values in stats fields (e.g., `min_response_time` when there are zero requests).
- **Verify JSON serialisation round-trips** for any dict that will be written with `json.dump`. Non-serialisable types (exceptions, `datetime`, `Path`, custom objects) must be converted to strings or primitives before dumping.
- Test files live in `tests/` and follow the naming pattern `test_<module>.py`.
- Use `unittest.mock.MagicMock` to stub MongoDB clients, Locust environments, and stats objects. See existing tests in `tests/test_insert_benchmarks.py` and `tests/test_runner.py` for patterns.
- Keep class-level state (`_seed_done`, `_extra_seed_done`, `_sharding_error`) reset between tests to ensure isolation.

## Code Style

- **Python 3.9+** compatible (no `X | Y` union syntax; use `Optional[X]` and `Union`).
- **Formatter / Linter:** [Ruff](https://docs.astral.sh/ruff/) — line length 100.
- Type hints on all public functions.
- Docstrings on all public classes and functions (Google style).
- Use `dataclass` for config/data structures.
- Use `pathlib.Path` for file paths.

```bash
# Format code
ruff format .

# Lint
ruff check .
```

## Common Pitfalls

- **Locust events must fire on the environment instance** (`self.environment.events`), NOT on the global `locust.events` module. Using global events will silently drop all stats.
- **Locust error entries can hold raw `Exception` objects**, not just strings. Always call `str()` on `.error` before writing to JSON or displaying in reports.
- **Sharding may not be supported by all engines.** The `shardCollection` admin command can fail on Atlas free-tier, standalone `mongod`, or engines that don't support it. When sharding fails, every subsequent task must report failures via `self.fail_if_sharding_error(op_name)`.
- **Class-level flags are shared across users.** `_seed_done`, `_extra_seed_done`, and `_sharding_error` are on the benchmark class, not the instance. Tests must reset these flags before each test case.
- **`json.dump` will raise `TypeError` for any non-primitive type.** Ensure every value is a `str`, `int`, `float`, `bool`, `None`, `list`, or `dict` before serialising.
