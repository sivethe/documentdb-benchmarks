# Contributing to DocumentDB Benchmarks

This guide covers how to write new benchmarks, configure them, and run the test suite.

## Writing Custom Benchmarks

### 1. Create a Benchmark Module

Create a new file under `benchmark_runner/benchmarks/<category>/`:

```python
# benchmark_runner/benchmarks/my_category/my_benchmark.py
from locust import task, between
from benchmark_runner.base_benchmark import MongoUser

class MyBenchmarkUser(MongoUser):
    wait_time = between(0, 0.01)

    def on_start(self):
        super().on_start()
        self.my_param = self.get_param("my_param", "default_value")
        self.run_once_across_all_users(self._setup)

    def _setup(self):
        if self.get_param("drop_on_start", True):
            self.collection.drop()
        self._setup_sharding()
        self.seed_collection(num_docs=1000, document_size=256)
        self.create_indexes({"createdAt": 1})
        self._wait_for_index_builds()
        self._capture_indexes()
        self.capture_explain_plan(self._explain)

    def _build_filter(self) -> dict:
        return {"key": "value"}

    def _explain(self) -> dict:
        return self.db.command(
            "explain",
            {"find": self.collection.name, "filter": self._build_filter()},
            verbosity="allPlansExecution",
        )

    @task
    def my_operation(self):
        if self.fail_if_sharding_error("my_operation"):
            return
        with self.timed_operation("my_operation"):
            self.collection.find_one(self._build_filter())
```

### 2. Create a Config File

```yaml
# config/my_category/my_benchmark.yaml
benchmark_name: my_benchmark
benchmark_module: benchmarks.my_category.my_benchmark

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

### Config Imports

Configs can import from a parent using the `imports` key. The child only
needs to specify overrides — nested dicts like `workload_params` are
deep-merged rather than replaced:

```yaml
# config/my_category/my_benchmark_sharded.yaml
imports: my_benchmark.yaml      # path relative to this file
benchmark_name: my_benchmark_sharded
run_label: sharded
workload_params:
  sharded: true                  # merged into parent's workload_params
```

Chained imports (grandparent → parent → child) are supported; circular
references are detected.

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
| `self.run_once_across_all_users(fn)` | Run a setup function exactly once across all concurrent users |
| `self.save_json(filename, data)` | Save JSON data to the run output directory |
| `self.capture_explain_plan(fn)` | Run a function once and save its return value as the explain plan |
| `self.explain_aggregation(pipeline)` | Convenience wrapper for `db.command("explain", ...)` on an aggregation pipeline |
| `self.seed_collection(num_docs, document_size, batch_size)` | Batch-insert generated documents using `self.generate_document` |
| `self.create_indexes(index_spec, **kwargs)` | Create an index from a key dict (auto-adds `storageEngine` for Azure DocumentDB, skips wildcards) |
| `self.fail_if_sharding_error(name)` | Check for sharding failure and report a Locust failure if so |
| `self._setup_sharding()` | Configure collection sharding from workload_params (call inside setup) |
| `self._wait_for_index_builds()` | Poll until background index builds finish (call inside setup) |
| `self._capture_indexes()` | Capture and save the current index list (call inside setup) |

### Setup Phase

`MongoUser.run_once_across_all_users(setup_func)` runs *setup_func*
exactly once per benchmark class (across all concurrent users).
The runner waits for `_setup_done` before calling
`env.stats.reset_all()`, so any operations performed during setup
are guaranteed to be excluded from measured results.

The setup function is responsible for the full sequence — drop,
sharding, seeding, index creation, warmup — using helper methods
on `MongoUser`:

```python
def _setup(self):
    if self.get_param("drop_on_start", True):
        self.collection.drop()
    self._setup_sharding()
    self.seed_collection(self.seed_docs, self.document_size)
    self.create_indexes(self.indexSpec)
    self._wait_for_index_builds()
    self._capture_indexes()
    self.capture_explain_plan(lambda: self.explain_aggregation(pipeline))
```

### Explain Plan Capture

`MongoUser.capture_explain_plan(explain_func)` runs a callable exactly
once (after seeding) and stores the returned dict. The runner writes it
to `<csv_prefix>_explain.json` in the results directory.  Call it at the
end of `_setup()`, after seeding and index creation.

For aggregation benchmarks, use the built-in `self.explain_aggregation(pipeline)`
method which wraps `db.command("explain", ...)` using the current database
and collection:

```python
def _setup(self):
    # ... drop, seed, indexes ...
    self.capture_explain_plan(
        lambda: self.explain_aggregation(self._build_pipeline())
    )
```

For non-aggregation benchmarks (e.g. find, update), extract the query
into a shared helper (e.g. `_build_filter()`) and define a private
explain method that calls `self.db.command("explain", ...)` directly:

```python
def _setup(self):
    # ... drop, seed, indexes ...
    self.capture_explain_plan(self._explain_find)

def _build_filter(self) -> dict:
    return {"key": "value"}

def _explain_find(self) -> dict:
    return self.db.command(
        "explain",
        {"find": self.collection.name, "filter": self._build_filter()},
        verbosity="allPlansExecution",
    )
```

In both cases, the shared helper (`_build_pipeline()` or `_build_filter()`)
ensures the `@task` method and explain capture use the same logic.

Explain capture is optional and best suited for **read-heavy benchmarks**
(aggregations, finds, count queries) where the query plan is critical for
investigating performance. Insert-only benchmarks typically do not need it.

### Task Weight Config Pattern

When a benchmark defines multiple `@task` methods, each task **must** expose a
configurable weight via `workload_params` so users can selectively enable or
disable individual tasks from the YAML config without editing Python code.

### Benchmark Family Base Class Pattern

When a benchmark category has multiple variants that share the same
lifecycle (param reading, seeding, index creation, warmup) and only
differ in their core operation, introduce an **intermediate abstract
base class** in the category's `*_common.py` module.

This avoids duplicating `on_start()` and `_setup()` across every
variant.  Subclasses only need to implement the operation-specific
method(s) and a `@task`.

**Structure:**

```
benchmark_runner/benchmarks/my_category/
    my_category_common.py       # MyCategoryBenchmarkUser(MongoUser) + helpers
    my_variant_a_benchmark.py   # VariantAUser(MyCategoryBenchmarkUser)
    my_variant_b_benchmark.py   # VariantBUser(MyCategoryBenchmarkUser)
```

**Base class** (in `*_common.py`):

```python
from benchmark_runner.base_benchmark import MongoUser

class MyCategoryBenchmarkUser(MongoUser):
    """Abstract base for my_category benchmarks."""

    abstract = True

    def on_start(self):
        super().on_start()
        self.my_param = self.get_param("my_param", "default")
        # run_once_across_all_users() ensures only one user performs
        # the setup sequence (drop, seed, index, warmup).
        self.run_once_across_all_users(self._setup)

    def _setup(self):
        if self.get_param("drop_on_start", True):
            self.collection.drop()
        self._setup_sharding()
        self.seed_collection(self.seed_docs, self.document_size)
        self.create_indexes(self.indexSpec)
        self._wait_for_index_builds()
        self._capture_indexes()
        self.capture_explain_plan(
            lambda: self.explain_aggregation(self._build_pipeline())
        )

    def _build_pipeline(self):
        raise NotImplementedError
```

**Variant** (in `*_benchmark.py`):

```python
from benchmark_runner.benchmarks.my_category.my_category_common import (
    MyCategoryBenchmarkUser,
)

class VariantAUser(MyCategoryBenchmarkUser):
    wait_time = between(0.01, 0.05)

    def _build_pipeline(self):
        return [{"$group": {"_id": "$field", "count": {"$sum": 1}}}]

    @task
    def variant_a(self):
        if self.fail_if_sharding_error("variant_a"):
            return
        with self.timed_operation("variant_a"):
            list(self.collection.aggregate(self._build_pipeline()))
```

**Key rules:**
- Set `abstract = True` on the intermediate base class so Locust does
  not instantiate it directly.
- Use `self.seed_collection()` and `self.create_indexes()` from the
  `MongoUser` base class — these handle batching, logging, and
  Azure DocumentDB `storageEngine` options automatically.
- See `benchmark_runner/benchmarks/count/count_common.py` for the
  reference implementation (`CountBenchmarkUser`).

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

#### Controlling Which Tasks Run

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

## Data Generators

Reusable document generators live in `benchmark_runner/data_generators/` and are shared
across all benchmark categories. Each generator is a Python module that exposes a
`generate_document(size_bytes: int = <default>) -> dict` function.

Benchmarks select a generator via the `data_generator` workload parameter in their
YAML config.  The `MongoUser` base class resolves the generator at startup and
exposes it as `self.generate_document(size_bytes)`.  This means benchmarks do **not**
need to import a specific generator module — the choice is fully config-driven.

**Naming convention:** `document_<characteristic>.py`

- The file name **must** start with `document_` followed by a descriptive name
  (e.g. `document_standard.py`, `document_nested.py`, `document_arrays.py`).
- Each module exposes a `generate_document(size_bytes: int = <default>) -> dict`
  function with a Google-style docstring.
- Register the module in the `_GENERATORS` dict in
  `benchmark_runner/data_generators/__init__.py` so it can be referenced by
  short name in YAML configs.

| Module | Default size | Short name | Description |
|--------|-------------|------------|-------------|
| `document_standard.py` | 256 B | `standard` | Tiered schema with core scalars (`_id`, `createdAt`, `category`, `value`, `counter`, etc.), progressively adding `tags`, `metadata`, `profile`, `events`, `items`, and `payload` as size increases. Deterministic via `uniqueNumber` seed. |

Benchmarks use the generator resolved by the base class:

```python
# In a benchmark's @task method:
doc = self.generate_document(self.document_size)
```

To use a specific generator in a YAML config:

```yaml
workload_params:
  data_generator: standard   # short name from _GENERATORS registry
  document_size: 512          # size passed to generate_document()
```

To add a new generator, create a file following the naming convention,
register it in `__init__.py`, and add tests in `tests/test_insert_common.py`
or a dedicated test file.

## Testing

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
- Keep class-level state (`_setup_done`, `_sharding_error`, `_explain_done`, `_explain_result`, `_indexes_result`) reset between tests to ensure isolation.

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
- **Sharding may not be supported by all engines.** The `shardCollection` admin command can fail with `CommandNotFound` on Atlas free-tier, standalone `mongod`, or engines that don't support it. When sharding is requested (`sharded: true` in workload params) and the command fails, the error is stored on the class (`_sharding_error`) and every subsequent task must call `self.fail_if_sharding_error(op_name)` to report failures instead of silently running unsharded.
- **Class-level flags are shared across users.** `_setup_done`, `_sharding_error`, `_explain_done`, `_explain_result`, and `_indexes_result` are on the benchmark class, not the instance. Each subclass gets its own copy via `__init_subclass__`, but all instances of the same class share them. Tests must reset these flags before each test case.
- **`json.dump` will raise `TypeError` for any non-primitive type.** Before serialising report dicts, ensure every value is a `str`, `int`, `float`, `bool`, `None`, `list`, or `dict`. Watch for `Exception`, `Path`, `datetime`, and `bytes` objects sneaking into report data.

## Documentation Checklist for New Benchmarks

When adding a new benchmark, update **all** of the following so documentation stays in sync:

1. **Data generator** (if needed) — If the benchmark requires a new document shape, add a generator in `benchmark_runner/data_generators/` following the naming convention `document_<characteristic>.py`. Register it in `benchmark_runner/data_generators/__init__.py`. Add tests in `tests/test_insert_common.py` (or a new `test_data_generators.py` file). Update the generators table above.
2. **Benchmark module** — Create the Python file under `benchmark_runner/benchmarks/<category>/` (e.g. `insert_unique_index_benchmark.py`). Include a module-level docstring listing all `workload_params`. Use `self.generate_document` (resolved from config) instead of importing a generator directly.
3. **YAML configs** — Add a base config in `config/<category>/` that imports from the shared base (e.g. `insert_base.yaml`). Add a `*_sharded.yaml` variant if sharding applies.
4. **Tests** — Add test classes to the relevant test file in `tests/` (e.g. `tests/test_insert_benchmarks.py`). Cover: index creation, Azure `storageEngine` kwargs, seed-once behaviour, task execution, sharding-error handling, weight params, task weights, warmup behaviour, and explain plan capture (if implemented).
5. **README.md** — Only update if adding a **new benchmark category** (e.g. a new
   top-level folder under `benchmarks/` or `config/`). Add the folder to the
   project-structure tree. Do **not** list individual benchmark files or config
   variants in the README — those are discoverable from the filesystem and YAML
   inheritance. Update example commands only if they reference changed paths.
6. **`deploy/pipeline.config`** — Add the new config filename (commented out) under `[benchmarks]` so users can easily enable it.
7. **Run the full test suite** (`pytest`) and fix any failures before finishing.
