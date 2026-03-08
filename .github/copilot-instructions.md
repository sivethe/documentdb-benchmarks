# Copilot Instructions for documentdb-benchmarks

## Project Overview

This is a Python benchmark framework for MongoDB-compatible databases (MongoDB, Atlas, Azure DocumentDB, AWS DocumentDB) using [Locust](https://locust.io/). It has two main packages:

- **`benchmark_runner/`** — Runs benchmarks via Locust programmatically and collects metrics (CSV, Markdown reports, JSON metadata).
- **`benchmark_analyzer/`** — Loads results from multiple runs and generates comparison reports (console, Markdown, CSV).

## Dev Environment

- **Dev container** with Docker Compose: the app runs in a `devcontainer` service; MongoDB 7 runs in a separate `mongodb` service.
- MongoDB is accessible at `mongodb://mongodb:27017` from within the dev container (NOT `localhost`). The `MONGODB_URL` env var is set automatically.
- Docker is **NOT available inside the devcontainer** — `run-local.sh` and `run-aci.sh` must be run from the host VM, not from within the devcontainer.
- Python 3.12 with the package installed in editable mode (`pip install -e '.[dev]'`).
- Formatter: **Ruff** (also used as linter). Line length: **100**. Target: **Python 3.9+**.
- VS Code workspace settings (`.vscode/settings.json`) associate `*.config` files with INI syntax highlighting.

## Key Architecture Details

### Locust Integration (Programmatic API)

Benchmarks run Locust **programmatically** via `locust.env.Environment`, not via the Locust CLI. This is critical:

- **Events must be fired on `self.environment.events`** (the per-environment instance), NOT on the global `locust.events` module. Using global events will silently drop all stats.
- The `MongoUser` base class (`base_benchmark.py`) handles this — always use `self.timed_operation()` or `self.track_custom_metric()` to report stats.
- `StatsCSVFileWriter` requires a `percentiles_to_report` argument.

### Writing Benchmarks

- Create a new file under `benchmark_runner/benchmarks/`.
- Subclass `MongoUser` from `benchmark_runner.base_benchmark`.
- Use `@task` decorators and `self.timed_operation("operation_name")` context manager to time operations.
- Access workload parameters with `self.get_param("key", default)`.
- Configuration is attached to the Locust environment as `env.benchmark_config`.

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

### Configuration

- YAML config files in `config/` are the primary config mechanism for individual benchmarks.
- **Config inheritance**: a YAML config can set `inherits: parent.yaml` (path relative to the child file) to inherit all values from a parent config. The child only needs to specify overrides. Nested dicts like `workload_params` are deep-merged (not replaced). Chained inheritance is supported; circular references are detected.
- CLI arguments override YAML values.
- `BenchmarkConfig` dataclass in `config.py` holds all settings.
- Unknown YAML keys are passed through as `workload_params`.
- Report format: Markdown (`.md`) is the only report format. No HTML reports.

### Report Generation

- The runner generates reports inline (not via the analyzer).
- Markdown reports must have matching column counts in the header, separator, and data rows for VS Code preview rendering.
- Some Locust stats fields can be `None` (e.g., `min_response_time` when there are zero requests) — always handle `None` when formatting values.

## Common Commands

```bash
# Run a benchmark (inside devcontainer)
python -m benchmark_runner --config config/insert_benchmark.yaml --mongodb-url "mongodb://mongodb:27017"

# Analyze results
python -m benchmark_analyzer --results-dir results/insert

# Run tests
pytest

# Format code
ruff format .

# Lint
ruff check .

# Run benchmarks via Docker (from host VM, not devcontainer)
./deploy/run-local.sh deploy/pipeline.config
```

## Code Style

- Python 3.9+ compatible (no `X | Y` union syntax; use `Optional[X]` and `Union`).
- Type hints on all public functions.
- Docstrings on all public classes and functions (Google style).
- Line length: 100 characters.
- Use `dataclass` for config/data structures.
- Use `pathlib.Path` for file paths.

## File Layout Conventions

- Entry points are in `__main__.py` files (delegate to `main()` in the primary module).
- Benchmark YAML configs go in `config/`.
- Results are written to `results/YYYYMMDD-NNN/<engine_name>/`.
- Each run produces: `*_stats.csv`, `*_report.md`, `*_metadata.json`.

## Remote / Cloud Deployment

### Pipeline Config (`deploy/pipeline.config`)

Single INI-style config file shared by both `run-local.sh` and `run-aci.sh`. Sections:

- **(top-level)** — Global settings: `cpu`, `memory`, `results_dir`, `extra_args`.
- **`[docker]`** — Docker-specific: `network` (auto-detected if `auto`), `skip_build`.
- **`[aci]`** — ACI-specific: `resource_group`, `location`, `acr_name`, `cleanup`.
- **`[database_engines]`** — `engine_name=connection_string` pairs. Benchmarks run serially against each engine.
- **`[benchmarks]`** — One benchmark YAML filename per line (basename only; `config/` is prepended by the scripts).

### Deploy Scripts

- **`deploy/pipeline-common.sh`** — Shared shell logic sourced by both run scripts. Provides: config parsing (`parse_pipeline_config`, `parse_pipeline_args`), path resolution (`resolve_config_path`), helpers (`log`, `organize_results`), run-dir computation (`compute_run_dir`), and summary output.
- **`deploy/run-local.sh`** — Runs benchmarks in local Docker containers. Usage: `./deploy/run-local.sh deploy/pipeline.config`.
- **`deploy/run-aci.sh`** — Runs benchmarks on Azure Container Instances. Usage: `./deploy/run-aci.sh deploy/pipeline.config`.
- **`Dockerfile`** — Production image for running benchmarks in Docker or ACI.

### Important Shell Script Patterns

- Both scripts resolve the config path to absolute (`resolve_config_path`) BEFORE `cd "$PROJECT_ROOT"`, ensuring all relative paths (results_dir, Dockerfile, etc.) resolve from the project root regardless of where the script is invoked.
- **Never use `[[ test ]] && { ...; }` under `set -euo pipefail`** — when the test is false, the `&&` short-circuits with exit code 1 and `set -e` kills the script silently with no output. Always use `if/then/fi` instead.
- Container names include the engine name to avoid collisions when running multiple engines.
- Connection string credentials are masked in log output (`${url%%@*}@***`).

## Testing Guidelines

- **Always run the full test suite (`pytest`) after completing all proposed changes** to verify nothing is broken. Fix any failures before finishing.
- **Write tests for failure/error scenarios**, not just the happy path. Examples:
  - Operations that receive `Exception` objects instead of strings (e.g., Locust stats `.error` field may hold raw exception objects — ensure they are converted with `str()` before serialisation).
  - Setup steps that fail (e.g., `shardCollection` command not supported) — verify that downstream tasks handle the failure gracefully.
  - `None` values in stats fields (e.g., `min_response_time` when there are zero requests).
- **Verify JSON serialisation round-trips** for any dict that will be written with `json.dump`. Non-serialisable types (exceptions, `datetime`, `Path`, custom objects) must be converted to strings or primitives before dumping.
- Test files live in `tests/` and follow the naming pattern `test_<module>.py`.
- Use `unittest.mock.MagicMock` to stub MongoDB clients, Locust environments, and stats objects. See existing tests in `tests/test_insert_benchmark.py` and `tests/test_runner.py` for patterns.
- Keep class-level state (`_seed_done`, `_extra_seed_done`, `_sharding_error`) reset between tests to ensure isolation.

## Common Pitfalls

- **Locust error entries can hold raw `Exception` objects**, not just strings. Always call `str()` on `.error` before writing to JSON or displaying in reports.
- **Sharding may not be supported by all engines.** The `shardCollection` admin command can fail with `CommandNotFound` on Atlas free-tier, standalone `mongod`, or engines that don't support it. When sharding is requested (`sharded: true` in workload params) and the command fails, the error is stored on the class (`_sharding_error`) and every subsequent task must call `self.fail_if_sharding_error(op_name)` to report failures instead of silently running unsharded.
- **Class-level flags are shared across users.** `_seed_done`, `_extra_seed_done`, and `_sharding_error` are on the benchmark class, not the instance. Each subclass gets its own copy via `__init_subclass__`, but all instances of the same class share them. Tests must reset these flags before each test case.
- **`json.dump` will raise `TypeError` for any non-primitive type.** Before serialising report dicts, ensure every value is a `str`, `int`, `float`, `bool`, `None`, `list`, or `dict`. Watch for `Exception`, `Path`, `datetime`, and `bytes` objects sneaking into report data.
