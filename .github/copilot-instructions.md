# Copilot Instructions for documentdb-benchmarks

## Important References

**Before writing or modifying benchmarks, read `CONTRIBUTING.md`** — it is the
canonical source for:
- Writing benchmarks (MongoUser base class, `on_start()` lifecycle, `seed_collection`, `run_warmup`)
- Warmup phase and explain plan capture patterns
- Task weight config pattern
- Data generators (naming conventions, existing generators table)
- Configuration and config imports
- Testing guidelines and test patterns
- Code style
- Common pitfalls
- Documentation checklist for new benchmarks

Do **not** duplicate CONTRIBUTING.md content here. If a pattern or convention
applies to benchmark authoring, testing, or common pitfalls, it belongs in
CONTRIBUTING.md.

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

### Data Generators

Reusable document-generation helpers live in **`benchmark_runner/data_generators/`**.
See CONTRIBUTING.md for naming conventions, the generators table, and import patterns.

### Writing Benchmarks

See CONTRIBUTING.md for the full guide on writing benchmarks, including:
- `MongoUser` base class API
- Warmup phase (`run_warmup`)
- Explain plan capture (`capture_explain_plan`)
- Task weight config pattern
- Data generator usage

### Configuration

- YAML config files in `config/` are the primary config mechanism for individual benchmarks.
- **Config imports**: a YAML config can set `imports: parent.yaml` (path relative to the child file) to import all values from a parent config. The child only needs to specify overrides. Nested dicts like `workload_params` are deep-merged (not replaced). Chained imports are supported; circular references are detected.
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
python -m benchmark_runner --config config/insert/insert_no_index.yaml --mongodb-url "mongodb://mongodb:27017"

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

See CONTRIBUTING.md for the full code style guide. Key points:
- Python 3.9+ compatible (no `X | Y` union syntax; use `Optional[X]` and `Union`).
- Formatter/Linter: **Ruff**. Line length: **100**.
- Use `dataclass` for config/data structures.
- Use `pathlib.Path` for file paths.

## File Layout Conventions

- Entry points are in `__main__.py` files (delegate to `main()` in the primary module).
- Shared document generators live in `benchmark_runner/data_generators/` and follow the naming convention `document_<characteristic>.py` (e.g. `document_standard.py`, `document_nested.py`). Generators are registered by short name in `data_generators/__init__.py` and selected via the `data_generator` workload parameter in YAML configs.
- Benchmark YAML configs go in `config/`.
- Results are written to `results/YYYYMMDD-NNN/<engine_name>/`.
- Each run produces: `*_stats.csv`, `*_report.md`, `*_metadata.json`.

## Remote / Cloud Deployment

### Pipeline Config (`deploy/pipeline.config`)

Single INI-style config file shared by both `run-local.sh` and `run-aci.sh`. Sections:

- **(top-level)** — Global settings: `cpu`, `memory`, `results_dir`, `extra_args`, and Locust concurrency overrides (`users`, `spawn_rate`, `run_time`).
- **`[docker]`** — Docker-specific: `network` (auto-detected if `auto`), `skip_build`.
- **`[aci]`** — ACI-specific: `resource_group`, `location`, `acr_name`, `cleanup`.
- **`[database_engines]`** — `engine_name=connection_string` pairs. Benchmarks run serially against each engine.
- **`[benchmarks]`** — One benchmark YAML filename per line (basename only; `config/` is prepended by the scripts).

**`deploy/pipeline.config` is git-ignored** because it contains connection strings and secrets. The checked-in **`deploy/pipeline.config.template`** has the same structure with placeholder values. A devcontainer `postStartCommand` copies the template to `pipeline.config` on first startup if the file doesn't already exist.

**When adding or changing config variables in `pipeline.config.template`, also update `pipeline.config` if it exists locally** (it won't be committed). Both files must stay in sync structurally.

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

See CONTRIBUTING.md for the complete testing guidelines and test patterns.

- **Always run the full test suite (`pytest`) after completing all proposed changes.**
- Keep class-level state (`_seed_done`, `_extra_seed_done`, `_sharding_error`, `_warmup_done`) reset between tests to ensure isolation.

## Common Pitfalls

See CONTRIBUTING.md for the full list. Key items for AI-assisted development:
- **Events must fire on `self.environment.events`**, NOT on the global `locust.events` module.
- **`json.dump` will raise `TypeError` for any non-primitive type.** Watch for `Exception`, `Path`, `datetime` objects.
- **Class-level flags are shared across users.** Tests must reset these flags before each test case.

## Documentation Checklist for New Benchmarks

See CONTRIBUTING.md for the full checklist. In addition:
- **This file (`.github/copilot-instructions.md`)** — Only update if the new
  benchmark introduces **new generic patterns, conventions, or pitfalls** that
  apply broadly. Do **not** add per-benchmark tables, config lists, or variant
  details here — this file is for framework-level guidance and reference
  patterns, not an inventory of every benchmark.
