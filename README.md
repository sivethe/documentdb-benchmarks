# DocumentDB Benchmarks

A framework for benchmarking MongoDB-compatible databases using [Locust](https://locust.io/). Compare performance across different database engines (MongoDB, Atlas, Azure DocumentDB, AWS DocumentDB) and deployment configurations (single-node, sharded, replicated).

## Project Structure

```
documentdb-benchmarks/
├── benchmark_runner/           # Run benchmarks and collect metrics
│   ├── runner.py               # Main runner - orchestrates Locust execution
│   ├── config.py               # YAML config loading + CLI argument parsing
│   ├── base_benchmark.py       # MongoUser base class for all benchmarks
│   ├── data_generators/        # Shared document generators (used by all benchmarks)
│   │   └── document_256byte.py # ~256-byte documents with standard field schema
│   └── benchmarks/             # Individual benchmark definitions
│       └── insert/             # Insert (write) performance variants
│           ├── insert_no_index_benchmark.py
│           ├── insert_single_path_index_benchmark.py
│           ├── insert_composite_index_benchmark.py
│           ├── insert_wildcard_index_benchmark.py
│           └── insert_unique_index_benchmark.py
├── benchmark_analyzer/         # Analyze and compare results across runs
│   ├── analyzer.py             # CLI for analysis and comparison
│   ├── report_loader.py        # Load Locust CSV + metadata files
│   ├── comparator.py           # Compare runs across scenarios or databases
│   └── report_generator.py     # Generate console/HTML/CSV reports
├── config/                     # Example configuration files
│   └── insert/                 # Insert benchmark configs
│       ├── insert_base.yaml
│       ├── insert_no_index.yaml
│       ├── insert_single_path_index.yaml
│       ├── insert_composite_index.yaml
│       ├── insert_wildcard_index.yaml
│       ├── insert_unique_index.yaml
│       └── *_sharded.yaml      # Sharded variants for each
├── pyproject.toml
└── README.md
```

## Quick Start

### Installation

```bash
# Clone and install
cd documentdb-benchmarks
pip install -e '.[dev]'

# Enable the pre-commit hook to prevent accidental credential commits
git config core.hooksPath .githooks
```

> **Dev container users:** Both steps above run automatically via `postCreateCommand` — no manual setup needed.

### Running a Benchmark

```bash
# Using a config file (recommended)
python -m benchmark_runner --config config/insert/insert_no_index.yaml \
    --database-engine mongodb

# With CLI overrides
python -m benchmark_runner --config config/insert/insert_unique_index.yaml \
    --database-engine mongodb \
    --mongodb-url "mongodb://myhost:27017" \
    --users 20 \
    --run-time 120s

# Using the installed entry point
bench-run --config config/insert/insert_no_index.yaml --database-engine mongodb
```

### Analyzing Results

```bash
# View comparison on console
python -m benchmark_analyzer.analyzer --results-dir results/insert

# List all discovered runs
python -m benchmark_analyzer.analyzer --results-dir results/ --list-runs

# Compare across database engines
python -m benchmark_analyzer.analyzer --results-dir results/insert \
    --group-by database_engine --output insert_comparison.html

# Compare across configurations
python -m benchmark_analyzer.analyzer --results-dir results/insert \
    --group-by run_label --output config_comparison.html

# Export as CSV for spreadsheet analysis
python -m benchmark_analyzer.analyzer --results-dir results/ \
    --format csv --output comparison.csv

# Using the installed entry point
bench-analyze --results-dir results/insert --output report.html
```

## Writing Custom Benchmarks

See [CONTRIBUTING.md](CONTRIBUTING.md) for a full guide on writing new benchmarks,
the `MongoUser` base class API, running tests, and code style guidelines.

## Output Files

Each benchmark run generates:

| File | Description |
|------|-------------|
| `{prefix}_stats.csv` | Summary statistics per operation |
| `{prefix}_stats_history.csv` | Time-series statistics (every 5s) |
| `{prefix}_failures.csv` | Failure details |
| `{prefix}_report.md` | Markdown report |
| `{prefix}_metadata.json` | Run configuration and metadata (used by analyzer) |

## Cross-Database Comparison Workflow

1. **Define benchmark once** — write a benchmark module and base config
2. **Run against each target** — execute with different connection strings and labels:
   ```bash
   # MongoDB
   bench-run -c config/insert/insert_unique_index.yaml \
       --mongodb-url "mongodb://mongo:27017" \
       --database-engine mongodb --run-label "MongoDB 7.0"

   # Azure DocumentDB
   bench-run -c config/insert/insert_unique_index.yaml \
       --mongodb-url "mongodb://azure:10255/?ssl=true" \
       --database-engine azure-documentdb --run-label "Azure DocumentDB"

   # Atlas
   bench-run -c config/insert/insert_unique_index.yaml \
       --mongodb-url "mongodb+srv://atlas.example.net" \
       --database-engine atlas --run-label "Atlas M10"
   ```
3. **Compare results** — generate a unified comparison report:
   ```bash
   bench-analyze -d results/insert --group-by database_engine -o comparison.md
   ```

## Remote / Cloud Deployment

Both deployment scripts read a shared `deploy/pipeline.config` file that defines
database engines, benchmark configs, and environment-specific settings.

### Local Docker

Runs benchmarks in Docker containers on the local machine:

```bash
./deploy/run-local.sh deploy/pipeline.config
```

### Azure Container Instances (zero VM setup)

Runs benchmarks serverlessly in ACI. Requires the Azure CLI (`az login`):

```bash
./deploy/run-aci.sh deploy/pipeline.config
```

Configure database engines and benchmarks in `deploy/pipeline.config`:

```ini
# Global settings
cpu=2
memory=4g
results_dir=./results

# Docker-specific
[docker]
network=auto

# ACI-specific
[aci]
resource_group=benchmarks-rg
location=eastus

# Database engines to benchmark against
[database_engines]
mongodb=mongodb://mongodb:27017
# atlas=mongodb+srv://user:pass@cluster.mongodb.net

# Benchmarks to run (one per line — config/ is prepended automatically)
[benchmarks]
insert/insert_no_index.yaml
insert/insert_unique_index.yaml
```

Results are organized by engine under a timestamped run directory:
`./results/YYYYMMDD-NNN/<engine_name>/`.

## Configuration Reference

| Field | CLI Flag | Default | Description |
|-------|----------|---------|-------------|
| `mongodb_url` | `--mongodb-url` | `mongodb://localhost:27017` | Connection string |
| `database` | `--database` | `benchmark_db` | Database name |
| `collection` | `--collection` | `benchmark_collection` | Collection name |
| `benchmark_name` | `--benchmark-name` | _(required)_ | Name for this benchmark |
| `benchmark_module` | `--benchmark-module` | _(required)_ | Python module (e.g. `benchmarks.insert_benchmark`) |
| `run_label` | `--run-label` | _(from engine)_ | Label for grouping results |
| `database_engine` | `--database-engine` | _(required)_ | Engine identifier (e.g. `mongodb`, `atlas`, `azure-documentdb`) |
| `users` | `--users` / `-u` | `10` | Concurrent Locust users |
| `spawn_rate` | `--spawn-rate` / `-r` | `5` | Users spawned per second |
| `run_time` | `--run-time` / `-t` | `60s` | Test duration (`60s`, `5m`, `1h`) |
| `output_dir` | `--output-dir` / `-o` | `results` | Output directory |
| `workload_params` | _(config only)_ | `{}` | Benchmark-specific parameters |
| `inherits` | _(config only)_ | _(none)_ | Parent config file (relative path); values are deep-merged |

### Insert Benchmark Variants

All insert benchmarks inherit shared defaults from `config/insert/insert_base.yaml`.
Each variant adds a different index type to isolate its write-path overhead:

| Config | Index | Description |
|--------|-------|-------------|
| `insert_no_index.yaml` | _(none)_ | Baseline — only the default `_id` index |
| `insert_single_path_index.yaml` | `timestamp` ASC | Single-field ascending index |
| `insert_composite_index.yaml` | `(category, timestamp)` ASC | Multi-key compound index |
| `insert_wildcard_index.yaml` | `$**` | Root wildcard index (indexes every field) |
| `insert_unique_index.yaml` | `timestamp` ASC, **unique** | Unique index — adds duplicate-key check overhead |

Each variant also has a `*_sharded.yaml` config that enables collection sharding.

### Insert Benchmark `workload_params`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `document_size` | `256` | Approximate document size in bytes |
| `batch_size` | `100` | Documents per `insert_many` call |
| `drop_on_start` | `true` | Drop collection before starting |
| `sharded` | `false` | Shard the collection before running |
| `shard_key` | `"_id"` | Shard key path (`"_id"` or `"category"`) |
| `insert_one_weight` | `3` | Task weight for `insert_one` (set `0` to disable) |
| `insert_many_weight` | `1` | Task weight for `insert_many` (set `0` to disable) |
