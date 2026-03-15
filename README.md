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
│       ├── insert/             # Insert (write) performance variants
│       ├── count/              # Count/aggregation performance variants
├── benchmark_analyzer/         # Analyze and compare results across runs
│   ├── analyzer.py             # CLI for analysis and comparison
│   ├── report_loader.py        # Load Locust CSV + metadata files
│   ├── comparator.py           # Compare runs across scenarios or databases
│   └── report_generator.py     # Generate console/HTML/CSV reports
├── config/                     # Example configuration files
│   ├── insert/                 # Insert benchmark configs
│   ├── count/                  # Count/aggregation benchmark configs
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

# Locust concurrency overrides (optional).
# When set, these override the per-benchmark YAML config values.
# users=10
# spawn_rate=5
# run_time=60s

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
| `imports` | _(config only)_ | _(none)_ | Parent config file (relative path); values are deep-merged |

### Benchmark-Specific Parameters

Each benchmark category defines its own `workload_params` in its base YAML config
(e.g. `config/insert/insert_base.yaml`, `config/count/count_base.yaml`).
Refer to the base config and the benchmark module docstrings for the full list of
available parameters and defaults.
