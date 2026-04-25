# Contributing to the Benchmark Pipeline Workflow

This document explains how to modify and test
[`.github/workflows/benchmark.yml`](.github/workflows/benchmark.yml) — the
scheduled GitHub Actions workflow that runs benchmarks and publishes results
to the `gh-pages` branch.

## Overview

The workflow:

1. Runs nightly at 02:00 UTC (and on manual `workflow_dispatch`).
2. Generates `deploy/pipeline-ci.config` from
   [`deploy/pipeline-ci.config.template`](deploy/pipeline-ci.config.template)
   by injecting engine connection strings from GitHub Secrets.
3. Executes benchmarks via [`deploy/run-devcontainer.sh`](deploy/run-devcontainer.sh).
4. Publishes the latest results with
   [`deploy/publish_results.py`](deploy/publish_results.py) into the
   `gh-pages` branch under `data/`.

## Required GitHub Secrets

| Secret | Purpose |
|--------|---------|
| `AZURE_DOCUMENTDB_URL` | Azure Cosmos DB (DocumentDB) connection string |

Add more secrets by:

1. Adding an `env:` entry in the **Generate pipeline config** step.
2. Adding a matching `if [[ -n "${NEW_URL:-}" ]]; then engine_lines+="engine=$NEW_URL"$'\n'; fi` block.
3. Documenting the secret in the workflow header comment.

## Local Validation

### 1. Lint the YAML

```bash
pipx run yamllint .github/workflows/benchmark.yml
```

Repo-wide yamllint config lives in [`.yamllint`](.yamllint).

### 2. Validate the workflow schema

`actionlint` is a Go binary. One-time install:

```bash
bash <(curl -s https://raw.githubusercontent.com/rhysd/actionlint/main/scripts/download-actionlint.bash) latest ~/.local/bin
export PATH="$HOME/.local/bin:$PATH"   # add to ~/.bashrc to persist
```

Run:

```bash
actionlint .github/workflows/benchmark.yml
```

`actionlint` also runs `shellcheck` on inline `run:` blocks, catching most
shell bugs before CI.

### 3. Exercise the pipeline scripts directly

Most issues are in the shell and Python logic, not the YAML. Test those
without GitHub Actions:

```bash
# Inside the devcontainer:

# (a) Generate a config from the template, as the workflow does.
#     Engine entries MUST go under [database_engines], not appended at EOF
#     (which would land them inside [benchmarks]).
cp deploy/pipeline-ci.config.template /tmp/ci.config
awk -v url="$MONGODB_URL" '
  { print }
  /^\[database_engines\]$/ { print "azure_documentdb=" url }
' /tmp/ci.config > /tmp/ci.config.tmp && mv /tmp/ci.config.tmp /tmp/ci.config

# (b) Run benchmarks.
./deploy/run-devcontainer.sh /tmp/ci.config

# (c) Publish results to a throwaway data dir.
python deploy/publish_results.py \
  --results-dir "$(ls -1d results/*/ | sort | tail -1)" \
  --output-dir /tmp/gh-pages-data
```

Unit tests for the publisher:

```bash
pytest tests/test_publish_results.py
```

#### Simulating multiple pipeline iterations

To exercise the merge/dedup/index logic of the publisher, run the benchmark
+ publish loop several times in a row. Each `bench-run` stamps a fresh
`start_time`, so every iteration appends a new set of operation entries
(one per measured operation) to the single document at
`/tmp/gh-pages-data/<benchmark>/<engine>.json`.

```bash
ITERATIONS=3
for i in $(seq 1 "$ITERATIONS"); do
  echo "=== Iteration $i/$ITERATIONS ==="
  ./deploy/run-devcontainer.sh /tmp/ci.config

  LATEST_RUN=$(ls -1d results/*/ | sort | tail -1)
  python deploy/publish_results.py \
    --results-dir "$LATEST_RUN" \
    --output-dir /tmp/gh-pages-data
done

OUT=/tmp/gh-pages-data/insert_no_index/azure_documentdb.json

# Distinct run count (one per unique start_time).
jq '[.operations[].timestamp] | unique | length' "$OUT"

# All recorded run timestamps.
jq '[.operations[].timestamp] | unique' "$OUT"

# Total operation entries (runs × operations per run).
jq '.operations | length' "$OUT"

# Idempotence check: republishing the same run must NOT add new operations.
BEFORE=$(jq '.operations | length' "$OUT")
python deploy/publish_results.py \
  --results-dir "$LATEST_RUN" \
  --output-dir /tmp/gh-pages-data
AFTER=$(jq '.operations | length' "$OUT")
echo "operations: $BEFORE -> $AFTER (must be equal)"
```

### 4. Full workflow run with `act` (optional)

[`act`](https://github.com/nektos/act) runs the full workflow locally in
Docker. Must be run from the **host VM**, not from inside the devcontainer
(Docker is not available inside).

```bash
# Install act (host VM)
curl -s https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash

# Secrets file (do NOT commit)
cat > .secrets <<EOF
AZURE_DOCUMENTDB_URL=mongodb://user:pass@account.mongo.cosmos.azure.com:10255/?ssl=true
EOF

# Simulate the scheduled run with a larger runner image
act schedule \
  --secret-file .secrets \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest

# Simulate manual dispatch with a benchmark filter
act workflow_dispatch \
  --secret-file .secrets \
  --input benchmarks=insert/insert_no_index.yaml \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest
```

**Caveats with `act`:**

- The "Commit and push to gh-pages" step will fail locally (no `gh-pages`
  branch, no auth). Comment it out while iterating, or use `--dry-run` to
  only validate job planning.
- `secrets.GITHUB_TOKEN` is empty unless you pass `-s GITHUB_TOKEN=$(gh auth token)`.
- Real benchmark runs take minutes; filter to a single config for fast feedback.

## Final Check Before Opening a PR

```bash
pipx run yamllint .github/workflows/benchmark.yml
actionlint .github/workflows/benchmark.yml
pytest tests/test_publish_results.py
```

Then trigger the workflow once via **Actions → Benchmark Pipeline → Run
workflow** on your PR branch to confirm end-to-end behaviour before merge.

## Common Pitfalls

- **Never commit real connection strings.** Use GitHub Secrets and the
  `pipeline-ci.config.template` placeholders.
- **Quote shell variables** in `run:` blocks (`"$VAR"`). `shellcheck` via
  `actionlint` catches most of these.
- **Don't log the raw connection string.** The existing steps mask it by
  filtering `=mongodb` lines out of the config dump.
- **Keep the job idempotent.** The publisher dedupes operations by
  `(timestamp, operation_name)` within each `<benchmark>/<engine>.json`
  document, so re-running a commit is safe. New runs whose common
  metadata (`users`, `run_time`, `workload_params`, …) doesn't match the
  existing file are skipped with a warning rather than corrupting history.
- **`on:` triggers aren't a boolean.** yamllint's default `truthy` rule is
  already relaxed in [`.yamllint`](.yamllint) — don't re-enable it
  without excluding workflow files.
