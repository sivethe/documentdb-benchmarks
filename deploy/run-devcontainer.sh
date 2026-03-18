#!/usr/bin/env bash
# deploy/run-devcontainer.sh — Run benchmarks inline inside the devcontainer.
#
# Like run-local.sh but runs benchmarks directly as Python processes
# instead of spinning up Docker containers. Intended for use inside the
# devcontainer where the package is already installed in editable mode.
#
# Prerequisites:
#   - Running inside the devcontainer (pip install -e '.[dev]' already done)
#
# Usage:
#   ./deploy/run-devcontainer.sh deploy/pipeline.config
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/pipeline-common.sh"

# ---------- usage ----------
USAGE="Usage: $0 <pipeline.config>

Runs benchmarks directly inside the devcontainer (no Docker).

The config file specifies database engines and benchmark configs.
See deploy/pipeline.config for an example.

  -h, --help    Show this help"

# ---------- parse config ----------
resolve_config_path "$@"
cd "$PROJECT_ROOT"
parse_pipeline_args "$USAGE" "${RESOLVED_ARGS[@]}"

# ---------- preflight ----------
if ! command -v bench-run &>/dev/null; then
    echo "ERROR: bench-run command not found."
    echo "  Install the package first: pip install -e '.[dev]'"
    exit 1
fi

# ---------- prepare results directory ----------
mkdir -p "$RESULTS_RUN_DIR"
log "Results will be saved to: $RESULTS_RUN_DIR"

# ---------- run benchmarks ----------
run_benchmark() {
    local config_basename="$1"
    local engine_name="$2"
    local mongodb_url="$3"
    local engine_results_dir="$4"

    log "[$engine_name] Running benchmark: $config_basename"

    bench-run \
        --config "config/$config_basename" \
        --mongodb-url "$mongodb_url" \
        --database-engine "$engine_name" \
        --output-dir "$engine_results_dir" \
        $CONCURRENCY_ARGS $EXTRA_ARGS

    local exit_code=$?
    if [[ $exit_code -eq 0 ]]; then
        log "[$engine_name] Benchmark $config_basename completed successfully."
    else
        log "[$engine_name] WARNING: $config_basename exited with code $exit_code"
    fi
}

# ---------- execute ----------
print_summary

for i in "${!ENGINE_NAMES[@]}"; do
    engine_name="${ENGINE_NAMES[$i]}"
    engine_url="${ENGINE_URLS[$i]}"
    engine_dir="$RESULTS_RUN_DIR/$engine_name"

    mkdir -p "$engine_dir"

    log "=== Engine: $engine_name ==="
    log "Connection: $(mask_url "$engine_url")"
    log "Results: $engine_dir"
    echo ""

    for cfg in "${CONFIG_BASENAMES[@]}"; do
        run_benchmark "$cfg" "$engine_name" "$engine_url" "$engine_dir"
    done

    log "Organizing results for $engine_name..."
    organize_results "$engine_dir"
    echo ""
done

print_done
