#!/usr/bin/env bash
# deploy/run-local.sh — Run benchmarks locally using Docker.
#
# Reads deploy/pipeline.config and runs benchmarks in Docker containers
# for each database engine. Results are organized by engine under
# a timestamped run directory.
#
# Prerequisites:
#   - Docker installed and running
#
# Usage:
#   ./deploy/run-local.sh deploy/pipeline.config
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/pipeline-common.sh"

# ---------- usage ----------
USAGE="Usage: $0 <pipeline.config>

Runs benchmarks locally in Docker containers.

The config file specifies database engines, Docker settings, and benchmark
configs. See deploy/pipeline.config for an example.

  -h, --help    Show this help"

# ---------- parse config ----------
# Resolve config path from original cwd, then cd to project root so all
# relative paths (results_dir, Dockerfile, etc.) resolve consistently.
resolve_config_path "$@"
cd "$PROJECT_ROOT"
parse_pipeline_args "$USAGE" "${RESOLVED_ARGS[@]}"

# ---------- Docker preflight ----------
check_docker() {
    if ! command -v docker &>/dev/null; then
        echo "ERROR: Docker is not installed."
        echo "  Install: https://docs.docker.com/get-docker/"
        exit 1
    fi
    if ! docker info &>/dev/null 2>&1; then
        echo "ERROR: Docker daemon is not running."
        exit 1
    fi
}

detect_network() {
    local net
    net=$(docker network ls --filter name=benchnet --format '{{.Name}}' | head -1)
    if [[ -n "$net" ]]; then
        echo "$net"
        return
    fi
    echo "host"
}

check_docker

FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

# Auto-detect network
if [[ -z "$DOCKER_NETWORK" || "$DOCKER_NETWORK" == "auto" ]]; then
    DOCKER_NETWORK=$(detect_network)
    log "Auto-detected Docker network: $DOCKER_NETWORK"
else
    log "Using Docker network: $DOCKER_NETWORK"
fi

# ---------- build image ----------
if [[ "$SKIP_BUILD" == true ]]; then
    log "Skipping image build (skip_build=true)"
else
    log "Building Docker image: $FULL_IMAGE"
    docker build -t "$FULL_IMAGE" -f Dockerfile .
fi

# ---------- prepare results directory ----------
mkdir -p "$RESULTS_RUN_DIR"
log "Results will be saved to: $RESULTS_RUN_DIR"

# ---------- run benchmarks ----------
run_benchmark_docker() {
    local config_basename="$1"
    local engine_name="$2"
    local mongodb_url="$3"
    local engine_results_abs="$4"
    local run_name="${engine_name}-${config_basename%.yaml}"
    local container_name="bench-${run_name}"
    container_name=$(echo "$container_name" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/-/g' | cut -c1-63)

    log "[$engine_name] Running benchmark: $config_basename (container: $container_name)"

    docker rm -f "$container_name" 2>/dev/null || true

    local network_args=()
    if [[ "$DOCKER_NETWORK" == "host" ]]; then
        network_args=(--network host)
    else
        network_args=(--network "$DOCKER_NETWORK")
    fi

    docker run \
        --name "$container_name" \
        "${network_args[@]}" \
        --cpus "$CPU" \
        --memory "$MEMORY" \
        -v "$engine_results_abs:/app/results" \
        "$FULL_IMAGE" \
        bench-run --config "config/$config_basename" \
            --mongodb-url "$mongodb_url" \
            --database-engine "$engine_name" \
            --output-dir results \
            $CONCURRENCY_ARGS $EXTRA_ARGS

    local exit_code=$?
    if [[ $exit_code -eq 0 ]]; then
        log "[$engine_name] Benchmark $config_basename completed successfully."
    else
        log "[$engine_name] WARNING: $config_basename exited with code $exit_code"
    fi

    docker rm -f "$container_name" 2>/dev/null || true
}

# ---------- execute ----------
print_summary

for i in "${!ENGINE_NAMES[@]}"; do
    engine_name="${ENGINE_NAMES[$i]}"
    engine_url="${ENGINE_URLS[$i]}"
    engine_dir="$RESULTS_RUN_DIR/$engine_name"

    mkdir -p "$engine_dir"
    engine_dir_abs=$(cd "$engine_dir" && pwd)

    log "=== Engine: $engine_name ==="
    log "Connection: $(mask_url "$engine_url")"
    log "Results: $engine_dir"
    echo ""

    for cfg in "${CONFIG_BASENAMES[@]}"; do
        run_benchmark_docker "$cfg" "$engine_name" "$engine_url" "$engine_dir_abs"
    done

    log "Organizing results for $engine_name..."
    organize_results "$engine_dir"
    echo ""
done

print_done
