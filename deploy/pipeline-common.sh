#!/usr/bin/env bash
# deploy/pipeline-common.sh — Shared logic for benchmark pipeline scripts.
#
# Sourced by run-local.sh and run-aci.sh. Provides:
#   - Pipeline config parsing (global settings, [docker], [aci],
#     [database_engines], [benchmarks] sections)
#   - Helper functions (log, organize_results)
#   - Run directory computation (YYYYMMDD-NNN)
#   - Config validation
#
# After sourcing, the following variables are available:
#   PIPELINE_CONFIG       Path to the pipeline config file
#   EXTRA_ARGS            Extra arguments for bench-run
#   RESULTS_LOCAL         Base results directory (before run-dir computation)
#   RESULTS_RUN_DIR       Timestamped run directory
#   CPU                   CPU cores
#   MEMORY                Memory limit
#   USERS                 Locust concurrent users (empty = YAML default)
#   SPAWN_RATE            Locust spawn rate (empty = YAML default)
#   RUN_TIME              Locust run duration (empty = YAML default)
#   CONCURRENCY_ARGS      CLI flags built from USERS/SPAWN_RATE/RUN_TIME
#   IMAGE_NAME            Docker image name
#   IMAGE_TAG             Docker image tag
#   ENGINE_NAMES[]        Array of database engine names
#   ENGINE_URLS[]         Array of database engine connection strings
#   CONFIG_BASENAMES[]    Array of benchmark YAML filenames
#
# Docker-specific (used by run-local.sh):
#   DOCKER_NETWORK        Docker network name or "auto"
#   SKIP_BUILD            Whether to skip image build
#
# ACI-specific (used by run-aci.sh):
#   ACI_RESOURCE_GROUP    Azure resource group
#   ACI_LOCATION          Azure region
#   ACI_ACR_NAME          Azure Container Registry name (empty = auto-create)
#   ACI_CLEANUP           Whether to delete Azure resources after run

# ---------- defaults ----------
EXTRA_ARGS=""
RESULTS_LOCAL="./results"
IMAGE_NAME="documentdb-benchmarks"
IMAGE_TAG="latest"
CPU="2"
MEMORY="4g"

# Locust concurrency defaults (empty = use YAML config defaults)
USERS=""
SPAWN_RATE=""
RUN_TIME=""

# Docker defaults
DOCKER_NETWORK="auto"
SKIP_BUILD=false

# ACI defaults
ACI_RESOURCE_GROUP="benchmarks-rg"
ACI_LOCATION="eastus"
ACI_ACR_NAME=""
ACI_CLEANUP=false

# Database engines: parallel arrays
declare -a ENGINE_NAMES=()
declare -a ENGINE_URLS=()

# Benchmark configs
declare -a CONFIGS=()

# ---------- helpers ----------
log() { echo -e "\033[1;34m==>\033[0m $*"; }

# Mask credentials in a MongoDB connection string for safe logging.
# Example: mongodb+srv://user:pass@host/db  ->  mongodb+srv://***:***@host/db
mask_url() {
    local url="$1"
    echo "$url" | sed -E 's|(mongodb(\+srv)?://)([^@]+)@|\1***:***@|'
}

# ---------- resolve config path ----------
# Resolves the pipeline config file argument to an absolute path from the
# current working directory BEFORE any cd happens. Sets RESOLVED_ARGS array.
# Usage: resolve_config_path "$@"   (call before cd "$PROJECT_ROOT")
resolve_config_path() {
    declare -g -a RESOLVED_ARGS=()
    for arg in "$@"; do
        if [[ "$arg" != -* && -f "$arg" ]]; then
            # Convert relative file path to absolute
            RESOLVED_ARGS+=("$(cd "$(dirname "$arg")" && pwd)/$(basename "$arg")")
        else
            RESOLVED_ARGS+=("$arg")
        fi
    done
}

# ---------- parse pipeline config ----------
# Usage: parse_pipeline_config <path>
parse_pipeline_config() {
    local config_file="$1"
    local current_section=""

    while IFS= read -r line || [[ -n "$line" ]]; do
        # Strip leading/trailing whitespace
        line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        # Skip comments and blank lines
        [[ -z "$line" || "$line" == \#* ]] && continue
        # Detect section headers
        if [[ "$line" == "["*"]" ]]; then
            current_section="$line"
            continue
        fi
        case "$current_section" in
            "[database_engines]")
                local engine_name="${line%%=*}"
                local engine_url="${line#*=}"
                ENGINE_NAMES+=("$engine_name")
                ENGINE_URLS+=("$engine_url")
                ;;
            "[benchmarks]")
                CONFIGS+=("$line")
                ;;
            "[docker]")
                local key="${line%%=*}"
                local value="${line#*=}"
                case "$key" in
                    network)        DOCKER_NETWORK="$value" ;;
                    skip_build)     SKIP_BUILD="$value" ;;
                    *)              echo "WARNING: unknown [docker] key: $key" ;;
                esac
                ;;
            "[aci]")
                local key="${line%%=*}"
                local value="${line#*=}"
                case "$key" in
                    resource_group) ACI_RESOURCE_GROUP="$value" ;;
                    location)       ACI_LOCATION="$value" ;;
                    acr_name)       ACI_ACR_NAME="$value" ;;
                    cleanup)        ACI_CLEANUP="$value" ;;
                    *)              echo "WARNING: unknown [aci] key: $key" ;;
                esac
                ;;
            "")
                # Global key=value settings (before any section)
                local key="${line%%=*}"
                local value="${line#*=}"
                case "$key" in
                    cpu)            CPU="$value" ;;
                    memory)         MEMORY="$value" ;;
                    results_dir)    RESULTS_LOCAL="$value" ;;
                    extra_args)     EXTRA_ARGS="$value" ;;
                    users)          USERS="$value" ;;
                    spawn_rate)     SPAWN_RATE="$value" ;;
                    run_time)       RUN_TIME="$value" ;;
                    *)              echo "WARNING: unknown global key: $key" ;;
                esac
                ;;
            *)
                echo "WARNING: unknown section: $current_section"
                ;;
        esac
    done < "$config_file"
}

# ---------- build concurrency args ----------
# Builds CLI arguments for bench-run from pipeline concurrency settings.
# Only includes flags that are explicitly set (non-empty).
build_concurrency_args() {
    CONCURRENCY_ARGS=""
    if [[ -n "$USERS" ]]; then
        CONCURRENCY_ARGS+="--users $USERS "
    fi
    if [[ -n "$SPAWN_RATE" ]]; then
        CONCURRENCY_ARGS+="--spawn-rate $SPAWN_RATE "
    fi
    if [[ -n "$RUN_TIME" ]]; then
        CONCURRENCY_ARGS+="--run-time $RUN_TIME "
    fi
}

# ---------- validate config ----------
validate_pipeline_config() {
    if [[ ${#ENGINE_NAMES[@]} -eq 0 ]]; then
        echo "ERROR: no engines in [database_engines] section of $PIPELINE_CONFIG"
        exit 1
    fi
    if [[ ${#CONFIGS[@]} -eq 0 ]]; then
        echo "ERROR: no benchmarks in [benchmarks] section of $PIPELINE_CONFIG"
        exit 1
    fi
}

# ---------- compute unique run directory (YYYYMMDD-NNN) ----------
compute_run_dir() {
    local run_date
    run_date=$(date +%Y%m%d)
    if ! mkdir -p "$RESULTS_LOCAL" 2>/dev/null; then
        echo "ERROR: Cannot create results directory: $RESULTS_LOCAL"
        echo "  It may be owned by another user (e.g. root from devcontainer)."
        echo "  Fix with:  sudo chown -R \$(id -u):\$(id -g) $RESULTS_LOCAL"
        exit 1
    fi
    if [[ ! -w "$RESULTS_LOCAL" ]]; then
        echo "ERROR: Results directory is not writable: $RESULTS_LOCAL"
        echo "  It may be owned by another user (e.g. root from devcontainer)."
        echo "  Fix with:  sudo chown -R \$(id -u):\$(id -g) $RESULTS_LOCAL"
        exit 1
    fi
    local seq_num=1
    for d in "$RESULTS_LOCAL"/${run_date}-*/; do
        if [[ -d "$d" ]]; then
            local base num
            base=$(basename "$d")
            num=${base#"${run_date}-"}
            num=${num%%/}
            if [[ "$num" =~ ^[0-9]+$ ]] && (( 10#$num >= seq_num )); then
                seq_num=$((10#$num + 1))
            fi
        fi
    done
    RESULTS_RUN_DIR=$(printf "%s/%s-%03d" "$RESULTS_LOCAL" "$run_date" "$seq_num")
}

# ---------- extract benchmark basenames ----------
extract_config_basenames() {
    declare -g -a CONFIG_BASENAMES=()
    for cfg in "${CONFIGS[@]}"; do
        CONFIG_BASENAMES+=("$cfg")
    done
}

# ---------- organize results ----------
# Moves loose files into matching subdirectories.
organize_results() {
    local target_dir="$1"
    for f in "$target_dir"/*; do
        [[ -f "$f" ]] || continue
        local fname
        fname=$(basename "$f")
        # Skip the combined report
        [[ "$fname" == combined_report* ]] && continue
        # Find the subdirectory whose name is a prefix of the file name
        local moved=false
        for d in "$target_dir"/*/; do
            [[ -d "$d" ]] || continue
            local dname
            dname=$(basename "$d")
            if [[ "$fname" == "${dname}_"* ]]; then
                mv "$f" "$d"
                moved=true
                break
            fi
        done
        # If no matching subdirectory, create one from the metadata prefix
        if [[ "$moved" == false ]]; then
            local prefix
            prefix=$(echo "$fname" | sed -E 's/_(stats_history|stats|exceptions|failures|metadata|report)\.(csv|json|md)$//')
            if [[ -n "$prefix" && "$prefix" != "$fname" ]]; then
                mkdir -p "$target_dir/$prefix"
                mv "$f" "$target_dir/$prefix/"
            fi
        fi
    done
}

# ---------- print summary ----------
print_summary() {
    log "Pipeline: $PIPELINE_CONFIG"
    log "Database engines: ${ENGINE_NAMES[*]}"
    log "Benchmarks per engine: ${#CONFIG_BASENAMES[@]} (${CONFIG_BASENAMES[*]})"
    log "Total runs: $(( ${#ENGINE_NAMES[@]} * ${#CONFIG_BASENAMES[@]} ))"
    echo ""
}

# ---------- print final output ----------
print_done() {
    log "Done! Results saved to $RESULTS_RUN_DIR/"
    log "Directory structure:"
    for engine in "${ENGINE_NAMES[@]}"; do
        echo "  $RESULTS_RUN_DIR/$engine/"
    done
    echo ""
    echo "Generate a combined report for one engine:"
    echo "  python -m benchmark_analyzer -d $RESULTS_RUN_DIR/<engine> --output $RESULTS_RUN_DIR/<engine>/combined_report.md"
    echo ""
    echo "Compare across engines:"
    echo "  python -m benchmark_analyzer -d $RESULTS_RUN_DIR/ --group-by database_engine --output $RESULTS_RUN_DIR/comparison.md"
}

# ---------- CLI entry: parse positional arg ----------
# Usage: parse_pipeline_args "$@"
# Sets PIPELINE_CONFIG and calls parse/validate/extract.
parse_pipeline_args() {
    local usage_text="$1"; shift
    PIPELINE_CONFIG=""
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                echo "$usage_text"
                exit 1
                ;;
            *)
                if [[ -z "$PIPELINE_CONFIG" ]]; then
                    PIPELINE_CONFIG="$1"; shift
                else
                    echo "ERROR: unexpected argument: $1"
                    echo "$usage_text"
                    exit 1
                fi
                ;;
        esac
    done

    if [[ -z "$PIPELINE_CONFIG" ]]; then
        echo "ERROR: pipeline config file is required"
        echo "$usage_text"
        exit 1
    fi
    if [[ ! -f "$PIPELINE_CONFIG" ]]; then
        echo "ERROR: file not found: $PIPELINE_CONFIG"
        exit 1
    fi

    parse_pipeline_config "$PIPELINE_CONFIG"
    validate_pipeline_config
    extract_config_basenames
    build_concurrency_args
    compute_run_dir
}
