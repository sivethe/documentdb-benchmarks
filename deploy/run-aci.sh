#!/usr/bin/env bash
# deploy/run-aci.sh — Run benchmarks on Azure Container Instances.
#
# Reads deploy/pipeline.config and runs benchmarks in ACI for each database
# engine. Results are downloaded via an Azure File Share and organized by
# engine under a timestamped run directory.
#
# Prerequisites:
#   - Azure CLI (`az`) installed and logged in (`az login`)
#   - Docker installed locally (to build the image)
#
# Usage:
#   ./deploy/run-aci.sh deploy/pipeline.config
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/pipeline-common.sh"

# ---------- usage ----------
USAGE="Usage: $0 <pipeline.config>

Runs benchmarks on Azure Container Instances.

The config file specifies database engines, ACI settings, and benchmark
configs. See deploy/pipeline.config for an example.

  -h, --help    Show this help"

# ---------- parse config ----------
# Resolve config path from original cwd, then cd to project root.
resolve_config_path "$@"
cd "$PROJECT_ROOT"
parse_pipeline_args "$USAGE" "${RESOLVED_ARGS[@]}"

# ---------- ACI internals ----------
CONTAINER_NAME="bench-runner"
STORAGE_ACCOUNT=""
SHARE_NAME="benchresults"
ACR_NAME_PROVIDED=false
[[ -n "$ACI_ACR_NAME" ]] && ACR_NAME_PROVIDED=true

# ---------- Azure preflight ----------
check_az() {
    if ! command -v az &>/dev/null; then
        echo "ERROR: Azure CLI (az) is not installed."
        echo "  Install: https://learn.microsoft.com/en-us/cli/azure/install-azure-cli"
        exit 1
    fi
    if ! az account show &>/dev/null; then
        echo "ERROR: Not logged in to Azure. Run: az login"
        exit 1
    fi
}

check_az
log "Azure subscription: $(az account show --query name -o tsv)"

# ---------- resource group ----------
if ! az group show --name "$ACI_RESOURCE_GROUP" &>/dev/null; then
    log "Creating resource group: $ACI_RESOURCE_GROUP ($ACI_LOCATION)"
    az group create --name "$ACI_RESOURCE_GROUP" --location "$ACI_LOCATION" -o none
else
    log "Using existing resource group: $ACI_RESOURCE_GROUP"
fi

# ---------- container registry ----------
if [[ "$ACR_NAME_PROVIDED" == false ]]; then
    ACI_ACR_NAME="benchacr$(az account show --query id -o tsv | tr -d '-' | head -c 12)"
fi

if ! az acr show --name "$ACI_ACR_NAME" --resource-group "$ACI_RESOURCE_GROUP" &>/dev/null 2>&1; then
    log "Creating Azure Container Registry: $ACI_ACR_NAME"
    az acr create \
        --resource-group "$ACI_RESOURCE_GROUP" \
        --name "$ACI_ACR_NAME" \
        --sku Basic \
        --admin-enabled true \
        -o none
else
    log "Using existing ACR: $ACI_ACR_NAME"
fi

ACR_LOGIN_SERVER=$(az acr show --name "$ACI_ACR_NAME" --query loginServer -o tsv)
ACR_PASSWORD=$(az acr credential show --name "$ACI_ACR_NAME" --query "passwords[0].value" -o tsv)
FULL_IMAGE="${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${IMAGE_TAG}"

# ---------- build and push image ----------
log "Building and pushing image to $ACR_LOGIN_SERVER..."
az acr login --name "$ACI_ACR_NAME" 2>/dev/null || true
docker build -t "$FULL_IMAGE" -f Dockerfile .
docker push "$FULL_IMAGE"

# ---------- storage account + file share ----------
if [[ -z "$STORAGE_ACCOUNT" ]]; then
    STORAGE_ACCOUNT="benchstor$(az account show --query id -o tsv | tr -d '-' | head -c 12)"
fi

if ! az storage account show --name "$STORAGE_ACCOUNT" --resource-group "$ACI_RESOURCE_GROUP" &>/dev/null 2>&1; then
    log "Creating storage account: $STORAGE_ACCOUNT"
    az storage account create \
        --resource-group "$ACI_RESOURCE_GROUP" \
        --name "$STORAGE_ACCOUNT" \
        --location "$ACI_LOCATION" \
        --sku Standard_LRS \
        -o none
fi

STORAGE_KEY=$(az storage account keys list \
    --resource-group "$ACI_RESOURCE_GROUP" \
    --account-name "$STORAGE_ACCOUNT" \
    --query "[0].value" -o tsv)

if ! az storage share show --name "$SHARE_NAME" --account-name "$STORAGE_ACCOUNT" --account-key "$STORAGE_KEY" &>/dev/null 2>&1; then
    log "Creating file share: $SHARE_NAME"
    az storage share create \
        --name "$SHARE_NAME" \
        --account-name "$STORAGE_ACCOUNT" \
        --account-key "$STORAGE_KEY" \
        -o none
fi

# ---------- run benchmarks per engine ----------
run_benchmark_aci() {
    local config_basename="$1"
    local engine_name="$2"
    local mongodb_url="$3"
    local run_name="${engine_name}-${config_basename%.yaml}"
    local aci_name="${CONTAINER_NAME}-${run_name}"
    # ACI names: lowercase, alphanumeric, hyphens only, max 63 chars
    aci_name=$(echo "$aci_name" | tr '_' '-' | tr '[:upper:]' '[:lower:]' | cut -c1-63)

    log "[$engine_name] Running benchmark: $config_basename (ACI: $aci_name)"

    # Delete previous instance
    az container delete \
        --resource-group "$ACI_RESOURCE_GROUP" \
        --name "$aci_name" \
        --yes 2>/dev/null || true

    # Create and run
    az container create \
        --resource-group "$ACI_RESOURCE_GROUP" \
        --name "$aci_name" \
        --image "$FULL_IMAGE" \
        --registry-login-server "$ACR_LOGIN_SERVER" \
        --registry-username "$ACI_ACR_NAME" \
        --registry-password "$ACR_PASSWORD" \
        --cpu "$CPU" \
        --memory "$MEMORY" \
        --os-type Linux \
        --restart-policy Never \
        --azure-file-volume-account-name "$STORAGE_ACCOUNT" \
        --azure-file-volume-account-key "$STORAGE_KEY" \
        --azure-file-volume-share-name "$SHARE_NAME" \
        --azure-file-volume-mount-path /app/results \
        --command-line "bench-run --config config/$config_basename --mongodb-url '$mongodb_url' --database-engine '$engine_name' --output-dir results $EXTRA_ARGS" \
        --location "$ACI_LOCATION" \
        -o none

    # Wait for completion
    log "[$engine_name] Waiting for $aci_name to complete..."
    while true; do
        state=$(az container show \
            --resource-group "$ACI_RESOURCE_GROUP" \
            --name "$aci_name" \
            --query "containers[0].instanceView.currentState.state" \
            -o tsv 2>/dev/null || echo "Unknown")

        case "$state" in
            Terminated)
                exit_code=$(az container show \
                    --resource-group "$ACI_RESOURCE_GROUP" \
                    --name "$aci_name" \
                    --query "containers[0].instanceView.currentState.exitCode" \
                    -o tsv)
                if [[ "$exit_code" == "0" ]]; then
                    log "[$engine_name] Benchmark $config_basename completed successfully."
                else
                    log "[$engine_name] WARNING: $config_basename exited with code $exit_code"
                    az container logs --resource-group "$ACI_RESOURCE_GROUP" --name "$aci_name"
                fi
                break
                ;;
            Failed)
                log "[$engine_name] ERROR: Container $aci_name failed."
                az container logs --resource-group "$ACI_RESOURCE_GROUP" --name "$aci_name"
                break
                ;;
            *)
                echo -n "."
                sleep 10
                ;;
        esac
    done
    echo ""

    # Show logs
    log "[$engine_name] Container logs for $config_basename:"
    az container logs --resource-group "$ACI_RESOURCE_GROUP" --name "$aci_name" 2>/dev/null || true

    # Cleanup container
    az container delete \
        --resource-group "$ACI_RESOURCE_GROUP" \
        --name "$aci_name" \
        --yes -o none 2>/dev/null || true
}

# ---------- execute ----------
print_summary

mkdir -p "$RESULTS_RUN_DIR"
log "Results will be saved to: $RESULTS_RUN_DIR"

for i in "${!ENGINE_NAMES[@]}"; do
    engine_name="${ENGINE_NAMES[$i]}"
    engine_url="${ENGINE_URLS[$i]}"
    engine_dir="$RESULTS_RUN_DIR/$engine_name"

    log "=== Engine: $engine_name ==="
    log "Connection: $(mask_url "$engine_url")"
    echo ""

    # Clear file share for this engine's run
    log "[$engine_name] Clearing file share: $SHARE_NAME"
    az storage file delete-batch \
        --source "$SHARE_NAME" \
        --account-name "$STORAGE_ACCOUNT" \
        --account-key "$STORAGE_KEY" \
        -o none 2>/dev/null || true

    for cfg in "${CONFIG_BASENAMES[@]}"; do
        run_benchmark_aci "$cfg" "$engine_name" "$engine_url"
    done

    # Download results for this engine
    log "[$engine_name] Downloading results from Azure File Share..."
    mkdir -p "$engine_dir"
    az storage file download-batch \
        --destination "$engine_dir" \
        --source "$SHARE_NAME" \
        --account-name "$STORAGE_ACCOUNT" \
        --account-key "$STORAGE_KEY" \
        --no-progress \
        -o none

    log "[$engine_name] Organizing results..."
    organize_results "$engine_dir"
    echo ""
done

# ---------- cleanup (optional) ----------
if [[ "$ACI_CLEANUP" == true ]]; then
    log "Cleaning up Azure resources..."
    az group delete --name "$ACI_RESOURCE_GROUP" --yes --no-wait
    log "Resource group $ACI_RESOURCE_GROUP marked for deletion."
fi

# ---------- done ----------
print_done
if [[ "$ACI_CLEANUP" == false ]]; then
    echo ""
    echo "To delete all Azure resources when done:"
    echo "  az group delete --name $ACI_RESOURCE_GROUP --yes"
fi
