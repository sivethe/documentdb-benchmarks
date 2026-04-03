#!/usr/bin/env bash
# Post-startup script for the devcontainer.
# Copies pipeline.config.template to pipeline.config if it doesn't already exist,
# so developers start with a working config without committing secrets.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG="${SCRIPT_DIR}/pipeline.config"
TEMPLATE="${SCRIPT_DIR}/pipeline.config.template"

if [[ ! -f "$CONFIG" ]]; then
    cp "$TEMPLATE" "$CONFIG"
    echo "Created deploy/pipeline.config from template."
else
    echo "deploy/pipeline.config already exists, skipping copy."
fi
