#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if pgrep -f "python3 -m http.server 8000" >/dev/null 2>&1; then
  exit 0
fi

nohup python3 -m http.server 8000 --bind 0.0.0.0 >/tmp/documentdb-benchmarks-http-server.log 2>&1 &