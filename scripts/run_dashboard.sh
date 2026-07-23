#!/bin/sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
port="${PORT:-8765}"

cd "$project_root"
exec python3 dashboard/server.py --host 0.0.0.0 --port "$port" "$@"
