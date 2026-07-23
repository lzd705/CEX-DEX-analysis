#!/bin/sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
port="${PORT:-8765}"
host="${HOST:-127.0.0.1}"

cd "$project_root"
exec python3 dashboard/server.py --host "$host" --port "$port" "$@"
