#!/usr/bin/env sh
set -eu

PORT_VALUE="${PORT:-10000}"
exec python -m uvicorn backend.main:app --host 0.0.0.0 --port "$PORT_VALUE"
