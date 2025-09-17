#!/usr/bin/env sh
# Entrypoint responsible for bootstrapping the FastAPI service.
set -euo pipefail

HOST="${UVICORN_HOST:-0.0.0.0}"
PORT="${UVICORN_PORT:-8000}"
APP_MODULE="${APP_MODULE:-app.main:app}"
ENVIRONMENT="${ENVIRONMENT:-production}"
WORKERS="${UVICORN_WORKERS:-4}"

if [ "$ENVIRONMENT" = "development" ]; then
  exec uvicorn "$APP_MODULE" --host "$HOST" --port "$PORT" --reload
else
  exec uvicorn "$APP_MODULE" --host "$HOST" --port "$PORT" --workers "$WORKERS"
fi
