#!/usr/bin/env bash
# Developer helper. Usage: scripts/dev.sh <command>
#   setup     create .venv and install Relay with dev dependencies (pinned)
#   test      run the test suite (set REDIS_URL to include real-Redis tests)
#   lint      ruff check + format check
#   fmt       ruff format + autofix
#   run       run the gateway locally (uses $RELAY_CONFIG)
#   redis     start a throwaway local Redis on 127.0.0.1:6390 for tests
#   up/down   start/stop the Docker Compose stack
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PYTHON:-python3}"

case "${1:-help}" in
  setup)
    "$PY" -m venv .venv
    .venv/bin/pip install --upgrade pip
    .venv/bin/pip install -r requirements-dev.lock
    .venv/bin/pip install --no-deps -e .
    ;;
  test)  .venv/bin/pytest "${@:2}" ;;
  lint)  .venv/bin/ruff check . && .venv/bin/ruff format --check . ;;
  fmt)   .venv/bin/ruff format . && .venv/bin/ruff check --fix . ;;
  run)   .venv/bin/relay "${@:2}" ;;
  redis) docker run -d --rm --name relay-test-redis -p 127.0.0.1:6390:6379 redis:7.4-alpine
         echo "export REDIS_URL=redis://127.0.0.1:6390/15" ;;
  up)    docker compose up -d --build && scripts/smoke_test.sh ;;
  down)  docker compose --profile multi --profile bench down ;;
  *)     sed -n '2,10p' "$0" ;;
esac
