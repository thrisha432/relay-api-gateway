#!/usr/bin/env bash
# Independent verification: run every check from scratch and keep the log.
#
#   scripts/verify.sh            # lint, types, vuln scan, full tests (real Redis), stack + smoke
#   scripts/verify.sh --quick    # skip the Docker stack
#
# Output is tee'd to verification/verify-<timestamp>.log, so the evidence is
# your own terminal output, not a number copied from a README.
set -uo pipefail
cd "$(dirname "$0")/.."
mkdir -p verification
LOG="verification/verify-$(date -u +%Y%m%dT%H%M%SZ).log"
exec > >(tee "$LOG") 2>&1

QUICK="${1:-}"
FAILED=()
step() { echo; echo "==> $*"; }
check() { local name="$1"; shift; if "$@"; then echo "PASS: $name"; else echo "FAIL: $name"; FAILED+=("$name"); fi; }

step "Environment"
echo "date: $(date -u)"; echo "git: $(git rev-parse --short HEAD) $(git status --porcelain | wc -l | tr -d ' ') uncommitted file(s)"
python3 --version; docker --version 2>/dev/null || echo "docker: not available"

step "Python environment"
if [[ ! -x .venv/bin/python ]]; then scripts/dev.sh setup; fi
.venv/bin/python --version

step "Lint and format";            check ruff .venv/bin/ruff check . && check ruff-format .venv/bin/ruff format --check .
step "Static types (mypy --strict)"; check mypy .venv/bin/mypy
step "Dependency vulnerabilities";  check pip-audit .venv/bin/pip-audit -r requirements.lock

step "Throwaway Redis for real-Redis tests"
if docker info >/dev/null 2>&1; then
  docker ps --format '{{.Names}}' | grep -q '^relay-test-redis$' \
    || docker run -d --rm --name relay-test-redis -p 127.0.0.1:6390:6379 redis:7.4-alpine >/dev/null
  export REDIS_URL=redis://127.0.0.1:6390/15
  sleep 1
else
  echo "Docker unavailable: Redis-marked tests will be SKIPPED (fakeredis variants still run)."
fi

step "Full test suite with coverage"
check pytest .venv/bin/pytest --cov --cov-report=term -q

if [[ "$QUICK" != "--quick" ]] && docker info >/dev/null 2>&1; then
  step "Docker Compose stack + end-to-end smoke test"
  docker compose up -d --build >/dev/null && check smoke scripts/smoke_test.sh
fi

step "Result"
if ((${#FAILED[@]})); then echo "FAILED: ${FAILED[*]}"; echo "log: $LOG"; exit 1; fi
echo "ALL CHECKS PASSED"; echo "log: $LOG"
