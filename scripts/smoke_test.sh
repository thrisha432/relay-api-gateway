#!/usr/bin/env bash
# End-to-end smoke test against a running stack (docker compose up -d).
# Usage: scripts/smoke_test.sh [gateway_url]
set -euo pipefail

GW="${1:-http://127.0.0.1:8080}"
fail() { echo "FAIL: $*" >&2; exit 1; }
status() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

echo "Waiting for $GW/health/ready ..."
for _ in $(seq 1 60); do
  [[ "$(status "$GW/health/ready")" == "200" ]] && break
  sleep 1
done
[[ "$(status "$GW/health/ready")" == "200" ]] || fail "gateway not ready"

echo "1. liveness";        [[ "$(status "$GW/health/live")" == "200" ]] || fail "live"
echo "2. unknown route";   [[ "$(status "$GW/nope")" == "404" ]] || fail "404 expected"

echo "3. round-robin across both backends"
seen=$(for _ in 1 2 3 4; do curl -s "$GW/api/bench-ha/items" | python3 -c 'import json,sys; print(json.load(sys.stdin)["instance"])'; done | sort -u | tr '\n' ' ')
[[ "$seen" == "backend-a backend-b " ]] || fail "expected both backends, saw: $seen"

echo "4. cache MISS then HIT"
key="smoke-$RANDOM-$RANDOM"
first=$(curl -s -D - -o /dev/null "$GW/api/bench-cached/$key" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-relay-cache"{print $2}')
second=$(curl -s -D - -o /dev/null "$GW/api/bench-cached/$key" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-relay-cache"{print $2}')
[[ "$first" == "MISS" && "$second" == "HIT" ]] || fail "cache: got $first then $second"

echo "5. rate limit -> 429 on /api/orders (capacity 5)"
codes=$(for _ in $(seq 1 8); do status "$GW/api/orders/x"; echo; done | sort | uniq -c | tr -s ' ' | tr '\n' ';')
[[ "$codes" == *"429"* ]] || fail "expected a 429, got: $codes"

echo "6. metrics endpoint"
curl -s "$GW/metrics" | grep -q '^relay_requests_total' || fail "metrics missing"

echo "OK: all smoke checks passed"
