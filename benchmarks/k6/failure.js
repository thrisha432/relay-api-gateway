// Failure injection: steady GET load on /api/bench-ha (backend-a + backend-b).
// At FAIL_AT seconds, backend-b starts returning 503 and failing health checks;
// at RECOVER_AT it recovers. Gateway-side retry / breaker counters are scraped
// from /metrics at the end and printed with the summary.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { scrapeCounters, summarize } from './lib.js';

const GW = __ENV.GATEWAY_URL || 'http://relay:8080';
const B = __ENV.BACKEND_B_URL || 'http://backend-b:8002';
const FAIL_AT = Number(__ENV.FAIL_AT || 20);
const RECOVER_AT = Number(__ENV.RECOVER_AT || 50);
const DURATION = __ENV.DURATION || '80s';

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  discardResponseBodies: true,
  scenarios: {
    traffic: { executor: 'constant-vus', vus: Number(__ENV.VUS || 10), duration: DURATION },
    chaos: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'chaos', maxDuration: DURATION },
  },
};

const COUNTERS = [
  'relay_retries_total',
  'relay_retries_exhausted_total',
  'relay_circuit_breaker_transitions_total',
  'relay_circuit_breaker_rejections_total',
  'relay_no_healthy_upstream_total',
];

export function setup() {
  http.post(`${B}/__mock/reset`);
  // responseType 'text': bodies are discarded by default (discardResponseBodies).
  return { before: scrapeCounters(http.get(`${GW}/metrics`, { responseType: 'text' }).body, COUNTERS) };
}

export default function () {
  const res = http.get(`${GW}/api/bench-ha/items?id=${__ITER % 100}`, { tags: { phase: 'traffic' } });
  check(res, { 'status 200': (r) => r.status === 200 });
}

export function chaos() {
  sleep(FAIL_AT);
  http.post(`${B}/__mock/config`, JSON.stringify({ healthy: false, fail_status: 503 }), {
    headers: { 'Content-Type': 'application/json' },
  });
  sleep(RECOVER_AT - FAIL_AT);
  http.post(`${B}/__mock/reset`);
}

export function teardown(data) {
  http.post(`${B}/__mock/reset`);
  const after = scrapeCounters(http.get(`${GW}/metrics`, { responseType: 'text' }).body, COUNTERS);
  const delta = {};
  for (const k of COUNTERS) delta[k] = after[k] - data.before[k];
  console.log(`GATEWAY_COUNTERS ${JSON.stringify(delta)}`);
  return delta;
}

export function handleSummary(data) {
  return summarize('failure', data, [
    `failure window: backend-b unhealthy + 503 from t=${FAIL_AT}s to t=${RECOVER_AT}s`,
  ]);
}
