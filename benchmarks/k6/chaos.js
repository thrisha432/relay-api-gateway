// Fixed-rate traffic for chaos timelines (scripts/chaos_timeline.py).
//
// Open-loop: RATE req/s whatever happens, so a failure shows up as errors or
// latency, not as k6 quietly sending less. Each VU aggregates its own
// per-second stats and logs one "TL {...}" line per second (raw per-request
// output would be tens of MB); the orchestrator merges them into a timeline.
import http from 'k6/http';

const BASE = __ENV.BASE || 'http://relay:8080';
const ROUTE = __ENV.ROUTE || '/api/bench-ha';
const BUCKETS = [2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]; // must match scripts/chaos_timeline.py

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    traffic: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 300),
      timeUnit: '1s',
      duration: __ENV.DURATION || '70s',
      preAllocatedVUs: 200,
      maxVUs: 600,
    },
  },
};

let current = null;

function flush() {
  if (current && current.n > 0) console.log(`TL ${JSON.stringify(current)}`);
}

export default function () {
  const res = http.get(`${BASE}${ROUTE}/items?key=k${__ITER % 100}`, {
    timeout: '10s',
    responseCallback: http.expectedStatuses({ min: 200, max: 399 }),
  });
  const sec = Math.floor(Date.now() / 1000);
  if (!current || current.s !== sec) {
    flush();
    current = { s: sec, n: 0, codes: {}, b: new Array(BUCKETS.length + 1).fill(0) };
  }
  current.n += 1;
  const code = String(res.status); // "0" = connection failed (no HTTP response)
  current.codes[code] = (current.codes[code] || 0) + 1;
  let i = BUCKETS.findIndex((edge) => res.timings.duration <= edge);
  if (i < 0) i = BUCKETS.length;
  current.b[i] += 1;
}

export function teardown() {}

export function handleSummary(data) {
  flush();
  const out = { stdout: 'done\n' };
  if (__ENV.OUT) out[__ENV.OUT] = JSON.stringify(data);
  return out;
}
