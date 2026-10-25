// One parametric scenario used by scripts/bench_matrix.py for every matrix cell.
//
// EXECUTOR=closed : VUS virtual users loop with no think time (throughput at saturation)
// EXECUTOR=open   : constant arrival RATE req/s regardless of response time, so
//                   queueing delay is measured instead of hidden (no coordinated
//                   omission). Requests k6 could not start in time are reported
//                   as dropped_iterations.
// KEYS=N          : cycle through N distinct URLs (cache-friendly); 0 = all unique
// HERD_MS=M       : every VU requests the same key, which changes every M ms
//                   (cold-cache stampede)
// DELAY_MS        : upstream (mock backend) processing time per request
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE;
const ROUTE = __ENV.ROUTE || '/api/bench';
const KEYS = Number(__ENV.KEYS || 100);
const HERD_MS = Number(__ENV.HERD_MS || 0);
const DELAY_MS = Number(__ENV.DELAY_MS || 0);
const DURATION = __ENV.DURATION || '20s';
const OPEN = (__ENV.EXECUTOR || 'closed') === 'open';

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  // Spread requests over every IP behind a DNS name (horizontal-scaling runs).
  dns: { ttl: '0', select: 'roundRobin', policy: 'any' },
  scenarios: {
    main: OPEN
      ? {
          executor: 'constant-arrival-rate',
          rate: Number(__ENV.RATE || 500),
          timeUnit: '1s',
          duration: DURATION,
          preAllocatedVUs: Number(__ENV.MAX_VUS || 400),
          maxVUs: Number(__ENV.MAX_VUS || 400),
        }
      : { executor: 'constant-vus', vus: Number(__ENV.VUS || 10), duration: DURATION },
  },
};

let seq = 0;
export default function () {
  let key;
  if (HERD_MS > 0) key = `herd-${Math.floor(Date.now() / HERD_MS)}`;
  else if (KEYS > 0) key = `k${(__VU * 7919 + seq++) % KEYS}`;
  else key = `u${__VU}-${seq++}`;
  const delay = DELAY_MS > 0 ? `&delay_ms=${DELAY_MS}` : '';
  const res = http.get(`${BASE}${ROUTE}/items?key=${key}${delay}`, {
    responseCallback: http.expectedStatuses({ min: 200, max: 399 }),
  });
  check(res, { ok: (r) => r.status >= 200 && r.status < 400 });
}

export function handleSummary(data) {
  const out = { stdout: 'done\n' };
  if (__ENV.OUT) out[__ENV.OUT] = JSON.stringify(data);
  return out;
}
