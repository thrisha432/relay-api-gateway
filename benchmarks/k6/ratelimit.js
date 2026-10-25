// Distributed rate limiting: one client alternates between two gateway
// instances (relay, relay-2) on /api/orders (capacity 5, refill 1/s).
// Shared Redis state means the total allowed across BOTH instances should stay
// within capacity + rate * duration. Requires `--profile multi`.
import http from 'k6/http';
import { Counter } from 'k6/metrics';
import { summarize } from './lib.js';

const GW1 = __ENV.GATEWAY_URL || 'http://relay:8080';
const GW2 = __ENV.GATEWAY2_URL || 'http://relay-2:8080';
const SECONDS = Number(__ENV.SECONDS || 30);
const allowed = new Counter('allowed_200');
const limited = new Counter('limited_429');

export const options = {
  vus: Number(__ENV.VUS || 5),
  duration: `${SECONDS}s`,
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const gw = __ITER % 2 === 0 ? GW1 : GW2;
  const res = http.get(`${gw}/api/orders/rl-${__VU}`, { responseCallback: http.expectedStatuses(200, 429) });
  if (res.status === 200) allowed.add(1);
  else if (res.status === 429) limited.add(1);
}

export function handleSummary(data) {
  const ok = data.metrics.allowed_200 ? data.metrics.allowed_200.values.count : 0;
  const no = data.metrics.limited_429 ? data.metrics.limited_429.values.count : 0;
  const ceiling = 5 + 1 * SECONDS;
  return summarize('ratelimit-two-gateways', data, [
    `allowed (200): ${ok}  limited (429): ${no}`,
    `theoretical ceiling (capacity 5 + 1/s x ${SECONDS}s): ${ceiling}  -> ${ok <= ceiling + 1 ? 'WITHIN' : 'EXCEEDED'}`,
  ]);
}
