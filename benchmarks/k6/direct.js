// Baseline: k6 -> backend-a directly (no gateway). Same path/query mix as gateway.js.
import http from 'k6/http';
import { check } from 'k6';
import { loadOptions, summarize } from './lib.js';

const DIRECT = __ENV.DIRECT_URL || 'http://backend-a:8001';
export const options = loadOptions();

export default function () {
  const id = (__VU * 1000 + __ITER) % 100;
  const res = http.get(`${DIRECT}/api/bench/items?id=${id}`);
  check(res, { 'status 200': (r) => r.status === 200 });
}

export function handleSummary(data) {
  return summarize('direct', data);
}
