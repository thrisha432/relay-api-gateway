// Gateway path. MODE=uncached -> /api/bench (no cache)
//               MODE=cached   -> /api/bench-cached with a small repeating key
//                                set (cache-friendly workload).
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { loadOptions, summarize } from './lib.js';

const GW = __ENV.GATEWAY_URL || 'http://relay:8080';
const MODE = __ENV.MODE || 'uncached';
const cacheHits = new Counter('relay_cache_hits');
const cacheMisses = new Counter('relay_cache_misses');

export const options = loadOptions();

export default function () {
  const id = (__VU * 1000 + __ITER) % (MODE === 'cached' ? 50 : 100);
  const route = MODE === 'cached' ? 'bench-cached' : 'bench';
  const res = http.get(`${GW}/api/${route}/items?id=${id}`);
  check(res, { 'status 200': (r) => r.status === 200 });
  const cache = res.headers['X-Relay-Cache'];
  if (cache === 'HIT') cacheHits.add(1);
  else if (cache === 'MISS') cacheMisses.add(1);
}

export function handleSummary(data) {
  const hits = data.metrics.relay_cache_hits ? data.metrics.relay_cache_hits.values.count : 0;
  const misses = data.metrics.relay_cache_misses ? data.metrics.relay_cache_misses.values.count : 0;
  const extra = MODE === 'cached'
    ? [`cache hit ratio: ${((hits / Math.max(1, hits + misses)) * 100).toFixed(2)}% (${hits} hits / ${misses} misses)`]
    : [];
  return summarize(`gateway-${MODE}`, data, extra);
}
