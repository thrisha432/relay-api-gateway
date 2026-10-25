// Shared helpers for Relay k6 scenarios. No remote jslib imports, so the
// scripts run fully offline.

export function loadOptions() {
  return {
    vus: Number(__ENV.VUS || 10),
    duration: __ENV.DURATION || '60s',
    summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
    discardResponseBodies: true,
  };
}

function fmt(ms) {
  return ms === undefined ? 'n/a' : `${ms.toFixed(2)}ms`;
}

// Writes the raw k6 summary JSON to $OUT (if set) and a short text summary to stdout.
export function summarize(name, data, extra) {
  const d = data.metrics.http_req_duration ? data.metrics.http_req_duration.values : {};
  const reqs = data.metrics.http_reqs ? data.metrics.http_reqs.values : {};
  const failed = data.metrics.http_req_failed ? data.metrics.http_req_failed.values : {};
  const lines = [
    `scenario: ${name}`,
    `requests: ${reqs.count}  rps: ${(reqs.rate || 0).toFixed(1)}`,
    `latency: p50 ${fmt(d.med)}  p95 ${fmt(d['p(95)'])}  p99 ${fmt(d['p(99)'])}  max ${fmt(d.max)}`,
    `http_req_failed rate: ${((failed.rate || 0) * 100).toFixed(3)}%`,
  ];
  if (extra) lines.push(...extra);
  const out = { stdout: lines.join('\n') + '\n' };
  if (__ENV.OUT) out[__ENV.OUT] = JSON.stringify(data, null, 2);
  return out;
}

// Parse selected counters from Prometheus text exposition (for gateway-side numbers).
export function scrapeCounters(text, names) {
  const totals = {};
  for (const name of names) totals[name] = 0;
  for (const line of text.split('\n')) {
    if (line.startsWith('#')) continue;
    for (const name of names) {
      if (line.startsWith(name + '{') || line.startsWith(name + ' ')) {
        totals[name] += Number(line.split(' ').pop());
      }
    }
  }
  return totals;
}
