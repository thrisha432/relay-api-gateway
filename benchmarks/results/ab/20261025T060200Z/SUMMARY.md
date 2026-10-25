A/B: 10 VUs, 15s, route /api/bench

| Image | Runs | Median req/s (min-max) | Median p50 ms | Median p99 ms |
|---|---|---|---|---|
| `relay:before` | 3 | 2,050 (2,045-2,107) | 3.71 | 27.31 |
| `relay:after` | 3 | 2,050 (2,016-2,099) | 3.71 | 26.49 |
