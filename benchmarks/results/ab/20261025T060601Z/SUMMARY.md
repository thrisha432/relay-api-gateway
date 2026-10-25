A/B: 10 VUs, 15s, route /api/bench

| Image | Runs | Median req/s (min-max) | Median p50 ms | Median p99 ms |
|---|---|---|---|---|
| `relay:v0` | 3 | 2,035 (2,010-2,125) | 3.75 | 28.20 |
| `relay:after` | 3 | 2,021 (1,994-2,103) | 3.76 | 26.80 |
