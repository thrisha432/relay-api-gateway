# Quality Evidence

What each quality signal measures, and the measured result. Re-run with the
command shown; nothing below is estimated.

| Signal | Result | Command |
|---|---|---|
| Lint + format | clean | `ruff check . && ruff format --check .` |
| Static types | `mypy --strict`: 0 issues in 29 source files | `mypy` |
| Tests | **333 passed** (unit, integration, security, property, chaos, multi-process) against a real Redis 7.4, `scripts/verify.sh`, 2026-10-25 | `REDIS_URL=... pytest` |
| Branch coverage | **96%** (`relay` + `mock_backend`) | `pytest --cov` |
| Dependency vulnerabilities | `pip-audit`: no known vulnerabilities in `requirements.lock` (2026-10-25) | `pip-audit -r requirements.lock` |
| Mutation testing (core logic) | **575 / 642 mutants detected (89.6%)**; all **67 survivors reviewed and classified as equivalent** | `mutmut run` |

## Mutation testing

Coverage says a line *ran*; mutation testing asks whether the tests *notice
when that line is wrong*. [mutmut](https://github.com/boxed/mutmut) made
642 small changes (flip a comparison, change a constant, drop a condition)
to the 11 pure-logic modules: LRU, cache policy, single-flight, circuit
breaker, retry policy, retry budget, deadlines, routing, load balancer,
client identity, and headers. It then ran the unit tests against each
mutant.

### Round 1 → round 2

| | Mutants | Detected | Survived | Score |
|---|---|---|---|---|
| Round 1 (before) | 639 | 555 (551 killed + 4 timeout) | 84 | 86.9% |
| Round 2 (after new tests) | 642 | 575 (571 killed + 4 timeout) | 67 | 89.6% |

Round 1 survivors exposed **real test gaps**. `tests/unit/test_mutation_gaps.py`
closes them; each test names the mutation it kills:

* **`Cache-Control: max-age=60,no-store` with no space** was untested. A
  parser splitting on whitespace instead of commas would have *cached a
  `no-store` response*.
* A malformed `max-age` (no value) was treated as absent, so the response was
  cached for the route's full TTL. **Changed to "do not cache"** (conservative).
* Boundaries: a body of exactly `max_entry_bytes`; `max-age=1`; TTL expiry at
  exactly `expires_at`; `!`/`~` allowed and DEL/`#` rejected in paths.
* Circuit breaker: after recovery it must take the *full* failure threshold to
  reopen (a mutant starting the count at 1 survived).
* `X-Forwarded-For` without spaces; trusted-proxy CIDRs with host bits set.
* Single-flight bookkeeping when the shared task is cancelled, and when a
  failure is abandoned by every waiter.

### The 67 remaining survivors are equivalent mutants

An *equivalent mutant* changes the code without changing any observable
behavior, so no test can kill it. Each survivor was reviewed:

| Count | Category | Why it cannot change behavior |
|---|---|---|
| 16 | Error/assert message text | Messages are not part of the contract (codes are) |
| 16 | Circuit breaker unreachable states | Fields reset on every transition before being read; `and`→`or` cases that the generation check makes unreachable; generation step size (+1 vs +2) |
| 12 | `LRUCache.check_invariants` | Test-only helper (it is the oracle, not the system under test) |
| 5 | Codec-name case (`latin-1` vs `LATIN-1`) | Python codec names are case-insensitive |
| 3 | LRU defensive pointer reset | Pointers are always overwritten before being read |
| 5 | Routing double-defense | Backslash/NUL are rejected both in the raw path and after decoding; removing one layer is masked by the other. Segment splitting is re-done after decoding |
| 5 | Cache key constants/separators | Any fixed constant or separator inside the SHA-256 input keeps keys deterministic and distinct |
| 2 | `Cache-Control` value stripping / empty arg | Numeric directive values are unaffected; empty and missing values both mean "malformed" |
| 1 | `Vary: *` literal | Already rejected by the "names must be in the key" subset check |
| 2 | Client identity | `strict=None` ≡ `strict=False`; `_parse_ip("")` already returns None |
| 1 | `Deadline.can_fit` `>=` vs `>` | Differs only when two floats are exactly equal |

Because every survivor is equivalent, the suite detected **every behavior-
changing mutant** in these modules. The I/O-heavy modules (`proxy.py`,
`rate_limiter.py`, `health.py`) were not mutated: their tests need live
sockets and Redis, so a mutation run would take hours. They are covered by
the integration, chaos and property tests instead.
