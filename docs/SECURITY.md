# Security Notes and Responsible-Use Scope

Relay is a **local portfolio and learning project**. It has been designed and
tested with security in mind, but it has **not** been audited, hardened, or
certified for production or internet-facing use. Do not deploy it as a
public edge proxy.

## What Relay does to stay safe

| Concern | Mitigation | Where |
|---|---|---|
| SSRF / open proxy | Upstreams come only from validated config (http/https, no credentials, no query or fragment). Clients choose a *route*, never a host. | `config.py`, `routing.py` |
| Path traversal | Dot segments rejected, including encoded forms (`%2e%2e`, `..%2F`); NUL bytes, backslashes and non-ASCII rejected; raw-path matching only on segment boundaries. | `routing.validate_raw_path` |
| Hop-by-hop / smuggling-adjacent headers | RFC 9110 hop-by-hop set and every `Connection`-named header stripped in both directions; `Content-Length` recomputed; `Transfer-Encoding` never forwarded. | `headers.py` |
| Spoofed client IPs | `X-Forwarded-For` ignored unless the peer is in `server.trusted_proxies`; then the right-most untrusted hop is used. | `client_identity.py` |
| Resource exhaustion | Admission control: at most `max_in_flight` proxied requests, the rest get a cheap 503 `overloaded`; header size/count caps (431); request body cap (413, enforced while streaming); response body cap; bounded timeouts (connect/attempt/request); at most 5 attempts; bounded connection pools; one health task per route; bounded LRU. | `proxy.py`, `config.py` |
| Abuse | Per-client token bucket; **fail-closed** by default if Redis is down. | `rate_limiter.py` |
| Duplicate writes | POST/PUT/PATCH/DELETE never retried (config rejects them in `retry_methods`). | `resilience/retry.py`, `config.py` |
| Cache poisoning / data leaks | Credentialed requests (`Authorization`, `Cookie`) bypass the cache; `Set-Cookie`, `private`, `no-store`, `no-cache`, unexpected `Vary` → never stored; `X-Relay-Cache` cannot be spoofed by upstreams. | `cache/response_cache.py` |
| Information leakage | Stable JSON errors with no exception text, stack traces, or upstream URLs; `/__relay/routes` omits URLs; TRACE/CONNECT rejected. | `errors.py`, `middleware.py` |
| Logging secrets | Access logs contain ids, statuses and timings only: no bodies, `Authorization`, cookies, query strings, or client IPs. Rate-limit keys store a SHA-256 of the client id. | `middleware.py`, `rate_limiter.py` |
| Metric cardinality / leakage | Labels limited to config ids and fixed enums; a test asserts paths, queries and IPs never appear. | `metrics.py`, `tests/integration/test_metrics.py` |
| Secrets in the repo | None needed. `.env` is git-ignored; `.env.example` holds placeholders; CI runs gitleaks. | `.gitignore`, CI |
| Container | Non-root user (uid 10001); pinned base image and dependencies. | `Dockerfile` |
| Local exposure | Every Compose port is bound to `127.0.0.1`; Redis has no published port. | `compose.yaml` |

## Adversarial test suite

`tests/security/test_security.py` writes **raw bytes over real sockets** to a
Uvicorn-served gateway, so the real HTTP parser is in the loop and no client
library normalizes the input first. It tries to *break* each guarantee:

* **SSRF:** absolute-form targets to an external host, the cloud metadata
  address (`169.254.169.254`), and an internal Redis; `Host`,
  `X-Forwarded-Host`, `X-Original-URL`, `X-Rewrite-URL`, and `Forwarded`
  overrides; `?url=` and `?upstream=` parameters. All still reach only the
  configured upstream.
* **Escaping the route:** `//evil`, `%2e%2e`, `..%2F`, `%5C`, `%00`, `@`, and
  an encoded slash hiding a dot segment. All rejected (400/404) and never
  forwarded.
* **Redirects:** an upstream 302 to an internal address is returned to the
  client, never followed.
* **Framing:** conflicting `Content-Length` + `Transfer-Encoding` with a
  smuggled second request, rejected before anything reaches the upstream;
  oversized and too-many headers (431); missing `Host` (400).
* **Spoofing:** an upstream cannot spoof `X-Relay-Cache` or `X-Request-ID`
  or inject `Connection`-listed headers.
* **Leakage:** a Redis password in the URL never appears in responses,
  readiness output, the route summary, metrics, or logs. Errors contain no
  exception text, upstream addresses, or tracebacks. Access logs contain no
  query strings, credentials, cookies, or client IPs.

Static analysis: Ruff's `S` (flake8-bandit) rules run on every lint, and
`pip-audit` scans the locked dependencies in CI.

## Known limitations (read before reusing this code)

* **No TLS termination** and no authentication or authorization of clients.
  Upstream TLS is supported via `https://` URLs (httpx verifies certificates
  by default), but there is no mTLS or custom CA configuration.
* **Credentials are forwarded** to the matched route's upstreams. There is no
  per-route header allowlist or denylist.
* **No request smuggling test suite.** Relay relies on Uvicorn/h11/httptools
  for HTTP/1.1 parsing and does not forward `Transfer-Encoding`, but it has
  not been fuzzed.
* **Buffered responses**, capped at 10 MiB by default: memory use per
  request can reach that cap.
* **The Grafana default admin password** in Compose is `admin` unless
  `GRAFANA_ADMIN_PASSWORD` is set; anonymous read-only access is enabled for
  the demo. Local use only.
* **The mock backend has unauthenticated control endpoints** (`/__mock/*`)
  for failure injection. It is a test fixture and must never be exposed.
* **Readiness and `/metrics` are unauthenticated**, appropriate only on a
  private network.
* **Per-process circuit-breaker and health state**: multiple gateway
  instances do not share failure knowledge.

## Reporting

This is a personal portfolio repository. If you find a security issue,
please open a GitHub issue without exploit details, or contact the
maintainer privately.
