"""Property-based tests (Hypothesis).

Instead of hand-picked examples, Hypothesis generates thousands of inputs and
shrinks any failure to a minimal counterexample. Three techniques here:

* **Stateful testing**: the LRU is driven by random operation sequences and
  compared step by step with an ``OrderedDict`` model.
* **Differential testing**: the Lua token bucket (run inside Redis) is
  compared with an independent pure-Python reference implementation of the
  same math, over random timelines.
* **Invariant testing**: for any arrival pattern, the limiter never admits
  more than ``capacity + rate x elapsed``.
"""

from __future__ import annotations

import asyncio
import math
from collections import OrderedDict
from urllib.parse import quote

import fakeredis
import hypothesis.strategies as st
from hypothesis import HealthCheck, given, settings
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from relay.cache.lru import LRUCache
from relay.cache.response_cache import cache_key
from relay.config import RateLimitConfig, parse_config
from relay.rate_limiter import RedisRateLimiter
from relay.routing import InvalidPathError, Router, validate_raw_path

# ------------------------------------------------------------- LRU (stateful)


class LRUMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.capacity = 0
        self.cache: LRUCache[int, int] | None = None
        self.model: OrderedDict[int, int] = OrderedDict()

    @precondition(lambda self: self.cache is None)
    @rule(capacity=st.integers(min_value=0, max_value=6))
    def create(self, capacity: int) -> None:
        self.capacity = capacity
        self.cache = LRUCache(capacity)

    @precondition(lambda self: self.cache is not None)
    @rule(key=st.integers(0, 10), value=st.integers())
    def put(self, key: int, value: int) -> None:
        assert self.cache is not None
        expected = None
        if self.capacity > 0:
            if key in self.model:
                self.model.move_to_end(key)
            elif len(self.model) >= self.capacity:
                expected = self.model.popitem(last=False)
            self.model[key] = value
        assert self.cache.put(key, value) == expected

    @precondition(lambda self: self.cache is not None)
    @rule(key=st.integers(0, 10))
    def get(self, key: int) -> None:
        assert self.cache is not None
        expected = self.model.get(key)
        if expected is not None:
            self.model.move_to_end(key)
        assert self.cache.get(key) == expected

    @precondition(lambda self: self.cache is not None)
    @rule(key=st.integers(0, 10))
    def delete(self, key: int) -> None:
        assert self.cache is not None
        assert self.cache.delete(key) == (self.model.pop(key, None) is not None)

    @precondition(lambda self: self.cache is not None)
    @rule(key=st.integers(0, 10))
    def peek_does_not_reorder(self, key: int) -> None:
        assert self.cache is not None
        assert self.cache.peek(key) == self.model.get(key)

    @invariant()
    def matches_model(self) -> None:
        if self.cache is not None:
            self.cache.check_invariants()
            assert list(self.cache.keys()) == list(reversed(self.model))


TestLRUStateful = LRUMachine.TestCase
TestLRUStateful.settings = settings(max_examples=300, stateful_step_count=60, deadline=None)


# ----------------------------------------- token bucket: reference model


def reference_bucket(
    state: tuple[float, float] | None, now_us: int, capacity: int, rate: float, cost: int = 1
) -> tuple[bool, float, tuple[float, float]]:
    """Independent Python statement of the token-bucket rules in the Lua script."""
    tokens, ts = state if state is not None else (float(capacity), float(now_us))
    elapsed = max(0.0, now_us - ts)
    tokens = min(float(capacity), tokens + elapsed * rate / 1_000_000)
    allowed = tokens >= cost
    if allowed:
        tokens -= cost
    return allowed, tokens, (tokens, max(float(now_us), ts))


timelines = st.lists(
    st.integers(min_value=-2_000_000, max_value=3_000_000),  # step in us, may go back
    min_size=1,
    max_size=60,
)


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    capacity=st.integers(1, 20),
    rate=st.sampled_from([0.1, 0.5, 1.0, 2.5, 10.0, 100.0]),
    steps=timelines,
)
def test_lua_bucket_matches_reference_model(capacity: int, rate: float, steps: list[int]) -> None:
    """Differential test: Lua-in-Redis and the Python model agree on every step."""

    async def run() -> None:
        limiter = RedisRateLimiter(fakeredis.FakeAsyncRedis())
        cfg = RateLimitConfig(capacity=capacity, refill_rate_per_second=rate)
        now = 1_700_000_000_000_000
        state = None
        for step in steps:
            now += step
            decision = await limiter.acquire("r", "c", cfg, now_us=now)
            allowed, tokens, state = reference_bucket(state, now, capacity, rate)
            assert decision.allowed == allowed
            assert decision.remaining == math.floor(tokens + 1e-9)

    asyncio.run(run())


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    capacity=st.integers(1, 15),
    rate=st.sampled_from([0.5, 1.0, 4.0, 20.0]),
    gaps_ms=st.lists(st.integers(0, 400), min_size=1, max_size=80),
)
def test_limiter_never_admits_more_than_capacity_plus_refill(
    capacity: int, rate: float, gaps_ms: list[int]
) -> None:
    """Invariant: admitted(t0..t1) <= capacity + rate * (t1 - t0) for any arrivals."""

    async def run() -> None:
        limiter = RedisRateLimiter(fakeredis.FakeAsyncRedis())
        cfg = RateLimitConfig(capacity=capacity, refill_rate_per_second=rate)
        start = now = 1_700_000_000_000_000
        admitted = 0
        for gap in gaps_ms:
            now += gap * 1000
            if (await limiter.acquire("r", "c", cfg, now_us=now)).allowed:
                admitted += 1
            elapsed_s = (now - start) / 1_000_000
            assert admitted <= capacity + rate * elapsed_s + 1e-9

    asyncio.run(run())


# ------------------------------------------------------------------ routing

segment = st.text(alphabet="abcdefghij-_", min_size=1, max_size=4)
prefixes = st.lists(
    st.lists(segment, min_size=1, max_size=3), min_size=1, max_size=6, unique_by=tuple
)


def naive_match(prefixes_: list[str], path: str) -> str | None:
    """Obviously-correct reference: check every prefix, keep the longest."""
    candidates = [p for p in prefixes_ if path == p or path.startswith(p + "/")]
    return max(candidates, key=len, default=None)


@settings(max_examples=400, deadline=None)
@given(
    prefix_parts=prefixes, path_parts=st.lists(segment, min_size=1, max_size=5), slash=st.booleans()
)
def test_router_matches_reference_longest_prefix(prefix_parts, path_parts, slash) -> None:
    all_prefixes = ["/api/" + "/".join(parts) for parts in prefix_parts]
    routes = [
        {"id": f"r{i}", "path_prefix": p, "upstreams": [{"id": "a", "url": "http://a:1"}]}
        for i, p in enumerate(all_prefixes)
    ]
    router = Router(parse_config({"routes": routes}).routes)
    path = "/api/" + "/".join(path_parts) + ("/" if slash else "")
    match = router.match(path)
    expected = naive_match(all_prefixes, path)
    assert (match.route.path_prefix if match else None) == expected


@settings(max_examples=400, deadline=None)
@given(
    before=st.lists(segment, max_size=3),
    after=st.lists(segment, max_size=3),
    dots=st.sampled_from([".", ".."]),
    encode_mask=st.integers(0, 3),
)
def test_any_dot_segment_is_rejected_however_encoded(before, after, dots, encode_mask) -> None:
    # Encode some dots as %2e / %2E: the decoded form must still be rejected.
    encoded = "".join(
        (
            quote(ch, safe="").replace("%2E", "%2e" if i % 2 else "%2E")
            if encode_mask >> i & 1
            else ch
        )
        if ch == "."
        else ch
        for i, ch in enumerate(dots)
    )
    path = "/" + "/".join([*before, encoded, *after])
    try:
        validate_raw_path(path)
    except InvalidPathError:
        return
    raise AssertionError(f"accepted {path!r}")


# ---------------------------------------------------------------- cache key

header_values = st.text(alphabet=st.characters(min_codepoint=33, max_codepoint=126), max_size=12)


@settings(max_examples=300, deadline=None)
@given(
    route=segment,
    path=st.lists(segment, max_size=4),
    query=st.text(alphabet="abc=&123", max_size=12),
    accept=header_values,
    other=header_values,
)
def test_cache_key_depends_only_on_its_inputs(route, path, query, accept, other) -> None:
    vary = ("accept", "accept-encoding")
    raw_path = "/" + "/".join(path)
    base = cache_key(route, raw_path, query, [(b"accept", accept.encode())], vary)
    # Header name case and unrelated headers do not change the key ...
    assert base == cache_key(
        route, raw_path, query, [(b"ACCEPT", accept.encode()), (b"x-other", other.encode())], vary
    )
    # ... but the query string always does.
    assert base != cache_key(route, raw_path, query + "x", [(b"accept", accept.encode())], vary)
