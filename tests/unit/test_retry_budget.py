from relay.resilience.retry_budget import RetryBudget


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_starts_full_and_is_capped() -> None:
    clock = Clock()
    budget = RetryBudget(0.2, 0.0, max_balance=3, clock=clock)
    assert [budget.try_spend() for _ in range(4)] == [True, True, True, False]


def test_requests_deposit_ratio() -> None:
    clock = Clock()
    budget = RetryBudget(0.25, 0.0, max_balance=10, clock=clock)
    while budget.try_spend():
        pass
    for _ in range(4):  # 4 x 0.25 = 1 token
        budget.record_request()
    assert budget.try_spend()
    assert not budget.try_spend()


def test_long_run_retries_bounded_by_ratio() -> None:
    clock = Clock()
    budget = RetryBudget(0.2, 0.0, max_balance=1, clock=clock)
    retries = 0
    for _ in range(1000):
        budget.record_request()
        if budget.try_spend():
            retries += 1
    assert 199 <= retries <= 201  # ~20% of 1,000 requests


def test_time_based_reserve_refills_and_caps() -> None:
    clock = Clock()
    budget = RetryBudget(0.0, 2.0, max_balance=5, clock=clock)
    while budget.try_spend():
        pass
    clock.now += 1.0  # 2 tokens
    assert [budget.try_spend() for _ in range(3)] == [True, True, False]
    clock.now += 1000  # long idle: capped at max_balance
    assert budget.balance == 5


def test_clock_going_backwards_adds_nothing() -> None:
    clock = Clock()
    budget = RetryBudget(0.0, 10.0, max_balance=5, clock=clock)
    while budget.try_spend():
        pass
    clock.now -= 100
    assert budget.balance == 0
