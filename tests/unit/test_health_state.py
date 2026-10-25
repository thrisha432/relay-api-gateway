from relay.config import HealthCheckConfig
from relay.health import UpstreamHealth

CFG = HealthCheckConfig(unhealthy_threshold=3, healthy_threshold=2)


def test_marks_unhealthy_only_after_consecutive_failures() -> None:
    state = UpstreamHealth()
    assert not state.record(False, CFG)
    assert not state.record(False, CFG)
    assert state.record(False, CFG)  # third consecutive failure flips
    assert state.healthy is False


def test_success_resets_failure_streak() -> None:
    state = UpstreamHealth()
    for ok in (False, False, True, False, False):
        state.record(ok, CFG)
    assert state.healthy is True


def test_recovers_after_consecutive_successes() -> None:
    state = UpstreamHealth(healthy=False)
    assert not state.record(True, CFG)
    assert not state.record(False, CFG)  # streak broken
    assert not state.record(True, CFG)
    assert state.record(True, CFG)
    assert state.healthy is True
