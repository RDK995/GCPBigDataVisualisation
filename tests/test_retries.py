import pytest

from src.utils.retries import with_default_retry


def test_with_default_retry_success_after_failure():
    state = {"calls": 0}

    @with_default_retry
    def sometimes_fails() -> str:
        state["calls"] += 1
        if state["calls"] < 2:
            raise ConnectionError("temporary")
        return "ok"

    assert sometimes_fails() == "ok"
    assert state["calls"] == 2


def test_with_default_retry_raises_after_max_attempts():
    state = {"calls": 0}

    @with_default_retry
    def always_fails() -> str:
        state["calls"] += 1
        raise ConnectionError("still broken")

    with pytest.raises(ConnectionError):
        always_fails()

    assert state["calls"] == 5
