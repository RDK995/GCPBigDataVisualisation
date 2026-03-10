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
