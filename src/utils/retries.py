"""Retry helpers with exponential backoff."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


def with_default_retry(func: Callable[P, T]) -> Callable[P, T]:
    """Apply standard retry policy for network-bound operations."""

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except (TimeoutError, ConnectionError):
                if attempt == max_attempts:
                    raise
                backoff_seconds = min(30, (2 ** (attempt - 1)) + random.uniform(0, 0.5))
                time.sleep(backoff_seconds)

        raise RuntimeError("retry wrapper exhausted unexpectedly")

    return wrapper
