"""Monotonic deadline controls shared by route-leg collectors."""

from __future__ import annotations

import time
from typing import Callable


class CollectionDeadlineExceeded(TimeoutError):
    """Raised when a collection operation has no usable time remaining."""


class CollectionDeadline:
    """A monotonic deadline that clamps requests and retry sleeps."""

    def __init__(
        self,
        expires_at: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.expires_at = float(expires_at)
        self._clock = clock
        self._sleeper = sleeper

    @classmethod
    def for_duration(
        cls,
        duration_seconds: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> "CollectionDeadline":
        duration = max(0.0, float(duration_seconds))
        return cls(
            clock() + duration,
            clock=clock,
            sleeper=sleeper,
        )

    def remaining_seconds(self) -> float:
        return max(0.0, self.expires_at - self._clock())

    def require_remaining(self) -> float:
        remaining = self.remaining_seconds()
        if remaining <= 0:
            raise CollectionDeadlineExceeded("collection deadline exceeded")
        return remaining

    def request_timeout(self, timeout_seconds: float) -> float:
        timeout = float(timeout_seconds)
        if timeout <= 0:
            raise ValueError("timeout_seconds must be positive")
        return min(timeout, self.require_remaining())

    def sleep_before_retry(self, seconds: float) -> None:
        delay = max(0.0, float(seconds))
        remaining = self.require_remaining()
        self._sleeper(min(delay, remaining))
        self.require_remaining()
