"""Injectable time source.

Every component that needs the current time reads it from a ``Clock`` so that
tests and benchmarks can move time deterministically instead of sleeping.
"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """Seconds since the Unix epoch as a float."""


class SystemClock:
    """Wall-clock time."""

    def now(self) -> float:
        return time.time()


class ManualClock:
    """A clock that only moves when told to. Used in tests and benchmarks."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> float:
        self._now += float(seconds)
        return self._now

    def set(self, timestamp: float) -> None:
        self._now = float(timestamp)
