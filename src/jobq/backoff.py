"""Retry backoff policies."""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class Backoff:
    """Exponential backoff with full-range jitter.

    delay(attempt) = min(base * factor ** (attempt - 1), max_delay) * U(1 - jitter, 1 + jitter)

    ``attempt`` is 1 for the first failure. Jitter spreads retries so that a
    burst of failures (a downstream outage) does not come back as a burst.
    """

    base: float = 1.0
    factor: float = 2.0
    max_delay: float = 300.0
    jitter: float = 0.2

    def delay(self, attempt: int, rng: random.Random | None = None) -> float:
        if attempt < 1:
            attempt = 1
        raw = min(self.base * (self.factor ** (attempt - 1)), self.max_delay)
        if self.jitter <= 0:
            return raw
        r = rng if rng is not None else random
        spread = r.uniform(1.0 - self.jitter, 1.0 + self.jitter)
        return max(0.0, raw * spread)


DEFAULT_BACKOFF = Backoff()
