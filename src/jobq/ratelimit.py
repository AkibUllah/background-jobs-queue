"""Per-task rate limits: ``"10/s"`` style specs and the token bucket behind them.

A limit of ``N`` per ``period`` seconds is a token bucket holding at most ``N``
tokens that refills continuously at ``N / period`` tokens per second. Claiming
a job of a limited task takes one token; when the bucket is empty the job stays
queued and the claim reports how long until the next token, so workers sleep
exactly that long instead of polling.

``take_token`` is the reference implementation. The memory backend calls it
directly and the Redis claim script is a line-for-line port, so both backends
throttle identically.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_UNITS = {
    "s": 1.0,
    "sec": 1.0,
    "second": 1.0,
    "m": 60.0,
    "min": 60.0,
    "minute": 60.0,
    "h": 3600.0,
    "hour": 3600.0,
}
_SPEC = re.compile(r"^\s*(\d+)\s*/\s*(\d+(?:\.\d+)?)?\s*([a-z]+)\s*$")


@dataclass(frozen=True)
class RateLimit:
    """At most ``limit`` jobs per ``period`` seconds, shared by every worker."""

    limit: int
    period: float = 1.0

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError("rate limit must allow at least 1 job per period")
        if self.period <= 0:
            raise ValueError("rate limit period must be positive")

    @classmethod
    def parse(cls, spec: str | RateLimit) -> RateLimit:
        """Parse ``"10/s"``, ``"600/m"``, ``"100/h"`` or ``"5/10s"`` (5 per 10 seconds)."""
        if isinstance(spec, RateLimit):
            return spec
        match = _SPEC.match(spec.lower())
        if not match or match.group(3) not in _UNITS:
            raise ValueError(
                f"invalid rate limit {spec!r}; expected '<n>/s', '<n>/m', '<n>/h' or '<n>/<k>s'"
            )
        count, multiplier, unit = match.groups()
        period = _UNITS[unit] * (float(multiplier) if multiplier else 1.0)
        return cls(limit=int(count), period=period)

    @property
    def refill_rate(self) -> float:
        """Tokens added per second."""
        return self.limit / self.period

    def __str__(self) -> str:
        for unit, seconds in (("s", 1.0), ("m", 60.0), ("h", 3600.0)):
            if self.period == seconds:
                return f"{self.limit}/{unit}"
        return f"{self.limit}/{self.period:g}s"

    def to_dict(self) -> dict[str, float | int | str]:
        return {"spec": str(self), "limit": self.limit, "period": self.period}


def refill(limit: RateLimit, tokens: float | None, updated_at: float | None, now: float) -> float:
    """Tokens in the bucket at ``now``. A missing bucket is a full one."""
    if tokens is None or updated_at is None:
        return float(limit.limit)
    elapsed = max(0.0, now - updated_at)  # tolerate small clock skew between workers
    return min(float(limit.limit), tokens + elapsed * limit.refill_rate)


def take_token(
    limit: RateLimit, tokens: float | None, updated_at: float | None, now: float
) -> tuple[bool, float, float]:
    """Try to take one token.

    Returns ``(allowed, tokens_left, wait)``: ``wait`` is 0 when allowed,
    otherwise the seconds until the bucket holds a whole token again.
    """
    current = refill(limit, tokens, updated_at, now)
    if current >= 1.0:
        return True, current - 1.0, 0.0
    return False, current, (1.0 - current) / limit.refill_rate
