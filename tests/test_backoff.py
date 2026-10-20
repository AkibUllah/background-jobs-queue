from __future__ import annotations

import random

from jobq.backoff import Backoff


def test_exponential_growth_without_jitter():
    b = Backoff(base=1, factor=2, max_delay=100, jitter=0)
    assert [b.delay(n) for n in (1, 2, 3, 4)] == [1, 2, 4, 8]


def test_capped_at_max_delay():
    b = Backoff(base=10, factor=10, max_delay=300, jitter=0)
    assert b.delay(5) == 300


def test_jitter_stays_within_bounds_and_is_deterministic_with_seeded_rng():
    b = Backoff(base=10, factor=1, max_delay=10, jitter=0.25)
    rng = random.Random(42)
    samples = [b.delay(1, rng) for _ in range(200)]
    assert all(7.5 <= s <= 12.5 for s in samples)
    assert len(set(round(s, 6) for s in samples)) > 50
    assert b.delay(1, random.Random(7)) == b.delay(1, random.Random(7))


def test_attempt_below_one_is_treated_as_first():
    b = Backoff(base=3, jitter=0)
    assert b.delay(0) == b.delay(1) == 3
