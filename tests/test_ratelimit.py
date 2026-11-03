from __future__ import annotations

import pytest

from jobq.ratelimit import RateLimit, refill, take_token


@pytest.mark.parametrize(
    ("spec", "limit", "period"),
    [
        ("10/s", 10, 1.0),
        ("10/sec", 10, 1.0),
        ("600/m", 600, 60.0),
        ("600/min", 600, 60.0),
        ("100/h", 100, 3600.0),
        (" 5 / 10s ", 5, 10.0),
        ("3/1.5s", 3, 1.5),
        ("2/2m", 2, 120.0),
        ("1/HOUR", 1, 3600.0),
    ],
)
def test_parse_valid_specs(spec: str, limit: int, period: float):
    parsed = RateLimit.parse(spec)
    assert parsed == RateLimit(limit, period)


@pytest.mark.parametrize("spec", ["", "10", "10/", "/s", "10/d", "ten/s", "-1/s", "0/s", "1/0s"])
def test_parse_rejects_invalid_specs(spec: str):
    with pytest.raises(ValueError):
        RateLimit.parse(spec)


def test_parse_passes_rate_limit_through():
    rl = RateLimit(4, 2.0)
    assert RateLimit.parse(rl) is rl


@pytest.mark.parametrize(
    ("rl", "text"),
    [
        (RateLimit(10, 1.0), "10/s"),
        (RateLimit(600, 60.0), "600/m"),
        (RateLimit(5, 3600.0), "5/h"),
        (RateLimit(5, 10.0), "5/10s"),
    ],
)
def test_str_round_trips(rl: RateLimit, text: str):
    assert str(rl) == text
    assert RateLimit.parse(text) == rl


def test_missing_bucket_is_full_and_refill_is_capped():
    rl = RateLimit(10, 1.0)
    assert refill(rl, None, None, 100.0) == 10.0
    assert refill(rl, 0.0, 100.0, 100.5) == pytest.approx(5.0)
    assert refill(rl, 0.0, 100.0, 500.0) == 10.0


def test_refill_ignores_clock_going_backwards():
    rl = RateLimit(10, 1.0)
    assert refill(rl, 2.0, 100.0, 99.0) == 2.0


def test_take_token_drains_then_reports_wait():
    rl = RateLimit(2, 1.0)  # 2 tokens, refills one every 0.5s
    ok, tokens, wait = take_token(rl, None, None, 0.0)
    assert ok and tokens == 1.0 and wait == 0.0
    ok, tokens, wait = take_token(rl, tokens, 0.0, 0.0)
    assert ok and tokens == 0.0
    ok, tokens, wait = take_token(rl, tokens, 0.0, 0.1)
    assert not ok
    assert tokens == pytest.approx(0.2)
    assert wait == pytest.approx(0.4)
    ok, tokens, _ = take_token(rl, 0.0, 0.0, 0.5)
    assert ok and tokens == pytest.approx(0.0)


def test_per_minute_limit_waits_for_the_next_token():
    rl = RateLimit.parse("6/m")  # one token every 10 seconds
    ok, _, wait = take_token(rl, 0.0, 0.0, 4.0)
    assert not ok and wait == pytest.approx(6.0)
