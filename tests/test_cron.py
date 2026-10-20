from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from jobq.cron import CronSpec, next_run
from jobq.exceptions import InvalidCron

UTC = timezone.utc


def dt(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


def test_every_minute_advances_one_minute():
    spec = CronSpec.parse("* * * * *")
    assert spec.next_after(dt(2025, 6, 15, 10, 30)) == dt(2025, 6, 15, 10, 31)


def test_seconds_are_truncated_before_stepping():
    spec = CronSpec.parse("* * * * *")
    after = datetime(2025, 6, 15, 10, 30, 45, tzinfo=UTC)
    assert spec.next_after(after) == dt(2025, 6, 15, 10, 31)


def test_specific_hour_minute_rolls_to_next_day():
    spec = CronSpec.parse("30 2 * * *")
    assert spec.next_after(dt(2025, 6, 15, 3, 0)) == dt(2025, 6, 16, 2, 30)
    assert spec.next_after(dt(2025, 6, 15, 1, 0)) == dt(2025, 6, 15, 2, 30)


def test_step_and_range_syntax():
    spec = CronSpec.parse("*/15 9-17 * * 1-5")
    assert spec.minutes == frozenset({0, 15, 30, 45})
    assert spec.hours == frozenset(range(9, 18))
    assert spec.weekdays == frozenset({1, 2, 3, 4, 5})
    # 2025-06-14 is a Saturday -> next is Monday 09:00
    assert spec.next_after(dt(2025, 6, 14, 12, 0)) == dt(2025, 6, 16, 9, 0)


def test_day_of_month_or_weekday_semantics():
    # Vixie cron: when both are restricted, either one matching fires the job.
    spec = CronSpec.parse("0 0 15 * fri")
    # 2025-06-13 is a Friday; from the 10th the next match is Fri 13th, not the 15th.
    assert spec.next_after(dt(2025, 6, 10, 0, 0)) == dt(2025, 6, 13, 0, 0)
    assert spec.next_after(dt(2025, 6, 13, 0, 0)) == dt(2025, 6, 15, 0, 0)


def test_month_names_and_aliases():
    assert CronSpec.parse("0 0 1 jan,jul *").months == frozenset({1, 7})
    assert CronSpec.parse("@daily").expression == "@daily"
    assert CronSpec.parse("@weekly").next_after(dt(2025, 6, 15, 0, 0)) == dt(2025, 6, 22, 0, 0)
    assert CronSpec.parse("@monthly").next_after(dt(2025, 6, 15, 0, 0)) == dt(2025, 7, 1, 0, 0)


def test_sunday_as_seven():
    spec = CronSpec.parse("0 0 * * 7")
    assert spec.weekdays == frozenset({0})
    assert spec.next_after(dt(2025, 6, 15, 0, 0)) == dt(2025, 6, 22, 0, 0)


def test_year_rollover():
    spec = CronSpec.parse("0 0 1 1 *")
    assert spec.next_after(dt(2025, 6, 15, 0, 0)) == dt(2026, 1, 1, 0, 0)


def test_timezone_aware_next_timestamp():
    # 02:30 Dhaka time (UTC+6) is 20:30 UTC the previous day.
    after = datetime(2025, 6, 15, 12, 0, tzinfo=ZoneInfo("Asia/Dhaka")).timestamp()
    ts = next_run("30 2 * * *", after, tz="Asia/Dhaka")
    got = datetime.fromtimestamp(ts, ZoneInfo("Asia/Dhaka"))
    assert (got.hour, got.minute, got.day) == (2, 30, 16)
    assert datetime.fromtimestamp(ts, UTC).hour == 20


@pytest.mark.parametrize(
    "expr",
    [
        "* * * *",
        "60 * * * *",
        "* 24 * * *",
        "* * 0 * *",
        "* * * 13 *",
        "* * * * 8",
        "a * * * *",
        "*/0 * * * *",
    ],
)
def test_invalid_expressions(expr: str):
    with pytest.raises(InvalidCron):
        CronSpec.parse(expr)


def test_impossible_spec_gives_up():
    with pytest.raises(InvalidCron):
        CronSpec.parse("0 0 31 2 *").next_after(dt(2025, 1, 1))
