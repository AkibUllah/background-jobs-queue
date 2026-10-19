"""Five-field cron expression parser with timezone-aware ``next_after``.

Supported syntax per field: ``*``, ``n``, ``a-b``, ``*/s``, ``a-b/s``, comma
lists, and the aliases ``@hourly``, ``@daily``, ``@midnight``, ``@weekly``,
``@monthly``, ``@yearly``/``@annually``. Day-of-week accepts 0-7 (0 and 7 are
Sunday) and the three-letter English names; month accepts 1-12 or names.

Day-of-month and day-of-week follow Vixie cron semantics: when both are
restricted the job runs when *either* matches.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from jobq.exceptions import InvalidCron

ALIASES = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}

MONTH_NAMES = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}  # fmt: skip
DOW_NAMES = {"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}


def _parse_field(
    text: str, low: int, high: int, names: dict[str, int] | None = None
) -> tuple[frozenset[int], bool]:
    """Return (allowed values, is_wildcard)."""
    allowed: set[int] = set()
    wildcard = False

    def value(token: str) -> int:
        token = token.strip().lower()
        if names and token in names:
            return names[token]
        if not token.lstrip("-").isdigit():
            raise InvalidCron(f"invalid value {token!r}")
        return int(token)

    for part in text.split(","):
        part = part.strip()
        if not part:
            raise InvalidCron(f"empty list element in {text!r}")
        step = 1
        if "/" in part:
            rng, step_text = part.split("/", 1)
            if not step_text.isdigit() or int(step_text) < 1:
                raise InvalidCron(f"invalid step in {part!r}")
            step = int(step_text)
        else:
            rng = part
        if rng == "*":
            start, end = low, high
            if step == 1:
                wildcard = True
        elif "-" in rng:
            a, b = rng.split("-", 1)
            start, end = value(a), value(b)
        else:
            start = end = value(rng)
            if step != 1:
                end = high  # "5/15" means every 15 starting at 5
        if start < low or end > high or start > end:
            raise InvalidCron(f"value out of range in {part!r} (allowed {low}-{high})")
        allowed.update(range(start, end + 1, step))
    return frozenset(allowed), wildcard


@dataclass(frozen=True)
class CronSpec:
    expression: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]
    day_wildcard: bool
    weekday_wildcard: bool

    @classmethod
    def parse(cls, expression: str) -> CronSpec:
        expr = expression.strip()
        expr = ALIASES.get(expr.lower(), expr)
        fields = expr.split()
        if len(fields) != 5:
            raise InvalidCron(f"expected 5 fields, got {len(fields)} in {expression!r}")
        minutes, _ = _parse_field(fields[0], 0, 59)
        hours, _ = _parse_field(fields[1], 0, 23)
        days, day_wild = _parse_field(fields[2], 1, 31)
        months, _ = _parse_field(fields[3], 1, 12, MONTH_NAMES)
        weekdays, dow_wild = _parse_field(fields[4], 0, 7, DOW_NAMES)
        if 7 in weekdays:
            weekdays = frozenset((weekdays - {7}) | {0})
        return cls(
            expression=expression,
            minutes=minutes,
            hours=hours,
            days=days,
            months=months,
            weekdays=weekdays,
            day_wildcard=day_wild,
            weekday_wildcard=dow_wild,
        )

    # ------------------------------------------------------------ matching
    def _day_matches(self, dt: datetime) -> bool:
        dom = dt.day in self.days
        dow = (dt.isoweekday() % 7) in self.weekdays
        if self.day_wildcard and self.weekday_wildcard:
            return True
        if self.day_wildcard:
            return dow
        if self.weekday_wildcard:
            return dom
        return dom or dow

    def matches(self, dt: datetime) -> bool:
        return (
            dt.month in self.months
            and self._day_matches(dt)
            and dt.hour in self.hours
            and dt.minute in self.minutes
        )

    def next_after(self, after: datetime) -> datetime:
        """First matching minute strictly after ``after``.

        ``after`` should be timezone-aware; the result keeps its tzinfo. The
        search walks month -> day -> hour -> minute, skipping whole units that
        cannot match, and gives up after four years (which only happens for
        impossible specs such as ``0 0 31 2 *``).
        """
        dt = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        limit = after + timedelta(days=366 * 4)
        while dt <= limit:
            if dt.month not in self.months:
                year, month = (dt.year + 1, 1) if dt.month == 12 else (dt.year, dt.month + 1)
                dt = dt.replace(year=year, month=month, day=1, hour=0, minute=0)
                continue
            if not self._day_matches(dt):
                dt = (dt + timedelta(days=1)).replace(hour=0, minute=0)
                continue
            if dt.hour not in self.hours:
                dt = (dt + timedelta(hours=1)).replace(minute=0)
                continue
            if dt.minute not in self.minutes:
                dt = dt + timedelta(minutes=1)
                continue
            return dt
        raise InvalidCron(f"{self.expression!r} never matches")

    def next_timestamp(self, after_ts: float, tz: str = "UTC") -> float:
        zone = ZoneInfo(tz)
        after = datetime.fromtimestamp(after_ts, zone)
        return self.next_after(after).timestamp()


def next_run(expression: str, after_ts: float, tz: str = "UTC") -> float:
    """Convenience wrapper: epoch seconds of the next run after ``after_ts``."""
    return CronSpec.parse(expression).next_timestamp(after_ts, tz)
