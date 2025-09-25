from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

from litestar.exceptions import ValidationException

from sensor_api.data.models import Statistic


def _dedupe_preserve_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for it in items:
        if it and it not in seen:
            seen.add(it)
            result.append(it)
    return result


def parse_sensors_param(raw: str | None) -> list[str] | None:
    """Parse a comma-separated sensors string into a de-duplicated list.

    - Trims whitespace
    - Removes empties
    - Preserves order while de-duplicating
    - Returns None if empty (means "all sensors")
    """
    if not raw:
        return None
    parts = [p.strip() for p in raw.split(",")]
    result = _dedupe_preserve_order([p for p in parts if p])
    return result if result else None


def parse_metrics_param(raw: str | None) -> list[str]:
    """Parse a comma-separated metrics string into a de-duplicated list.

    - Trims whitespace
    - Removes empties
    - Preserves order while de-duplicating
    - Returns empty list if None (means "all metrics")
    """
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    return _dedupe_preserve_order([p for p in parts if p])


def parse_stat(raw: str | None) -> Statistic:
    """Normalize a statistic alias into a Statistic enum.

    Accepts: "average", "min", "max", "sum" (case-insensitive).
    Defaults to average when None/empty.
    Raises ValidationException on invalid input.
    """
    stat_norm = (raw or "average").strip().lower()
    stat_map = {
        "average": Statistic.AVG,
        "min": Statistic.MIN,
        "max": Statistic.MAX,
        "sum": Statistic.SUM,
    }
    try:
        return stat_map[stat_norm]
    except KeyError as e:
        raise ValidationException("Invalid 'stat' value. Use one of: average, min, max, sum") from e


def compute_date_range_from_days(days: int | None, *, now: datetime | None = None) -> tuple[datetime, datetime]:
    """Compute [start, end] UTC range given a days-back integer.

    - Valid range: 1..31; default: exactly 1 day when None
    - Returns timezone-aware datetimes in UTC
    - Raises ValidationException if out of range
    """
    if days is not None:
        if days < 1 or days > 31:
            raise ValidationException("'days' must be between 1 and 31")
        days_back = days
    else:
        days_back = 1

    end = now or datetime.now(UTC)

    start = end - timedelta(days=days_back)
    return start, end
