from datetime import date, datetime, timezone
from typing import overload
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@overload
def to_utc(dt: None) -> None:
    pass


@overload
def to_utc(dt: datetime) -> datetime:
    pass


def to_utc(dt: datetime | None) -> datetime | None:
    """Ensure datetime has UTC tzinfo. Handles None gracefully."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def naive_utc(dt: datetime) -> datetime:
    """Convert datetime to naive UTC (strips tzinfo). For BSON/ClickHouse."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def utc_today() -> date:
    """Return the current date in UTC, independent of container timezone."""
    return datetime.now(timezone.utc).date()


def validate_timezone_name(tz_name: str | None) -> None:
    """Raise ``ValueError`` if ``tz_name`` is a non-empty, unrecognized IANA zone.

    Empty / ``None`` is allowed (callers default to UTC). Used at schedule write
    paths so an invalid timezone is rejected up front with a user-visible error
    instead of silently falling back to UTC at dispatch time (CHAOS-2689).
    """
    if not tz_name:
        return
    try:
        ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"Invalid timezone: {tz_name!r}") from exc
