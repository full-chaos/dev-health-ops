from datetime import datetime, timezone
from typing import Optional, overload


@overload
def to_utc(dt: None) -> None: ...


@overload
def to_utc(dt: datetime) -> datetime: ...


def to_utc(dt: Optional[datetime]) -> Optional[datetime]:
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
