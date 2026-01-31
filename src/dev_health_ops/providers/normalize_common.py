from datetime import datetime, timezone
from typing import Optional, overload


@overload
def to_utc(dt: None) -> None: ...


@overload
def to_utc(dt: datetime) -> datetime: ...


def to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
