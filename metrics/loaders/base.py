"""Base utilities shared across all backend loaders."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence


def naive_utc(dt: datetime) -> datetime:
    """Convert a datetime to naive UTC (BSON/ClickHouse friendly)."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def to_utc(dt: datetime) -> datetime:
    """Ensure datetime has UTC tzinfo."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_uuid(value: Any) -> Optional[uuid.UUID]:
    """Parse a value into a UUID, returning None on failure."""
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except Exception:
        return None


def safe_json_loads(value: Any) -> Any:
    """Safely parse JSON, returning None on failure."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return None


def chunked(values: Sequence[str], chunk_size: int) -> List[List[str]]:
    """Split a sequence into chunks of the given size."""
    return [list(values[i : i + chunk_size]) for i in range(0, len(values), chunk_size)]


def clickhouse_query_dicts(
    client: Any, query: str, parameters: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Execute a ClickHouse query and return results as list of dicts."""
    result = client.query(query, parameters=parameters)
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]
