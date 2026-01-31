from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy.inspection import inspect


def _register_sqlite_datetime_adapters() -> None:
    try:
        import sqlite3
    except Exception:
        return
    sqlite3.register_adapter(date, lambda value: value.isoformat())
    sqlite3.register_adapter(datetime, lambda value: value.isoformat(" "))


_register_sqlite_datetime_adapters()


def _parse_date_value(value: Any) -> Optional[date]:
    if value:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            pass
    return None


def _parse_datetime_value(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _serialize_value(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def model_to_dict(model: Any) -> Dict[str, Any]:
    mapper = inspect(model.__class__)
    data: Dict[str, Any] = {}
    for column in mapper.columns:
        data[column.key] = _serialize_value(getattr(model, column.key))
    return data


def detect_db_type(conn_string: str) -> str:
    if not conn_string:
        raise ValueError("Connection string is required")

    conn_lower = conn_string.lower()

    if conn_lower.startswith("clickhouse://") or conn_lower.startswith(
        (
            "clickhouse+http://",
            "clickhouse+https://",
            "clickhouse+native://",
        )
    ):
        return "clickhouse"

    if conn_lower.startswith("mongodb://") or conn_lower.startswith("mongodb+srv://"):
        return "mongo"

    if conn_lower.startswith("postgresql://") or conn_lower.startswith("postgres://"):
        return "postgres"
    if conn_lower.startswith("postgresql+asyncpg://"):
        return "postgres"

    if conn_lower.startswith("sqlite://") or conn_lower.startswith(
        "sqlite+aiosqlite://"
    ):
        return "sqlite"

    scheme = conn_string.split("://", 1)[0] if "://" in conn_string else "unknown"
    raise ValueError(
        f"Could not detect database type from connection string. "
        f"Supported: mongodb://, postgresql://, postgres://, sqlite://, "
        f"clickhouse://, or variations with async drivers. Got scheme: '{scheme}', "
        f"connection string (first 100 chars): {conn_string[:100]}..."
    )


def resolve_db_type(db_url: str, db_type: Optional[str]) -> str:
    if db_type:
        resolved = db_type.lower()
    else:
        try:
            resolved = detect_db_type(db_url)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc

    if resolved not in {"postgres", "mongo", "sqlite", "clickhouse"}:
        raise SystemExit(
            "DB_TYPE must be 'postgres', 'mongo', 'sqlite', or 'clickhouse'"
        )
    return resolved
