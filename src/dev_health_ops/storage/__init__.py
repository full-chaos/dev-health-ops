from __future__ import annotations

from typing import Optional, Union, Callable

from .clickhouse import ClickHouseStore
from .mongo import MongoStore
from .sqlalchemy import SQLAlchemyStore
from .utils import (
    _parse_date_value,
    _parse_datetime_value,
    _register_sqlite_datetime_adapters,
    _serialize_value,
    model_to_dict,
)


def detect_db_type(conn_string: str) -> str:
    """
    Detect database type from connection string.

    :param conn_string: Database connection string.
    :return: Database type ('postgres', 'sqlite', 'mongo', or 'clickhouse').
    :raises ValueError: If database type cannot be determined.
    """
    if not conn_string:
        raise ValueError("Connection string is required")

    conn_lower = conn_string.lower()

    # ClickHouse connection strings
    if conn_lower.startswith("clickhouse://") or conn_lower.startswith(
        (
            "clickhouse+http://",
            "clickhouse+https://",
            "clickhouse+native://",
        )
    ):
        return "clickhouse"

    # MongoDB connection strings
    if conn_lower.startswith("mongodb://") or conn_lower.startswith("mongodb+srv://"):
        return "mongo"

    # PostgreSQL connection strings
    if conn_lower.startswith("postgresql://") or conn_lower.startswith("postgres://"):
        return "postgres"
    if conn_lower.startswith("postgresql+asyncpg://"):
        return "postgres"

    # SQLite connection strings
    if conn_lower.startswith("sqlite://") or conn_lower.startswith(
        "sqlite+aiosqlite://"
    ):
        return "sqlite"

    # Extract scheme for better error reporting
    scheme = conn_string.split("://", 1)[0] if "://" in conn_string else "unknown"
    raise ValueError(
        f"Could not detect database type from connection string. "
        f"Supported: mongodb://, postgresql://, postgres://, sqlite://, "
        f"clickhouse://, or variations with async drivers. Got scheme: '{scheme}', "
        f"connection string (first 100 chars): {conn_string[:100]}..."
    )


def create_store(
    conn_string: str,
    db_type: Optional[str] = None,
    db_name: Optional[str] = None,
    echo: bool = False,
) -> Union["SQLAlchemyStore", "MongoStore", "ClickHouseStore"]:
    """
    Create a storage backend based on the connection string.

    This factory function automatically detects the database type from the
    connection string and returns the appropriate store implementation.

    :param conn_string: Database connection string.
    :param db_type: Optional explicit database type ('postgres', 'sqlite', 'mongo', 'clickhouse').
                   If not provided, it will be auto-detected from conn_string.
    :param db_name: Optional database name (for MongoDB).
    :param echo: Whether to echo SQL statements (for SQLAlchemy).
    :return: Appropriate store instance (SQLAlchemyStore, MongoStore, or ClickHouseStore).
    """
    if db_type is None:
        db_type = detect_db_type(conn_string)

    # Normalize connection string for async drivers if using SQLAlchemyStore
    if db_type in ("postgres", "postgresql", "sqlite"):
        if conn_string.startswith("sqlite://") and not conn_string.startswith(
            "sqlite+aiosqlite://"
        ):
            conn_string = conn_string.replace("sqlite://", "sqlite+aiosqlite://", 1)
        elif conn_string.startswith("postgresql://") and not conn_string.startswith(
            "postgresql+asyncpg://"
        ):
            conn_string = conn_string.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )
        elif conn_string.startswith("postgres://"):
            conn_string = conn_string.replace("postgres://", "postgresql+asyncpg://", 1)

    db_type = db_type.lower()

    if db_type == "mongo":
        return MongoStore(conn_string, db_name=db_name)
    elif db_type == "clickhouse":
        return ClickHouseStore(conn_string)
    elif db_type in ("postgres", "postgresql", "sqlite"):
        return SQLAlchemyStore(conn_string, echo=echo)
    else:
        raise ValueError(
            f"Unsupported database type: {db_type}. "
            f"Supported types: postgres, sqlite, mongo, clickhouse"
        )


def resolve_db_type(db_url: str, db_type: Optional[str]) -> str:
    """
    Resolve database type from URL or explicit type.
    """
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


async def run_with_store(db_url: str, db_type: str, handler: Callable) -> None:
    """
    Helper to create a store and run a handler within its context.
    """
    store = create_store(db_url, db_type)
    async with store:
        await handler(store)


__all__ = [
    "ClickHouseStore",
    "MongoStore",
    "SQLAlchemyStore",
    "create_store",
    "detect_db_type",
    "model_to_dict",
    "resolve_db_type",
    "run_with_store",
    "_parse_date_value",
    "_parse_datetime_value",
    "_register_sqlite_datetime_adapters",
    "_serialize_value",
]
