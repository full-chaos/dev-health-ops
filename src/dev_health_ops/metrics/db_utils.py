"""Database URL normalization utilities.

This module provides functions to normalize async database URLs to their sync
equivalents for compatibility with libraries that don't support async drivers
(e.g., clickhouse-connect, raw psycopg2).
"""


def normalize_sqlite_url(db_url: str) -> str:
    """Convert async SQLite URL to sync for driver compatibility.

    Args:
        db_url: Database URL, potentially using aiosqlite driver.

    Returns:
        Normalized URL with sync sqlite driver.

    Examples:
        >>> normalize_sqlite_url("sqlite+aiosqlite:///data.db")
        'sqlite:///data.db'
        >>> normalize_sqlite_url("sqlite:///data.db")
        'sqlite:///data.db'
    """
    if "sqlite+aiosqlite://" in db_url:
        return db_url.replace("sqlite+aiosqlite://", "sqlite://", 1)
    return db_url


def normalize_postgres_url(db_url: str) -> str:
    """Convert async PostgreSQL URL to sync for driver compatibility.

    Args:
        db_url: Database URL, potentially using asyncpg driver.

    Returns:
        Normalized URL with sync postgresql driver.

    Examples:
        >>> normalize_postgres_url("postgresql+asyncpg://user:pass@host/db")
        'postgresql://user:pass@host/db'
        >>> normalize_postgres_url("postgresql://user:pass@host/db")
        'postgresql://user:pass@host/db'
    """
    if "postgresql+asyncpg://" in db_url:
        return db_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return db_url


def normalize_db_url(db_url: str) -> str:
    """Normalize any async database URL to its sync equivalent.

    Applies all known normalization rules in sequence.

    Args:
        db_url: Database URL, potentially using async drivers.

    Returns:
        Normalized URL with sync drivers.

    Examples:
        >>> normalize_db_url("sqlite+aiosqlite:///data.db")
        'sqlite:///data.db'
        >>> normalize_db_url("postgresql+asyncpg://user:pass@host/db")
        'postgresql://user:pass@host/db'
        >>> normalize_db_url("clickhouse://localhost:8123/default")
        'clickhouse://localhost:8123/default'
    """
    url = normalize_sqlite_url(db_url)
    url = normalize_postgres_url(url)
    return url
