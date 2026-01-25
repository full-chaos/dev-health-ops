"""SQLite metrics sink implementation."""

from __future__ import annotations

from sqlalchemy import text

from dev_health_ops.metrics.sinks.sqlalchemy_base import SQLAlchemyMetricsSink


def _normalize_sqlite_url(db_url: str) -> str:
    """Normalize SQLite URL to sync driver."""
    if "sqlite+aiosqlite://" in db_url:
        return db_url.replace("sqlite+aiosqlite://", "sqlite://", 1)
    return db_url


class SQLiteMetricsSink(SQLAlchemyMetricsSink):
    """SQLite-specific metrics sink."""

    @property
    def backend_type(self) -> str:
        return "sqlite"

    def __init__(self, db_url: str) -> None:
        """Initialize SQLite sink with normalized URL."""
        super().__init__(_normalize_sqlite_url(db_url))

    @staticmethod
    def _table_has_column(conn, table: str, column: str) -> bool:
        """Check column existence using SQLite PRAGMA."""
        try:
            rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        except Exception:
            return False
        cols = {str(r[1]) for r in rows if len(r) >= 2}
        return column in cols
