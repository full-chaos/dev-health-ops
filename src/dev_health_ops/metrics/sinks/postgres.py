"""PostgreSQL metrics sink implementation with COPY bulk insert support."""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import Any, List, Sequence, TypeVar

from sqlalchemy import inspect

from dev_health_ops.metrics.sinks.sqlalchemy_base import SQLAlchemyMetricsSink

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _normalize_postgres_url(db_url: str) -> str:
    """Normalize PostgreSQL URL to sync driver."""
    if "postgresql+asyncpg://" in db_url:
        return db_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return db_url


def _serialize_value(value: Any) -> str:
    """Serialize a Python value to a string for COPY format."""
    if value is None:
        return "\\N"  # Postgres NULL representation
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, dict):
        import json

        return json.dumps(value)
    return str(value)


class PostgresMetricsSink(SQLAlchemyMetricsSink):
    """Postgres-specific metrics sink with COPY bulk insert support."""

    @property
    def backend_type(self) -> str:
        return "postgres"

    def __init__(self, db_url: str) -> None:
        """Initialize Postgres sink with normalized URL."""
        super().__init__(_normalize_postgres_url(db_url))

    @staticmethod
    def _table_has_column(conn, table: str, column: str) -> bool:
        """Check column existence using SQLAlchemy inspect."""
        try:
            columns = inspect(conn).get_columns(table)
        except Exception:
            return False
        return column in {col.get("name") for col in columns}

    def _copy_upsert(
        self,
        table: str,
        columns: List[str],
        primary_keys: List[str],
        rows: Sequence[Any],
    ) -> None:
        """
        Bulk insert using Postgres COPY via temp table + upsert.

        This is significantly faster than individual INSERTs for large batches.
        Uses: CREATE TEMP TABLE -> COPY -> INSERT...ON CONFLICT
        """
        if not rows:
            return

        # Build CSV-like data in memory
        buffer = io.StringIO()
        writer = csv.writer(
            buffer, delimiter="\t", quoting=csv.QUOTE_NONE, escapechar="\\"
        )

        for row in rows:
            row_dict = asdict(row) if hasattr(row, "__dataclass_fields__") else row
            values = [_serialize_value(row_dict.get(col)) for col in columns]
            writer.writerow(values)

        buffer.seek(0)
        data = buffer.getvalue()

        # Use raw connection for COPY
        with self.engine.connect() as conn:
            raw_conn = conn.connection.dbapi_connection
            if raw_conn is None:
                raise RuntimeError("No raw DB connection available")
            cursor = raw_conn.cursor()

            try:
                temp_table = f"_tmp_{table}"

                # Create temp table with same structure
                cursor.execute(f"""
                    CREATE TEMP TABLE IF NOT EXISTS {temp_table}
                    (LIKE {table} INCLUDING DEFAULTS)
                    ON COMMIT DROP
                """)

                # Truncate in case of retry
                cursor.execute(f"TRUNCATE {temp_table}")

                # COPY data into temp table
                copy_buffer = io.StringIO(data)
                cursor.copy_from(
                    copy_buffer,
                    temp_table,
                    sep="\t",
                    null="\\N",
                    columns=columns,
                )

                # Build upsert statement
                col_list = ", ".join(columns)
                update_cols = [c for c in columns if c not in primary_keys]
                update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
                conflict_cols = ", ".join(primary_keys)

                upsert_sql = f"""
                    INSERT INTO {table} ({col_list})
                    SELECT {col_list} FROM {temp_table}
                    ON CONFLICT ({conflict_cols}) DO UPDATE SET
                    {update_set}
                """
                cursor.execute(upsert_sql)

                raw_conn.commit()
                logger.debug("COPY upsert: %d rows into %s", len(rows), table)

            except Exception as e:
                raw_conn.rollback()
                logger.warning(
                    "COPY failed for %s, falling back to standard insert: %s", table, e
                )
                # Fall back to parent's standard insert method
                raise
            finally:
                cursor.close()
