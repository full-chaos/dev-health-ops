"""
Factory for creating metrics sink instances.

The sink backend is selected by passing a connection string to create_sink()
or via the DEV_HEALTH_SINK environment variable (factory-specific usage).
Note: Most parts of the application use DATABASE_URI for database configuration.

Supported backends:
- clickhouse: ClickHouse (default for analytics)
- mongo: MongoDB
- sqlite: SQLite (file-based)
- postgres: PostgreSQL

Example:
    # Via connection string
    sink = create_sink("clickhouse://localhost:8123/default")

    # Via env var (factory-specific)
    os.environ["DEV_HEALTH_SINK"] = "mongo://localhost:27017/dev_health"
    sink = create_sink()
"""

from __future__ import annotations

import logging
import os
from enum import Enum
from typing import Optional
from urllib.parse import urlparse

from dev_health_ops.metrics.sinks.base import BaseMetricsSink

logger = logging.getLogger(__name__)


class SinkBackend(str, Enum):
    """Supported sink backend types."""

    CLICKHOUSE = "clickhouse"
    MONGO = "mongo"
    SQLITE = "sqlite"
    POSTGRES = "postgres"


def detect_backend(dsn: str) -> SinkBackend:
    """
    Detect the sink backend type from a connection string.

    Args:
        dsn: Connection string (URL format)

    Returns:
        SinkBackend enum value

    Raises:
        ValueError: If the scheme is not recognized
    """
    parsed = urlparse(dsn)
    scheme = parsed.scheme.lower()

    # Handle SQLAlchemy-style schemes
    if scheme in (
        "clickhouse",
        "clickhouse+native",
        "clickhouse+http",
        "clickhouse+https",
    ):
        return SinkBackend.CLICKHOUSE
    elif scheme in ("mongodb", "mongodb+srv", "mongo"):
        return SinkBackend.MONGO
    elif scheme in ("sqlite", "sqlite+aiosqlite"):
        return SinkBackend.SQLITE
    elif scheme in (
        "postgresql",
        "postgresql+asyncpg",
        "postgresql+psycopg2",
        "postgres",
    ):
        return SinkBackend.POSTGRES
    else:
        raise ValueError(
            f"Unknown sink scheme '{scheme}'. Supported: "
            "clickhouse, mongo/mongodb, sqlite, postgres/postgresql"
        )


def create_sink(dsn: Optional[str] = None) -> BaseMetricsSink:
    """
    Create a metrics sink instance for the specified backend.

    Args:
        dsn: Connection string. If not provided, reads from DEV_HEALTH_SINK
             environment variable.

    Returns:
        A configured BaseMetricsSink implementation.

    Raises:
        ValueError: If no DSN is provided and DEV_HEALTH_SINK is not set.
        ValueError: If the DSN scheme is not recognized.

    Example:
        # Explicit DSN
        sink = create_sink("clickhouse://localhost:8123/default")

        # From environment
        os.environ["DEV_HEALTH_SINK"] = "mongo://localhost:27017/dev_health"
        sink = create_sink()
    """
    if dsn is None:
        dsn = os.environ.get("DEV_HEALTH_SINK")

    if not dsn:
        raise ValueError(
            "No sink DSN provided. Set DEV_HEALTH_SINK environment variable "
            "or pass dsn parameter to create_sink()."
        )

    backend = detect_backend(dsn)
    logger.info("Creating %s sink from DSN", backend.value)

    if backend == SinkBackend.CLICKHOUSE:
        from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

        return ClickHouseMetricsSink(dsn)

    elif backend == SinkBackend.MONGO:
        from dev_health_ops.metrics.sinks.mongo import MongoMetricsSink

        return MongoMetricsSink(dsn)

    elif backend == SinkBackend.SQLITE:
        from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink

        return SQLiteMetricsSink(dsn)

    elif backend == SinkBackend.POSTGRES:
        from dev_health_ops.metrics.sinks.postgres import PostgresMetricsSink

        return PostgresMetricsSink(dsn)

    else:
        raise ValueError(f"Unsupported backend: {backend}")
