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
from typing import Optional

from dev_health_ops.metrics.sinks.backend_types import detect_backend, SinkBackend
from dev_health_ops.metrics.sinks.base import BaseMetricsSink

logger = logging.getLogger(__name__)


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
