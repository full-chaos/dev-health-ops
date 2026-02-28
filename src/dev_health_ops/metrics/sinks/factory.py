"""
Factory for creating metrics sink instances.

ClickHouse is the only supported analytics backend (CHAOS-641).
MongoDB, PostgreSQL, and SQLite support was removed.

Example:
    sink = create_sink("clickhouse://localhost:8123/default")
"""

from __future__ import annotations

import logging
import os
from enum import Enum
from urllib.parse import urlparse

from dev_health_ops.metrics.sinks.base import BaseMetricsSink

logger = logging.getLogger(__name__)


class SinkBackend(str, Enum):
    """Supported sink backend types."""

    CLICKHOUSE = "clickhouse"


def detect_backend(dsn: str) -> SinkBackend:
    """
    Detect the sink backend type from a connection string.

    Args:
        dsn: Connection string (URL format)

    Returns:
        SinkBackend enum value

    Raises:
        ValueError: If the scheme is not a supported ClickHouse variant
    """
    parsed = urlparse(dsn)
    scheme = parsed.scheme.lower()

    if scheme in (
        "clickhouse",
        "clickhouse+native",
        "clickhouse+http",
        "clickhouse+https",
    ):
        return SinkBackend.CLICKHOUSE

    raise ValueError(
        f"Unknown or unsupported sink scheme '{scheme}'. "
        "Only ClickHouse is supported (CHAOS-641). "
        "Use a clickhouse:// connection string."
    )


def create_sink(dsn: str | None = None) -> BaseMetricsSink:
    """
    Create a ClickHouse metrics sink instance.

    Args:
        dsn: ClickHouse connection string. If not provided, reads from
             CLICKHOUSE_URI or DEV_HEALTH_SINK environment variable.

    Returns:
        A configured ClickHouseMetricsSink instance.

    Raises:
        ValueError: If no DSN is provided or the DSN is not a ClickHouse URI.
    """
    if dsn is None:
        dsn = os.environ.get("CLICKHOUSE_URI") or os.environ.get("DEV_HEALTH_SINK")

    if not dsn:
        raise ValueError(
            "No sink DSN provided. Set CLICKHOUSE_URI environment variable "
            "or pass dsn parameter to create_sink()."
        )

    backend = detect_backend(dsn)
    logger.info("Creating %s sink from DSN", backend.value)

    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    return ClickHouseMetricsSink(dsn)
