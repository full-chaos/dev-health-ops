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

# Process-local sink cache: keyed by (pid, resolved_dsn) -> BaseMetricsSink
_process_sinks: dict[tuple[int, str], BaseMetricsSink] = {}


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


def _resolve_dsn(dsn: str | None = None) -> str:
    """Resolve DSN from argument or environment, raising if absent."""
    if dsn is None:
        dsn = os.environ.get("CLICKHOUSE_URI") or os.environ.get("DEV_HEALTH_SINK")
    if not dsn:
        raise ValueError(
            "No sink DSN provided. Set CLICKHOUSE_URI environment variable "
            "or pass dsn parameter."
        )
    return dsn


def get_process_sink(dsn: str | None = None) -> BaseMetricsSink:
    """
    Return a process-local cached ClickHouseMetricsSink.

    The cache is keyed by (os.getpid(), resolved_dsn). A new sink is created
    on first call per (pid, dsn) pair and reused on subsequent calls. Callers
    MUST NOT call .close() on the returned sink — it is shared for the lifetime
    of the worker process.

    Use reset_process_sinks() to close and evict all cached sinks (called on
    worker_process_init and worker_process_shutdown to stay fork-safe).

    Args:
        dsn: ClickHouse connection string. If not provided, reads from
             CLICKHOUSE_URI or DEV_HEALTH_SINK environment variable.

    Returns:
        A cached ClickHouseMetricsSink instance for this process.
    """
    resolved = _resolve_dsn(dsn)
    key = (os.getpid(), resolved)
    if key not in _process_sinks:
        detect_backend(resolved)  # validate scheme early
        from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

        logger.debug("Creating process-local ClickHouse sink (pid=%d)", key[0])
        _process_sinks[key] = ClickHouseMetricsSink(resolved)
    return _process_sinks[key]


def reset_process_sinks() -> None:
    """
    Close and evict all process-local cached sinks.

    Called on worker_process_init (after fork, before tasks run) and on
    worker_process_shutdown. Safe to call multiple times.
    """
    for sink in list(_process_sinks.values()):
        try:
            sink.close()
        except Exception:
            logger.debug("Error closing cached sink during reset", exc_info=True)
    _process_sinks.clear()
