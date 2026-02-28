"""
Sink implementations for writing derived metrics.

ClickHouse is the only supported analytics backend.
All other backends (MongoDB, SQLite, PostgreSQL) were removed in CHAOS-641.

Usage:
    from dev_health_ops.metrics.sinks import create_sink, BaseMetricsSink

    sink = create_sink("clickhouse://localhost:8123/default")
    sink.ensure_schema()
    sink.write_repo_metrics(rows)
    sink.close()
"""

from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.sinks.factory import (
    SinkBackend,
    create_sink,
    detect_backend,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

__all__ = [
    "BaseMetricsSink",
    "SinkBackend",
    "create_sink",
    "detect_backend",
    "ClickHouseMetricsSink",
]
