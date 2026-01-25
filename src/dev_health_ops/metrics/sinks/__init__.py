"""
Sink implementations for writing derived metrics.

Sinks persist derived metrics data to various backends:
- ClickHouse: append-only analytics store (primary)
- MongoDB: document store with idempotent upserts
- SQLite: file-based relational store
- PostgreSQL: production relational store

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
from dev_health_ops.metrics.sinks.mongo import MongoMetricsSink
from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink
from dev_health_ops.metrics.sinks.postgres import PostgresMetricsSink

__all__ = [
    "BaseMetricsSink",
    "SinkBackend",
    "create_sink",
    "detect_backend",
    "ClickHouseMetricsSink",
    "MongoMetricsSink",
    "SQLiteMetricsSink",
    "PostgresMetricsSink",
]
