"""
Sink implementations for writing derived metrics.

Sinks persist derived metrics data to various backends:
- ClickHouse: append-only analytics store (primary, only supported for analytics)
- MongoDB: document store with idempotent upserts (DEPRECATED for analytics)
- SQLite: file-based relational store (DEPRECATED for analytics)
- PostgreSQL: production relational store (DEPRECATED for analytics)

DEPRECATION NOTICE:
MongoDB, PostgreSQL, and SQLite are deprecated for analytics use. ClickHouse is
the only supported analytics backend. Migrate to ClickHouse. See
docs/architecture/database-architecture.md for migration guidance.

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
