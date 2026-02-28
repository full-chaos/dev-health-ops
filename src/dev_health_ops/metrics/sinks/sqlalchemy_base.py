"""SQLAlchemy base sink — REMOVED.

SQLAlchemy-based analytics sinks were removed in CHAOS-641.
ClickHouse is the only supported analytics backend.
"""

raise ImportError(
    "SQLAlchemyMetricsSink has been removed. "
    "Use ClickHouseMetricsSink (CLICKHOUSE_URI) instead. "
    "See docs/architecture/database-architecture.md"
)
