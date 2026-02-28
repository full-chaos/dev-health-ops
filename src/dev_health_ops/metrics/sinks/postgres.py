"""PostgreSQL analytics sink — REMOVED.

PostgreSQL support for analytics was removed in CHAOS-641.
ClickHouse is the only supported analytics backend.
"""

raise ImportError(
    "PostgresMetricsSink has been removed. "
    "Use ClickHouseMetricsSink (CLICKHOUSE_URI) instead. "
    "See docs/architecture/database-architecture.md"
)
