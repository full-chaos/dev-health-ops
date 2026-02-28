"""SQLite analytics sink — REMOVED.

SQLite support for analytics was removed in CHAOS-641.
ClickHouse is the only supported analytics backend.
"""

raise ImportError(
    "SQLiteMetricsSink has been removed. "
    "Use ClickHouseMetricsSink (CLICKHOUSE_URI) instead. "
    "See docs/architecture/database-architecture.md"
)
