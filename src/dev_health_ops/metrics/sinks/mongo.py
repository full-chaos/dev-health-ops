"""MongoDB analytics sink — REMOVED.

MongoDB support for analytics was removed in CHAOS-641.
ClickHouse is the only supported analytics backend.
"""

raise ImportError(
    "MongoMetricsSink has been removed. "
    "Use ClickHouseMetricsSink (CLICKHOUSE_URI) instead. "
    "See docs/architecture/database-architecture.md"
)
