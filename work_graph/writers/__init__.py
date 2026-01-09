"""
Writers for persisting work graph data.
"""

from work_graph.writers.clickhouse import ClickHouseWorkGraphWriter

__all__ = [
    "ClickHouseWorkGraphWriter",
]
