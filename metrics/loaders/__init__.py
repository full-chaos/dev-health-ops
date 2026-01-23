"""
Data loader package for metrics job.

This package provides backend-specific loaders for ClickHouse, MongoDB, and SQLite,
with a unified interface for loading git facts, work items, and derived metrics.
"""

from __future__ import annotations

from metrics.loaders.base import (
    naive_utc,
    to_utc,
    parse_uuid,
    safe_json_loads,
    chunked,
    clickhouse_query_dicts,
)

__all__ = [
    # Base utilities
    "naive_utc",
    "to_utc",
    "parse_uuid",
    "safe_json_loads",
    "chunked",
    "clickhouse_query_dicts",
]
