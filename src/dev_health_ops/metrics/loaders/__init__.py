"""
Data loader package for metrics job.

This package provides backend-specific loaders for ClickHouse, MongoDB, and SQLite,
with a unified interface for loading git facts, work items, and derived metrics.
"""

from __future__ import annotations

from dev_health_ops.metrics.loaders.base import (
    DataLoader,
    naive_utc,
    to_utc,
    parse_uuid,
    safe_json_loads,
    chunked,
    clickhouse_query_dicts,
)
from dev_health_ops.metrics.loaders.validation import (
    ValidationError,
    validate_rows,
    validate_or_raise,
    validate_typed_dict,
)

__all__ = [
    "DataLoader",
    "naive_utc",
    "to_utc",
    "parse_uuid",
    "safe_json_loads",
    "chunked",
    "clickhouse_query_dicts",
    "ValidationError",
    "validate_rows",
    "validate_or_raise",
    "validate_typed_dict",
]
