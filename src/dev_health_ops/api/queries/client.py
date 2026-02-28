from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from dev_health_ops.api.services.auth import get_current_org_id
from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.sinks.factory import create_sink

logger = logging.getLogger(__name__)

_SHARED_SINK: BaseMetricsSink | None = None
_SHARED_DSN: str | None = None


async def get_global_sink(dsn: str) -> BaseMetricsSink:
    """Get the shared metrics sink, initializing if needed."""
    global _SHARED_SINK, _SHARED_DSN

    if _SHARED_SINK and dsn != _SHARED_DSN:
        logger.info("Closing metrics sink due to DSN change")
        _SHARED_SINK.close()
        _SHARED_SINK = None

    if _SHARED_SINK is None:
        logger.info("Initializing global metrics sink for %s", dsn)
        _SHARED_SINK = create_sink(dsn)
        _SHARED_DSN = dsn
        logger.info("Metrics sink initialized")
    return _SHARED_SINK


async def get_global_client(dsn: str) -> Any:
    sink = await get_global_sink(dsn)
    if hasattr(sink, "client"):
        return sink.client
    return sink


@asynccontextmanager
async def clickhouse_client(dsn: str) -> AsyncIterator[BaseMetricsSink]:
    """Compatibility wrapper for clickhouse_client context manager."""
    sink = await get_global_sink(dsn)
    yield sink


async def close_global_client() -> None:
    global _SHARED_SINK, _SHARED_DSN
    if _SHARED_SINK:
        _SHARED_SINK.close()
    _SHARED_SINK = None
    _SHARED_DSN = None


def require_clickhouse_backend(sink: BaseMetricsSink) -> None:
    """Raise ValueError when the sink is not backed by ClickHouse.

    Call this at the top of any analytics service function that relies on
    ClickHouse-specific SQL (ARRAY JOIN, JSONExtract, argMax, etc.).
    """
    if sink.backend_type != "clickhouse":
        raise ValueError(
            "This analytics endpoint requires ClickHouse. "
            "Configure CLICKHOUSE_URI "
            "(e.g. clickhouse://user:pass@host:8123/db). "
            "See docs/architecture/database-architecture.md"
        )


async def query_dicts(
    sink: Any, query: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    if sink is None:
        raise RuntimeError("ClickHouse client is None")

    if not hasattr(sink, "query_dicts") and not hasattr(sink, "query"):
        raise RuntimeError(
            f"Invalid ClickHouse client: {type(sink).__name__} (no 'query' method)"
        )

    # Auto-inject org_id from request context (set by get_current_user).
    # This ensures every ClickHouse query is tenant-scoped without manual threading.
    _org_id = get_current_org_id()
    if _org_id is not None:
        params = dict(params) if params else {}
        params["org_id"] = _org_id

    safe_query = sanitize_for_log(query)
    safe_params = {
        sanitize_for_log(k): sanitize_for_log(v) for k, v in (params or {}).items()
    }
    logger.debug("Executing query: %s with params %s", safe_query, safe_params)

    if hasattr(sink, "query_dicts"):
        return sink.query_dicts(query, params)

    result = sink.query(query, parameters=params)

    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]
