from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List

from dev_health_ops.metrics.sinks.factory import create_sink
from dev_health_ops.metrics.sinks.base import BaseMetricsSink

logger = logging.getLogger(__name__)

_SHARED_SINK: BaseMetricsSink | None = None
_SHARED_DSN: str | None = None


def _sanitize_for_log(value: Any, max_length: int = 1000) -> Any:
    """
    Sanitize a value for safe logging by removing CR/LF characters and
    truncating long strings. Applies recursively to common container types.

    - Strings: newline and carriage-return characters are replaced with
      spaces, and strings longer than ``max_length`` are truncated with a
      ``"...[truncated]"`` suffix.
    - Dicts: keys and values are sanitized recursively.
    - Lists, tuples, and sets: elements are sanitized recursively and
      returned as a list.
    - Other types are converted to strings and sanitized in the same way
      as regular strings.
    """

    def _clean_scalar(text: str) -> str:
        cleaned = text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length] + "...[truncated]"
        return cleaned

    if isinstance(value, str):
        return _clean_scalar(value)

    if isinstance(value, dict):
        sanitized_dict: Dict[Any, Any] = {}
        for k, v in value.items():
            # Sanitize keys via their string representation to avoid
            # introducing control characters into log output.
            sanitized_key = _clean_scalar(str(k))
            sanitized_dict[sanitized_key] = _sanitize_for_log(v, max_length=max_length)
        return sanitized_dict

    if isinstance(value, (list, tuple, set)):
        # Preserve order where applicable but always sanitize elements.
        return [_sanitize_for_log(elem, max_length=max_length) for elem in value]

    # Fallback for scalars and other objects: log a sanitized string
    # representation to ensure no control characters appear in logs.
    return _clean_scalar(str(value))


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
        return getattr(sink, "client")
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


async def query_dicts(
    sink: Any, query: str, params: Dict[str, Any]
) -> List[Dict[str, Any]]:
    if sink is None:
        raise RuntimeError("ClickHouse client is None")

    if not hasattr(sink, "query_dicts") and not hasattr(sink, "query"):
        raise RuntimeError(
            f"Invalid ClickHouse client: {type(sink).__name__} (no 'query' method)"
        )

    safe_query = _sanitize_for_log(query)
    safe_params = {
        _sanitize_for_log(k): _sanitize_for_log(v) for k, v in (params or {}).items()
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
