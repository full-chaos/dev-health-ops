from __future__ import annotations

import logging
import inspect
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List

import clickhouse_connect

logger = logging.getLogger(__name__)

_SHARED_CLIENT: Any = None
_SHARED_DSN: str | None = None


def _rows_to_dicts(result: Any) -> List[Dict[str, Any]]:
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


def _sanitize_for_log(value: Any, max_length: int = 1000) -> Any:
    """
    Sanitize a value for safe logging by removing CR/LF characters and
    truncating long strings. Applies recursively to common container types.

    - Strings: newline and carriage-return characters are replaced with
      spaces, and strings longer than ``max_length`` are truncated with a
      ``"...[truncated]"`` suffix.
    - Dicts, lists, and tuples: values/elements are sanitized recursively.
    - Other types are converted to strings and sanitized in the same way
      as regular strings.
    """
    if isinstance(value, str):
        cleaned = value.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length] + "...[truncated]"
        return cleaned
    if isinstance(value, dict):
        return {
            k: _sanitize_for_log(v, max_length=max_length) for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
    # For non-string scalars, log a sanitized string representation
    return _sanitize_for_log(str(value), max_length=max_length)
        return type(value)(sanitized_seq)
    return value


async def get_global_client(dsn: str) -> Any:
    """Get the shared ClickHouse client, initializing if needed."""
    global _SHARED_CLIENT, _SHARED_DSN

    if _SHARED_CLIENT and dsn != _SHARED_DSN:
        logger.info("Closing ClickHouse client due to DSN change")
        # Attempt close if it has it
        if hasattr(_SHARED_CLIENT, "close") and inspect.iscoroutinefunction(
            _SHARED_CLIENT.close
        ):
            await _SHARED_CLIENT.close()
        _SHARED_CLIENT = None

    if _SHARED_CLIENT is None:
        logger.info("Initializing global ClickHouse client for %s", dsn)
        if hasattr(clickhouse_connect, "get_async_client"):
            _SHARED_CLIENT = await clickhouse_connect.get_async_client(dsn=dsn)
        else:
            _SHARED_CLIENT = clickhouse_connect.get_client(dsn=dsn)
        _SHARED_DSN = dsn
        logger.info("ClickHouse client initialized")
    return _SHARED_CLIENT


@asynccontextmanager
async def clickhouse_client(dsn: str) -> AsyncIterator[Any]:
    global _SHARED_CLIENT, _SHARED_DSN

    if _SHARED_CLIENT and dsn != _SHARED_DSN:
        await close_global_client()

    if _SHARED_CLIENT is None:
        if hasattr(clickhouse_connect, "get_async_client"):
            _SHARED_CLIENT = await clickhouse_connect.get_async_client(dsn=dsn)
        else:
            _SHARED_CLIENT = clickhouse_connect.get_client(dsn=dsn)
        _SHARED_DSN = dsn

    yield _SHARED_CLIENT


async def close_global_client() -> None:
    global _SHARED_CLIENT, _SHARED_DSN
    if _SHARED_CLIENT:
        close = getattr(_SHARED_CLIENT, "close", None)
        if close is not None:
            if inspect.iscoroutinefunction(close):
                await close()
            else:
                close()
    _SHARED_CLIENT = None
    _SHARED_DSN = None


async def query_dicts(
    client: Any, query: str, params: Dict[str, Any]
) -> List[Dict[str, Any]]:
    if client is None:
        raise RuntimeError("ClickHouse client is None")
    safe_query = _sanitize_for_log(query)
    safe_params = {
        _sanitize_for_log(k): _sanitize_for_log(v)
        for k, v in (params or {}).items()
    }
    logger.debug("Executing query: %s with params %s", safe_query, safe_params)
        _sanitize_for_log(safe_params),
        "Executing query: %s with params %s",
        _sanitize_for_log(params),
        safe_params,
    )
    result = client.query(query, parameters=params)
    if inspect.isawaitable(result):
        result = await result

    return _rows_to_dicts(result)
