from __future__ import annotations

import asyncio
import logging
import inspect
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List

import clickhouse_connect

logger = logging.getLogger(__name__)

_SHARED_CLIENT: Any = None
_SHARED_DSN: str | None = None
_CLIENT_LOCK = asyncio.Lock()


def _rows_to_dicts(result: Any) -> List[Dict[str, Any]]:
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


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
    if not hasattr(client, "query"):
        raise RuntimeError(
            f"Invalid ClickHouse client: {type(client).__name__} (no 'query' method)"
        )

    logger.debug("Executing query: %s with params %s", query, params)
    result = client.query(query, parameters=params)
    if inspect.isawaitable(result):
        result = await result

    return _rows_to_dicts(result)
