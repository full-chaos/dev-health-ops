"""
Shared insert helpers for the ClickHouse sink package.

This is the ONE place for:
  - DEFAULT_BATCH_SIZE constant
  - _chunked() iterator helper
  - _dt_to_clickhouse_datetime() coercion helper
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Iterator, Sequence
from datetime import datetime, timezone
from typing import Any, TypeVar

DEFAULT_BATCH_SIZE = 10000

T = TypeVar("T")


class _ClickHouseSinkBase(ABC):
    client: Any
    org_id: str

    def _insert_rows(
        self,
        table: str,
        columns: list[str],
        rows: Any,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        raise NotImplementedError


def _chunked(seq: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _dt_to_clickhouse_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)
