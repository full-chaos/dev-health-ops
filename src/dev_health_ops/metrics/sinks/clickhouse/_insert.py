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
    """Normalize to a timezone-AWARE UTC datetime for clickhouse-connect.

    PR #1602 round-2 review NEW-2 (HIGH, BLOCKS): this used to convert to
    UTC and then STRIP tzinfo (``.replace(tzinfo=None)``) before returning.
    clickhouse-connect reinterprets a NAIVE datetime using the WRITER
    PROCESS's own local system timezone and re-converts it to UTC for the
    wire -- a second, spurious conversion stacked on top of the first one
    this function already did. Proven with a 3-TZ matrix (UTC,
    America/Los_Angeles, Asia/Tokyo): the same instant round-tripped through
    a naive value landed 0h/+8h/-9h off depending solely on the writer
    process's `TZ`. Returning an AWARE UTC datetime instead is not
    reinterpreted -- clickhouse-connect respects `tzinfo` directly and
    stores the correct instant regardless of the writer's local timezone
    (verified empirically against the real engine).

    The naive-input branch is fixed the same way: every caller in this
    codebase treats a naive datetime as ALREADY UTC (the CHAOS-3392
    no-wall-clock convention; `datetime.now(timezone.utc)` is used
    throughout), so a naive value reaching this function is stamped with
    UTC explicitly rather than left naive and vulnerable to the identical
    reinterpretation bug.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
