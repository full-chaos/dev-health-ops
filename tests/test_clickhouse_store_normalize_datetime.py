"""Unit coverage for `ClickHouseStore._normalize_datetime` directly.

PR #1602 round-2 review NEW-2 fixed the identical timezone-corruption bug
in TWO places: `metrics/sinks/clickhouse/_insert.py`'s
`_dt_to_clickhouse_datetime` (covered by a live 3-TZ round-trip proof
against the real engine, `tests/api/dev/test_project_declared_state_
history_clickhouse_live.py::test_new2_write_read_round_trip_is_
independent_of_writer_process_tz`) and this method, used by ~90 call sites
across `storage/clickhouse.py` -- but this second fix was only ever proven
INDIRECTLY, through F2/C4 writer tests that happen to exercise it as a side
effect. This file names and tests it directly.
"""

from __future__ import annotations

from datetime import UTC, datetime, timezone

from dev_health_ops.storage.clickhouse import ClickHouseStore


def test_normalize_datetime_returns_none_for_none() -> None:
    assert ClickHouseStore._normalize_datetime(None) is None


def test_normalize_datetime_passes_through_non_datetime_values() -> None:
    assert ClickHouseStore._normalize_datetime("not-a-datetime") == "not-a-datetime"
    assert ClickHouseStore._normalize_datetime(42) == 42


def test_normalize_datetime_stamps_a_naive_value_as_utc() -> None:
    """PR #1602 review NEW-2: a naive datetime reaching this function is
    treated as ALREADY UTC (this codebase's convention) -- it must come
    back timezone-AWARE, never left naive. A naive value handed directly
    to clickhouse-connect is reinterpreted using the WRITER PROCESS's
    local system timezone, corrupting the stored instant.
    """
    naive = datetime(2026, 1, 1, 12, 0, 0)
    result = ClickHouseStore._normalize_datetime(naive)
    assert result.tzinfo is not None
    assert result == naive.replace(tzinfo=UTC)


def test_normalize_datetime_converts_an_aware_value_to_utc() -> None:
    """An aware, non-UTC datetime must be converted to UTC and stay
    AWARE -- never stripped back to naive (the original NEW-2 bug: convert
    to UTC then `.replace(tzinfo=None)`, silently re-corruptible by
    clickhouse-connect's own naive-datetime handling).
    """
    from datetime import timedelta

    pdt = timezone(timedelta(hours=-7))
    aware = datetime(2026, 1, 1, 5, 0, 0, tzinfo=pdt)  # 12:00 UTC
    result = ClickHouseStore._normalize_datetime(aware)
    assert result.tzinfo is not None
    assert result == datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def test_normalize_datetime_is_a_noop_for_an_already_utc_value() -> None:
    already_utc = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    result = ClickHouseStore._normalize_datetime(already_utc)
    assert result == already_utc
    assert result.tzinfo is not None
