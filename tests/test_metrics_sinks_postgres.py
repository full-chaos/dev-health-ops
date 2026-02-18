from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

import pytest

from dev_health_ops.metrics.sinks.postgres import (
    PostgresMetricsSink,
    _serialize_value,
)


def test_serialize_value_handles_common_types():
    aware = datetime(2026, 2, 18, 12, 0, tzinfo=timezone.utc)
    naive = datetime(2026, 2, 18, 12, 0)

    assert _serialize_value(None) == "\\N"
    assert _serialize_value(True) == "t"
    assert _serialize_value(False) == "f"
    assert _serialize_value(date(2026, 2, 18)) == "2026-02-18"
    assert _serialize_value(aware) == "2026-02-18T12:00:00"
    assert _serialize_value(naive) == "2026-02-18T12:00:00"
    assert _serialize_value({"a": 1}) == '{"a": 1}'


def test_table_has_column_true_false_and_inspect_error(monkeypatch):
    class _Inspector:
        def __init__(self, columns):
            self._columns = columns

        def get_columns(self, _table):
            return self._columns

    monkeypatch.setattr(
        "dev_health_ops.metrics.sinks.postgres.inspect",
        lambda _conn: _Inspector([{"name": "col_a"}, {"name": "col_b"}]),
    )
    assert PostgresMetricsSink._table_has_column(object(), "table", "col_a") is True
    assert PostgresMetricsSink._table_has_column(object(), "table", "missing") is False

    def _raise(_conn):
        raise RuntimeError("boom")

    monkeypatch.setattr("dev_health_ops.metrics.sinks.postgres.inspect", _raise)
    assert PostgresMetricsSink._table_has_column(object(), "table", "col_a") is False


def test_copy_upsert_no_rows_returns_without_engine_access():
    sink = object.__new__(PostgresMetricsSink)

    class _Engine:
        def connect(self):
            raise AssertionError("engine.connect should not be called for empty rows")

    sink.engine = _Engine()

    sink._copy_upsert("t", ["id"], ["id"], [])


@dataclass
class _Row:
    id: int
    name: str
    active: bool
    payload: dict


class _Cursor:
    def __init__(self):
        self.executed = []
        self.copy_calls = []
        self.closed = False

    def execute(self, sql):
        self.executed.append(sql)

    def copy_from(self, file_obj, table, sep, null, columns):
        self.copy_calls.append(
            {
                "table": table,
                "sep": sep,
                "null": null,
                "columns": list(columns),
                "data": file_obj.getvalue(),
            }
        )

    def close(self):
        self.closed = True


class _RawConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class _ConnWrapper:
    def __init__(self, raw_conn):
        self.connection = type("_Inner", (), {"dbapi_connection": raw_conn})()


class _EngineContext:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc, tb):
        return False


class _Engine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return _EngineContext(self._conn)


def test_copy_upsert_writes_copy_and_upsert_sql():
    cursor = _Cursor()
    raw_conn = _RawConn(cursor)
    sink = object.__new__(PostgresMetricsSink)
    sink.engine = _Engine(_ConnWrapper(raw_conn))

    rows = [_Row(id=1, name="repo", active=True, payload={"k": "v"})]
    sink._copy_upsert(
        table="repo_metrics_daily",
        columns=["id", "name", "active", "payload"],
        primary_keys=["id"],
        rows=rows,
    )

    assert raw_conn.committed is True
    assert raw_conn.rolled_back is False
    assert cursor.closed is True
    assert cursor.copy_calls
    assert cursor.copy_calls[0]["table"] == "_tmp_repo_metrics_daily"
    assert cursor.copy_calls[0]["columns"] == ["id", "name", "active", "payload"]
    assert "repo" in cursor.copy_calls[0]["data"]
    assert "INSERT INTO repo_metrics_daily" in cursor.executed[-1]


def test_copy_upsert_rolls_back_and_raises_on_copy_error():
    class _FailingCursor(_Cursor):
        def copy_from(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise RuntimeError("copy failed")

    cursor = _FailingCursor()
    raw_conn = _RawConn(cursor)
    sink = object.__new__(PostgresMetricsSink)
    sink.engine = _Engine(_ConnWrapper(raw_conn))

    with pytest.raises(RuntimeError, match="copy failed"):
        sink._copy_upsert(
            table="repo_metrics_daily",
            columns=["id", "name", "active", "payload"],
            primary_keys=["id"],
            rows=[_Row(id=1, name="repo", active=True, payload={"k": "v"})],
        )

    assert raw_conn.rolled_back is True
    assert cursor.closed is True
