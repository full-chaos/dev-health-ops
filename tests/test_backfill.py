from __future__ import annotations

from datetime import date

import pytest

from dev_health_ops.backfill.chunker import chunk_date_range
from dev_health_ops.backfill.runner import run_backfill_for_config
from dev_health_ops.cli import build_parser


def test_chunk_date_range_single_day() -> None:
    since = date(2026, 1, 10)
    before = date(2026, 1, 10)

    assert chunk_date_range(since=since, before=before, chunk_days=7) == [
        (since, before)
    ]


def test_chunk_date_range_exactly_seven_days() -> None:
    since = date(2026, 1, 1)
    before = date(2026, 1, 7)

    assert chunk_date_range(since=since, before=before, chunk_days=7) == [
        (since, before)
    ]


def test_chunk_date_range_ten_days_creates_two_chunks() -> None:
    assert chunk_date_range(
        since=date(2026, 1, 1),
        before=date(2026, 1, 10),
        chunk_days=7,
    ) == [
        (date(2026, 1, 1), date(2026, 1, 7)),
        (date(2026, 1, 8), date(2026, 1, 10)),
    ]


def test_chunk_date_range_empty_range_raises() -> None:
    with pytest.raises(ValueError, match="since must be before or equal to before"):
        chunk_date_range(
            since=date(2026, 1, 11),
            before=date(2026, 1, 10),
            chunk_days=7,
        )


def test_backfill_cli_run_parses_args() -> None:
    parser = build_parser()
    ns = parser.parse_args(
        [
            "--org",
            "11111111-1111-1111-1111-111111111111",
            "backfill",
            "run",
            "--config-id",
            "22222222-2222-2222-2222-222222222222",
            "--since",
            "2026-01-01",
            "--before",
            "2026-01-10",
            "--sink",
            "clickhouse",
        ]
    )

    assert ns.command == "backfill"
    assert ns.backfill_command == "run"
    assert ns.config_id == "22222222-2222-2222-2222-222222222222"
    assert ns.since == date(2026, 1, 1)
    assert ns.before == date(2026, 1, 10)
    assert ns.sink == "clickhouse"
    assert callable(ns.func)


def test_run_backfill_for_config_raises_when_config_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Query:
        def filter(self, *args, **kwargs):
            return self

        def one_or_none(self):
            return None

    class _Session:
        def query(self, *args, **kwargs):
            return _Query()

    class _Ctx:
        def __enter__(self):
            return _Session()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.get_postgres_session_sync",
        lambda: _Ctx(),
    )

    with pytest.raises(ValueError, match="Sync configuration not found"):
        run_backfill_for_config(
            db_url="clickhouse://local",
            sync_config_id="33333333-3333-3333-3333-333333333333",
            org_id="44444444-4444-4444-4444-444444444444",
            since=date(2026, 1, 1),
            before=date(2026, 1, 10),
        )
