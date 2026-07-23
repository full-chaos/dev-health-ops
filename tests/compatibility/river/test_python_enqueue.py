from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy.pool import NullPool

from tests.compatibility.river import python_enqueue

FIXTURE_DIR = Path(__file__).with_name("fixtures")


def _args(**overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "max_attempts": 7,
        "priority": 2,
        "queue": "chaos3034",
        "scheduled_delay_ms": 0,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_compat_args_emit_the_versioned_cross_language_contract() -> None:
    payload = json.loads(python_enqueue.CompatArgs(marker="fixture-1").to_json())

    assert payload == {
        "contract_version": 1,
        "marker": "fixture-1",
        "source": "python",
    }


def test_go_fixture_matches_the_python_contract() -> None:
    payload = json.loads((FIXTURE_DIR / "go_job_v1.json").read_text())

    assert payload == {
        "contract_version": 1,
        "marker": "go-fixture",
        "source": "go",
    }
    assert set(payload) == {"contract_version", "marker", "source"}


def test_plain_postgresql_url_is_normalized_for_asyncpg() -> None:
    assert (
        python_enqueue._async_database_url("postgresql://u:p@db:5432/example")
        == "postgresql+asyncpg://u:p@db:5432/example"
    )


def test_non_postgresql_url_is_rejected() -> None:
    with pytest.raises(ValueError, match="must use postgresql"):
        python_enqueue._async_database_url("sqlite:///example.db")


def test_pgbouncer_engine_disables_client_pool_and_statement_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    sentinel = object()

    def fake_create_async_engine(url: str, **kwargs: object) -> object:
        captured["url"] = url
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(python_enqueue, "create_async_engine", fake_create_async_engine)

    engine = python_enqueue._engine(
        "postgresql://u:p@pooler:6432/example", pgbouncer=True
    )

    assert engine is sentinel
    assert captured["url"] == "postgresql+asyncpg://u:p@pooler:6432/example"
    assert captured["poolclass"] is NullPool
    connect_args = captured["connect_args"]
    assert isinstance(connect_args, dict)
    assert connect_args["statement_cache_size"] == 0
    assert connect_args["prepared_statement_cache_size"] == 0
    name_func = connect_args["prepared_statement_name_func"]
    assert callable(name_func)
    assert name_func() != name_func()


def test_insert_options_preserve_required_queue_policy() -> None:
    options = python_enqueue._insert_opts(_args())

    assert options.queue == "chaos3034"
    assert options.priority == 2
    assert options.max_attempts == 7
    assert options.scheduled_at is None
    assert options.tags == ["phase0", "python"]


def test_scheduled_insert_option_is_in_the_future() -> None:
    before = datetime.now(timezone.utc)
    options = python_enqueue._insert_opts(_args(scheduled_delay_ms=60_000))

    assert options.scheduled_at is not None
    assert options.scheduled_at.tzinfo is not None
    assert options.scheduled_at > before
