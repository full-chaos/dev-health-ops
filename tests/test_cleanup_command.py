from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import dev_health_ops.cli as cli_module


def test_cleanup_tokens_command_calls_cleanup_expired(monkeypatch):
    called = {"count": 0, "committed": False}

    class _FakeDB:
        async def commit(self) -> None:
            called["committed"] = True

    @asynccontextmanager
    async def _fake_get_postgres_session():
        yield _FakeDB()

    async def _fake_cleanup_expired(db):
        called["count"] += 1
        assert isinstance(db, _FakeDB)
        return 3

    monkeypatch.setattr(cli_module, "get_postgres_session", _fake_get_postgres_session)
    monkeypatch.setattr(cli_module, "cleanup_expired", _fake_cleanup_expired)

    parser = cli_module.build_parser()
    ns = parser.parse_args(["maintenance", "cleanup-tokens"])
    rc = asyncio.run(ns.func(ns))

    assert rc == 0
    assert called["count"] == 1
    assert called["committed"] is True


def test_backfill_ask_dev_ephemeral_expiry_command_drains_in_committed_batches(
    monkeypatch,
):
    """CHAOS-3404: the CLI command must loop DevPersistenceService.
    backfill_stranded_ephemeral_expiry in committed batches until a batch
    comes back short of the limit, matching cleanup_expired's own
    drain-loop shape, and must actually commit each batch (a backfill that
    never commits stamps nothing durably)."""
    calls = {"count": 0, "commits": 0}
    # First batch full (limit), second batch short -> loop stops after 2.
    responses = [500, 3]

    class _FakeService:
        def __init__(self, db):
            self.db = db

        async def backfill_stranded_ephemeral_expiry(self, *, limit):
            assert limit == 500
            calls["count"] += 1
            return responses[calls["count"] - 1]

    class _FakeDB:
        async def commit(self) -> None:
            calls["commits"] += 1

    @asynccontextmanager
    async def _fake_get_postgres_session():
        yield _FakeDB()

    monkeypatch.setattr(cli_module, "get_postgres_session", _fake_get_postgres_session)
    monkeypatch.setattr(
        "dev_health_ops.api.dev.persistence.DevPersistenceService", _FakeService
    )

    parser = cli_module.build_parser()
    ns = parser.parse_args(["maintenance", "backfill-ask-dev-ephemeral-expiry"])
    rc = asyncio.run(ns.func(ns))

    assert rc == 0
    assert calls["count"] == 2
    assert calls["commits"] == 2


def test_cleanup_all_command_calls_cleanup_expired(monkeypatch):
    called = {"count": 0, "committed": False}

    class _FakeDB:
        async def commit(self) -> None:
            called["committed"] = True

    @asynccontextmanager
    async def _fake_get_postgres_session():
        yield _FakeDB()

    async def _fake_cleanup_expired(db):
        called["count"] += 1
        assert isinstance(db, _FakeDB)
        return 5

    monkeypatch.setattr(cli_module, "get_postgres_session", _fake_get_postgres_session)
    monkeypatch.setattr(cli_module, "cleanup_expired", _fake_cleanup_expired)

    parser = cli_module.build_parser()
    ns = parser.parse_args(["maintenance", "cleanup-all"])
    rc = asyncio.run(ns.func(ns))

    assert rc == 0
    assert called["count"] == 1
    assert called["committed"] is True
