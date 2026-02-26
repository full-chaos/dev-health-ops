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
