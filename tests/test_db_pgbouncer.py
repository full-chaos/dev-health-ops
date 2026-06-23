"""Engine configuration for PgBouncer transaction-mode pooling (CHAOS-2065).

These guard the contract that, behind PgBouncer in transaction mode, the async
Postgres engine uses NullPool and disables asyncpg prepared-statement caching /
naming -- otherwise pooled server connections raise
``prepared statement "__asyncpg_stmt_*" does not exist``.
"""

from __future__ import annotations

import logging

import pytest
from sqlalchemy.pool import NullPool

from dev_health_ops import db


@pytest.fixture(autouse=True)
def _clear_pool_env(monkeypatch):
    for var in (
        "PGBOUNCER_TRANSACTION_MODE",
        "POSTGRES_CONNECT_TIMEOUT_SECONDS",
        "POSTGRES_DISABLE_POOLER_AUTODETECT",
        "POSTGRES_POOL_SIZE",
        "POSTGRES_MAX_OVERFLOW",
    ):
        monkeypatch.delenv(var, raising=False)


class TestPgbouncerTransactionModeFlag:
    def test_disabled_by_default(self):
        assert db._pgbouncer_transaction_mode() is False

    @pytest.mark.parametrize("val", ["1", "true", "TRUE", "Yes", "on"])
    def test_truthy_values_enable(self, monkeypatch, val):
        monkeypatch.setenv("PGBOUNCER_TRANSACTION_MODE", val)
        assert db._pgbouncer_transaction_mode() is True

    @pytest.mark.parametrize("val", ["0", "false", "no", "", "off", "  "])
    def test_falsy_values_disable(self, monkeypatch, val):
        monkeypatch.setenv("PGBOUNCER_TRANSACTION_MODE", val)
        assert db._pgbouncer_transaction_mode() is False

    def test_neon_pooler_host_enables_transaction_pooler_mode(self):
        uri = "postgresql+asyncpg://u:p@ep-cool-boat-af967qaf-pooler.c-2.us-west-2.aws.neon.tech/d"

        assert db._transaction_pooler_mode(uri) is True

    def test_neon_pooler_autodetect_can_be_disabled(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_DISABLE_POOLER_AUTODETECT", "true")
        uri = "postgresql+asyncpg://u:p@ep-cool-boat-af967qaf-pooler.c-2.us-west-2.aws.neon.tech/d"

        assert db._transaction_pooler_mode(uri) is False


class TestAsyncEngineKwargs:
    def test_direct_postgres_uses_queue_pool(self):
        kw = db._async_postgres_engine_kwargs("postgresql+asyncpg://u:p@h:5432/d")
        assert kw == {
            "pool_pre_ping": True,
            "pool_size": 20,
            "max_overflow": 10,
            "connect_args": {"timeout": 10.0},
        }
        assert "poolclass" not in kw

    def test_pgbouncer_uses_nullpool_and_disables_prepared_statements(
        self, monkeypatch
    ):
        monkeypatch.setenv("PGBOUNCER_TRANSACTION_MODE", "true")
        kw = db._async_postgres_engine_kwargs("postgresql+asyncpg://u:p@h:6432/d")

        assert kw["poolclass"] is NullPool
        # PgBouncer owns the pool -- SQLAlchemy must not also size one.
        assert "pool_size" not in kw and "max_overflow" not in kw

        connect_args = kw["connect_args"]
        assert connect_args["timeout"] == 10.0
        assert connect_args["statement_cache_size"] == 0
        name_func = connect_args["prepared_statement_name_func"]
        # Unique per call so names never collide across multiplexed server conns.
        assert name_func() != name_func()
        assert name_func().startswith("__asyncpg_")

    def test_pgbouncer_flag_ignored_for_non_postgres(self, monkeypatch):
        monkeypatch.setenv("PGBOUNCER_TRANSACTION_MODE", "true")
        kw = db._async_postgres_engine_kwargs("sqlite+aiosqlite:///:memory:")
        assert kw == {"pool_pre_ping": True}
        assert "poolclass" not in kw

    def test_pool_size_env_overridable(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_POOL_SIZE", "50")
        monkeypatch.setenv("POSTGRES_MAX_OVERFLOW", "25")
        kw = db._async_postgres_engine_kwargs("postgresql+asyncpg://u:p@h/d")
        assert kw["pool_size"] == 50
        assert kw["max_overflow"] == 25

    def test_connect_timeout_env_overridable(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT_SECONDS", "15")

        kw = db._async_postgres_engine_kwargs("postgresql+asyncpg://u:p@h/d")

        assert kw["connect_args"]["timeout"] == 15.0

    def test_invalid_connect_timeout_falls_back(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT_SECONDS", "nope")

        kw = db._async_postgres_engine_kwargs("postgresql+asyncpg://u:p@h/d")

        assert kw["connect_args"]["timeout"] == 10.0

    def test_neon_pooler_autodetect_uses_nullpool(self):
        uri = "postgresql+asyncpg://u:p@ep-cool-boat-af967qaf-pooler.c-2.us-west-2.aws.neon.tech/d"

        kw = db._async_postgres_engine_kwargs(uri)

        assert kw["poolclass"] is NullPool
        assert kw["connect_args"]["statement_cache_size"] == 0

    def test_neon_pooler_autodetect_uses_nullpool_for_sync_engine(self, monkeypatch):
        captured: dict[str, object] = {}

        def create_engine(dsn: str, **kwargs):
            captured["dsn"] = dsn
            captured.update(kwargs)
            return object()

        monkeypatch.setattr(db, "create_engine", create_engine)

        db.get_postgres_sync_engine(
            "postgresql+asyncpg://u:p@ep-cool-boat-af967qaf-pooler.c-2.us-west-2.aws.neon.tech/d?ssl=require"
        )

        assert captured["poolclass"] is NullPool

    def test_connection_posture_log_is_sanitized(self, caplog):
        uri = "postgresql+asyncpg://u:secret@ep-cool-boat-af967qaf-pooler.c-2.us-west-2.aws.neon.tech/d"
        kw = db._async_postgres_engine_kwargs(uri)

        with caplog.at_level(logging.INFO, logger="dev_health_ops.db"):
            db._log_postgres_connection_posture(uri, kw)

        record = next(
            r for r in caplog.records if r.message == "postgres_connection_posture"
        )
        assert record.driver == "postgresql+asyncpg"
        assert record.host == "ep-cool-boat-af967qaf-pooler.c-2.us-west-2.aws.neon.tech"
        assert record.database == "d"
        assert record.transaction_pooler_mode is True
        assert record.null_pool is True
        assert record.connect_timeout_seconds == 10.0
        assert "secret" not in caplog.text


class TestAsyncPostgresUrlNormalization:
    def test_converts_plain_postgres_to_asyncpg(self):
        uri = "postgresql://u:p@h/d"

        assert db._ensure_async_postgres(uri) == "postgresql+asyncpg://u:p@h/d"

    def test_translates_neon_sslmode_for_asyncpg(self):
        uri = "postgresql://u:p@h/d?sslmode=require&channel_binding=require"

        assert (
            db._ensure_async_postgres(uri) == "postgresql+asyncpg://u:p@h/d?ssl=require"
        )

    def test_preserves_existing_asyncpg_ssl_query(self):
        uri = "postgresql+asyncpg://u:p@h/d?ssl=true&sslmode=require"

        assert db._ensure_async_postgres(uri) == "postgresql+asyncpg://u:p@h/d?ssl=true"

    def test_strips_channel_binding_without_injecting_ssl(self):
        uri = "postgresql://u:p@h/d?channel_binding=require"

        assert db._ensure_async_postgres(uri) == "postgresql+asyncpg://u:p@h/d"

    def test_preserves_asyncpg_uri_without_query(self):
        uri = "postgresql+asyncpg://u:p@h/d"

        assert db._ensure_async_postgres(uri) == uri

    def test_preserves_asyncpg_ssl_query_without_sslmode(self):
        uri = "postgresql+asyncpg://u:p@h/d?ssl=require"

        assert db._ensure_async_postgres(uri) == uri

    def test_existing_ssl_query_takes_precedence_over_sslmode(self):
        uri = "postgresql+asyncpg://u:p@h/d?ssl=true&sslmode=verify-full"

        assert db._ensure_async_postgres(uri) == "postgresql+asyncpg://u:p@h/d?ssl=true"

    def test_preserves_asyncpg_supported_sslmode_values(self):
        uri = "postgresql+asyncpg://u:p@h/d?sslmode=prefer"

        assert (
            db._ensure_async_postgres(uri) == "postgresql+asyncpg://u:p@h/d?ssl=prefer"
        )

    def test_preserves_non_postgres_uri(self):
        uri = "clickhouse://u:p@h/d?sslmode=require"

        assert db._ensure_async_postgres(uri) == uri


class TestPoolSizeParsing:
    def test_defaults(self):
        assert db._pg_pool_size() == (20, 10)

    def test_invalid_values_fall_back_to_defaults(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_POOL_SIZE", "not-an-int")
        monkeypatch.setenv("POSTGRES_MAX_OVERFLOW", "")
        assert db._pg_pool_size() == (20, 10)
