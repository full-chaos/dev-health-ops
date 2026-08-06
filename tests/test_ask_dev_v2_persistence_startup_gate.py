"""Startup/health schema-revision gate proofs (CHAOS-3299).

Before this ticket, nothing checked the application schema revision at API
process startup or in ``/health`` -- a missing migration surfaced as a raw
``UndefinedTable``/``ProgrammingError`` on the first request that touched an
unmigrated table. This is a live-PostgreSQL-gated test (schema revision
tracking has no meaning against SQLite) proving the gate fires correctly at
each stage of the real alembic chain, and that ``api._lifespan.lifespan``
itself raises/aborts against an unmigrated database and starts cleanly
against a migrated one -- the acceptance clause is "controlled
startup/readiness failure", so the test must exercise ``lifespan()``, not
just the underlying primitive.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic import command
from sqlalchemy.engine import make_url

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"

pytestmark = pytest.mark.skipif(
    not os.getenv(_POSTGRES_URI_ENV), reason=f"requires {_POSTGRES_URI_ENV}"
)


@pytest.fixture
def scratch_database():
    """A throwaway PostgreSQL database (own alembic_version), auto-dropped."""

    configured = make_url(os.environ[_POSTGRES_URI_ENV])
    if configured.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")
    admin_url = configured.set(drivername="postgresql+psycopg2")
    dbname = f"ask_dev_v2_gate_{uuid.uuid4().hex[:16]}"

    admin_engine = sa.create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as connection:
        connection.execute(sa.text(f'CREATE DATABASE "{dbname}"'))
    host = configured.host or "localhost"
    port = configured.port or 5432
    user = configured.username or ""
    password = configured.password or ""
    try:
        sync_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        async_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{dbname}"
        yield sync_url, async_url
    finally:
        with admin_engine.connect() as connection:
            connection.execute(
                sa.text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    f"WHERE datname = '{dbname}'"
                )
            )
            connection.execute(sa.text(f'DROP DATABASE IF EXISTS "{dbname}"'))
        admin_engine.dispose()


def _upgrade_to(sync_url: str, revision: str) -> None:
    from dev_health_ops.migrate import _make_alembic_config

    # env.py always builds an async engine regardless of the configured URL
    # scheme, so the config URL must itself be async-capable. env.py also
    # re-resolves get_postgres_uri() at import time and overwrites whatever
    # _make_alembic_config already set, so POSTGRES_URI must be set too --
    # the autouse conftest fixture otherwise forces DATABASE_URI to an
    # in-memory sqlite for every test.
    async_url = sync_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    cfg = _make_alembic_config(async_url)
    previous = os.environ.get("POSTGRES_URI")
    os.environ["POSTGRES_URI"] = async_url
    try:
        command.upgrade(cfg, revision)
    finally:
        if previous is None:
            os.environ.pop("POSTGRES_URI", None)
        else:
            os.environ["POSTGRES_URI"] = previous


@pytest.mark.asyncio
async def test_application_schema_status_ancestor_walk(scratch_database) -> None:
    from dev_health_ops.migrate import application_schema_status

    sync_url, async_url = scratch_database

    satisfied, heads = await application_schema_status(async_url)
    assert satisfied is False
    assert heads == ()

    await asyncio.to_thread(_upgrade_to, sync_url, "0071")
    satisfied, heads = await application_schema_status(async_url)
    assert satisfied is False, "0071 predates the CHAOS-3299 revisions"

    await asyncio.to_thread(_upgrade_to, sync_url, "0074")
    satisfied, heads = await application_schema_status(async_url)
    assert satisfied is False, "0075 (VALIDATE CONSTRAINT) is still missing"

    await asyncio.to_thread(_upgrade_to, sync_url, "application_schema@head")
    satisfied, heads = await application_schema_status(async_url)
    assert satisfied is True
    assert heads == ("0087",)


@pytest.mark.asyncio
async def test_lifespan_aborts_startup_when_schema_revision_missing(
    scratch_database,
) -> None:
    """The acceptance clause verbatim: a controlled startup failure, not a
    runtime ``UndefinedTable``. Exercises the real ``lifespan()`` context
    manager against a database pinned at 0071 (pre-CHAOS-3299)."""

    from dev_health_ops.api._lifespan import lifespan

    sync_url, async_url = scratch_database
    await asyncio.to_thread(_upgrade_to, sync_url, "0071")

    with (
        patch.dict(os.environ, {"POSTGRES_URI": async_url}),
        patch(
            "dev_health_ops.api.middleware.rate_limit.verify_rate_limit_config",
            return_value=None,
        ),
    ):
        with pytest.raises(RuntimeError, match="application schema"):
            async with lifespan(_fake_app()):
                pytest.fail("lifespan must abort before yielding")


@pytest.mark.asyncio
async def test_lifespan_starts_cleanly_at_required_revision(
    scratch_database,
) -> None:
    from dev_health_ops.api._lifespan import lifespan

    sync_url, async_url = scratch_database
    await asyncio.to_thread(_upgrade_to, sync_url, "application_schema@head")

    with (
        patch.dict(os.environ, {"POSTGRES_URI": async_url}),
        patch(
            "dev_health_ops.api.middleware.rate_limit.verify_rate_limit_config",
            return_value=None,
        ),
    ):
        async with lifespan(_fake_app()):
            pass  # started cleanly


@pytest.mark.asyncio
async def test_health_postgres_check_reports_down_below_required_revision(
    scratch_database,
) -> None:
    """`/health` re-verifies on every probe -- covers the rolling-deploy
    window a one-time startup check cannot (a replica that booted before a
    migration completed elsewhere, or a manual DB rollback without a
    process restart)."""

    from dev_health_ops.api._health import _check_postgres_health

    sync_url, async_url = scratch_database
    await asyncio.to_thread(_upgrade_to, sync_url, "0074")  # 0075 still missing

    with patch.dict(os.environ, {"POSTGRES_URI": async_url}):
        name, status_value = await _check_postgres_health()
    assert name == "postgres"
    assert status_value == "down"

    await asyncio.to_thread(_upgrade_to, sync_url, "application_schema@head")
    with patch.dict(os.environ, {"POSTGRES_URI": async_url}):
        name, status_value = await _check_postgres_health()
    assert status_value == "ok"


def _fake_app():
    class _FakeApp:
        pass

    return _FakeApp()
