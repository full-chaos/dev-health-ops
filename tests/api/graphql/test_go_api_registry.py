"""Unit/integration tests for the operation rollout registry access layer
(CHAOS-4366 Wave 0, ``api/graphql/go_api_registry.py``).

Requires real Postgres (``pg_insert(...).on_conflict_do_nothing()`` is a
Postgres-dialect construct, not portable to the aiosqlite fixture other API
tests use) -- same skip/fail-in-CI convention as the alembic 0114 migration
tests in ``tests/test_go_api_operation_registry_postgres_migration.py``.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.graphql.go_api_registry import (
    lookup_routing_state,
    record_proof_run,
    register_candidate_build,
)
from dev_health_ops.models.go_api_registry import RoutingState

pytestmark = pytest.mark.asyncio

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = "src/dev_health_ops/alembic"


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", _ALEMBIC_DIR)
    return config


@pytest.fixture
def migrated_scratch_db(monkeypatch: pytest.MonkeyPatch) -> Iterator[URL]:
    """Create+migrate a scratch Postgres DB, sync (not async).

    Kept synchronous and separate from the ``session`` fixture below: alembic's
    ``env.py`` calls ``asyncio.run(...)`` internally, which raises
    ``RuntimeError: asyncio.run() cannot be called from a running event loop``
    if invoked from inside an already-async pytest fixture.
    """
    configured_uri = os.environ.get(_POSTGRES_URI_ENV)
    if configured_uri is None:
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            pytest.fail(f"{_POSTGRES_URI_ENV} must be configured for this test")
        pytest.skip(f"requires {_POSTGRES_URI_ENV}")

    configured_url = make_url(configured_uri)
    if configured_url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")

    database_name = f"test_chaos_4366_registry_{uuid.uuid4().hex}"
    admin_engine = sa.create_engine(
        configured_url.set(drivername="postgresql+psycopg2", database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')

        async_url = configured_url.set(
            drivername="postgresql+asyncpg", database=database_name
        )
        monkeypatch.setenv(
            "POSTGRES_URI", async_url.render_as_string(hide_password=False)
        )
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        command.upgrade(_migration_config(), "application_schema@head")

        yield async_url
    finally:
        with admin_engine.connect() as connection:
            connection.execute(
                sa.text(
                    """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = :database_name AND pid <> pg_backend_pid()
                    """
                ),
                {"database_name": database_name},
            )
            connection.exec_driver_sql(f'DROP DATABASE "{database_name}"')
        admin_engine.dispose()


@pytest_asyncio.fixture
async def session(migrated_scratch_db: URL) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(migrated_scratch_db)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as db_session:
        yield db_session
    await engine.dispose()


async def test_lookup_routing_state_returns_none_when_unregistered(
    session: AsyncSession,
) -> None:
    result = await lookup_routing_state(
        session,
        schema_digest="s1",
        document_digest="d1",
        selected_operation="featureFlags",
    )
    assert result is None


async def test_register_candidate_build_is_idempotent(session: AsyncSession) -> None:
    await register_candidate_build(
        session,
        schema_digest="s1",
        document_digest="d1",
        selected_operation="featureFlags",
        candidate_build="build-1",
    )
    await register_candidate_build(
        session,
        schema_digest="s1",
        document_digest="d1",
        selected_operation="featureFlags",
        candidate_build="build-1",
    )
    await session.commit()

    count = (
        await session.execute(
            sa.text(
                "SELECT count(*) FROM go_api_candidate_build "
                "WHERE schema_digest='s1' AND document_digest='d1' "
                "AND selected_operation='featureFlags' AND candidate_build='build-1'"
            )
        )
    ).scalar_one()
    assert count == 1


async def test_lookup_routing_state_returns_the_row_once_registered(
    session: AsyncSession,
) -> None:
    await register_candidate_build(
        session,
        schema_digest="s1",
        document_digest="d1",
        selected_operation="featureFlags",
        candidate_build="build-1",
    )
    session.add(
        RoutingState(
            schema_digest="s1",
            document_digest="d1",
            selected_operation="featureFlags",
            current_candidate_build="build-1",
            owner="go",
            mode="canary",
            rollout_percentage=5,
        )
    )
    await session.commit()

    result = await lookup_routing_state(
        session,
        schema_digest="s1",
        document_digest="d1",
        selected_operation="featureFlags",
    )
    assert result is not None
    assert result.mode == "canary"
    assert result.rollout_percentage == 5


async def test_record_proof_run_rejects_invalid_stage(session: AsyncSession) -> None:
    with pytest.raises(ValueError, match="invalid proof-run stage"):
        await record_proof_run(
            session,
            schema_digest="s1",
            document_digest="d1",
            selected_operation="featureFlags",
            candidate_build="build-1",
            request_identity="req-1",
            stage="not_a_real_stage",
            terminal_state="match",
        )


async def test_record_proof_run_rejects_shadow_without_watermark(
    session: AsyncSession,
) -> None:
    with pytest.raises(ValueError, match="requires data_watermark"):
        await record_proof_run(
            session,
            schema_digest="s1",
            document_digest="d1",
            selected_operation="featureFlags",
            candidate_build="build-1",
            request_identity="req-1",
            stage="shadow",
            terminal_state="match",
        )


async def test_record_proof_run_persists_a_valid_row(session: AsyncSession) -> None:
    await register_candidate_build(
        session,
        schema_digest="s1",
        document_digest="d1",
        selected_operation="featureFlags",
        candidate_build="build-1",
    )
    proof_run = await record_proof_run(
        session,
        schema_digest="s1",
        document_digest="d1",
        selected_operation="featureFlags",
        candidate_build="build-1",
        request_identity="req-1",
        stage="dual_run",
        terminal_state="mismatch",
    )
    await session.commit()

    assert proof_run.terminal_state == "mismatch"
    count = (
        await session.execute(
            sa.text("SELECT count(*) FROM go_api_proof_run WHERE id = :id"),
            {"id": str(proof_run.id)},
        )
    ).scalar_one()
    assert count == 1
