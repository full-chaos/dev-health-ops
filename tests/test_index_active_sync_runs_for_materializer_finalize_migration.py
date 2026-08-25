"""PostgreSQL proof for alembic 0111's active-sync-runs partial index (CHAOS-4262).

The migration's only job is to make ``sync_runs``'s ``status NOT IN (...)``
predicate sargable for the materializer's finalize/dispatch candidate scan,
which CHAOS-4262 found driving a Seq Scan whose planner cost estimate grows
with all of ``sync_runs`` history rather than the active set -- crossing
``jit_above_cost`` and triggering a JIT compile for a statement that touches
zero rows in steady state (see internal/syncreconciler/
materializer_jit_cost_integration_test.go for the executable EXPLAIN proof
that this specific index changes the plan).

An index that exists but that the planner declines to use is not a fix
(CHAOS-4092's own precedent test makes the same point for the sibling
terminal-repair index), so this asserts VALID + the exact predicate text
Postgres stores for it, not just presence by name.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine, make_url

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_INDEX = "ix_sync_runs_active_candidates"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture
def migrated_to_0110(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[PostgresMigrationHarness]:
    configured_uri = os.environ.get(_POSTGRES_URI_ENV)
    if configured_uri is None:
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            pytest.fail(
                f"{_POSTGRES_URI_ENV} must be configured for PostgreSQL migration tests"
            )
        pytest.skip(f"requires {_POSTGRES_URI_ENV}")

    configured_url = make_url(configured_uri)
    if configured_url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")

    database_name = f"test_chaos_4262_{uuid.uuid4().hex}"
    admin_engine = sa.create_engine(
        configured_url.set(drivername="postgresql+psycopg2", database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    database_created = False
    engine: Engine | None = None
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            database_created = True

        monkeypatch.setenv(
            "POSTGRES_URI",
            configured_url.set(
                drivername="postgresql+asyncpg", database=database_name
            ).render_as_string(hide_password=False),
        )
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        # CREATE INDEX CONCURRENTLY cannot run inside alembic's own migration
        # transaction wrapper on some drivers/backends; the migration itself
        # already isolates it via autocommit_block(), so upgrading through
        # alembic's command API (which uses the same connection alembic
        # always uses) is the right level to test at rather than reinventing
        # a bespoke runner.
        command.upgrade(_migration_config(), "0110")

        engine = sa.create_engine(
            configured_url.set(drivername="postgresql+psycopg2", database=database_name)
        )
        yield PostgresMigrationHarness(engine=engine)
    finally:
        if engine is not None:
            engine.dispose()
        if database_created:
            with admin_engine.connect() as connection:
                connection.execute(
                    sa.text(
                        """
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = :database_name
                          AND pid <> pg_backend_pid()
                        """
                    ),
                    {"database_name": database_name},
                )
                connection.exec_driver_sql(f'DROP DATABASE "{database_name}"')
        admin_engine.dispose()


def _revisions(engine: Engine) -> set[str]:
    with engine.connect() as connection:
        return {
            str(row[0])
            for row in connection.execute(
                sa.text("SELECT version_num FROM alembic_version")
            )
        }


def _index_row(engine: Engine) -> tuple[bool, str] | None:
    """(indisvalid, predicate expression) for the index, or None if absent."""

    with engine.connect() as connection:
        row = connection.execute(
            sa.text(
                """
                SELECT pg_index.indisvalid,
                       pg_get_expr(pg_index.indpred, pg_index.indrelid)
                FROM pg_index
                JOIN pg_class ON pg_class.oid = pg_index.indexrelid
                WHERE pg_class.relname = :index_name
                """
            ),
            {"index_name": _INDEX},
        ).one_or_none()
    if row is None:
        return None
    return bool(row[0]), str(row[1])


def test_0111_adds_valid_partial_index_with_the_exact_query_predicate(
    migrated_to_0110: PostgresMigrationHarness,
) -> None:
    assert _index_row(migrated_to_0110.engine) is None

    command.upgrade(_migration_config(), "0111")

    assert _revisions(migrated_to_0110.engine) == {"0111"}
    row = _index_row(migrated_to_0110.engine)
    assert row is not None, "0111 must create ix_sync_runs_active_candidates"
    is_valid, predicate = row
    assert is_valid, "a CONCURRENTLY build that failed midway leaves an INVALID index"
    # The predicate must be the exact NOT IN the materializer's finalize query
    # uses (case/spacing normalized by Postgres's own pretty-printer), not a
    # rewritten positive allowlist -- see the migration docstring / CHAOS-4107
    # for why a positive rewrite would silently stop covering a future status.
    normalized = predicate.replace("\n", " ")
    assert "status <> ALL" in normalized or "NOT (status = ANY" in normalized
    for status in ("success", "partial_failed", "failed"):
        assert status in normalized


def test_0111_downgrade_drops_the_index(
    migrated_to_0110: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0111")
    assert _index_row(migrated_to_0110.engine) is not None

    command.downgrade(_migration_config(), "0110")

    assert _revisions(migrated_to_0110.engine) == {"0110"}
    assert _index_row(migrated_to_0110.engine) is None
