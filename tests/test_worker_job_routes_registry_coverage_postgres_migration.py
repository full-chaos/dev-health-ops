"""PostgreSQL proof that every registered job kind has a worker_job_routes row.

CHAOS-3092 PR-B regression: metrics.remaining.work_item_attribution was added
to contracts/jobs/v1/registry.json (and migration-state.json, as
celery_removed/river/none -- Go-native from birth, no Celery predecessor) but
never got its own worker_job_routes seed row, unlike every other kind in that
same shape (system.sync_coverage_refresh via alembic 0094,
sync.team_repo_ownership_derivation via alembic 0115). internal/jobroute's
Controller.DeferredKinds iterates every registered kind and queries
worker_job_routes for each one on every reconciler step (including the
first, at process startup); a kind with no row there fails the WHOLE step
instantly with jobroute.ErrUnknownRoute -> joboutbox.ErrUnavailable -- a
crash with no Postgres-side error to find, because a zero-row SELECT isn't
one. 0123 fixed this kind; this test is the recurrence guard so the next
native-from-birth kind fails loudly here instead of live in a reconciler.

Migrations 0064 (baseline seed) and 0066 (River activation) only cover the
legacy checked-in kind list frozen at their own revision -- deliberately not
kept in sync with the live registry (test_worker_job_route_baseline_migration.py
pins that). A kind added after 0066 needs its OWN one-row seed migration,
same pattern as 0094/0115/0123, and this test is what catches a kind that
never got one.
"""

from __future__ import annotations

import json
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
_REGISTRY_PATH = (
    Path(__file__).parents[1] / "contracts" / "jobs" / "v1" / "registry.json"
)


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture
def migrated_to_application_schema_head(
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

    database_name = f"test_chaos_3092_worker_job_routes_{uuid.uuid4().hex}"
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
                drivername="postgresql+asyncpg",
                database=database_name,
            ).render_as_string(hide_password=False),
        )
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        command.upgrade(_migration_config(), "application_schema@head")

        engine = sa.create_engine(
            configured_url.set(
                drivername="postgresql+psycopg2",
                database=database_name,
            )
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


def _registry_kinds() -> set[str]:
    registry = json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))
    return {job["kind"] for job in registry["jobs"]}


def _seeded_route_kinds(engine: Engine) -> set[str]:
    with engine.connect() as connection:
        return {
            str(row[0])
            for row in connection.execute(
                sa.text("SELECT job_kind FROM worker_job_routes")
            )
        }


def test_every_registry_kind_has_a_worker_job_routes_row(
    migrated_to_application_schema_head: PostgresMigrationHarness,
) -> None:
    registry_kinds = _registry_kinds()
    seeded_kinds = _seeded_route_kinds(migrated_to_application_schema_head.engine)

    missing = registry_kinds - seeded_kinds
    assert not missing, (
        "contracts/jobs/v1/registry.json kind(s) with no worker_job_routes row "
        f"after the full application_schema migration chain: {sorted(missing)}. "
        "internal/jobroute.Controller.DeferredKinds queries this table for every "
        "registered kind on every reconciler step -- a missing row fails the "
        "whole step instantly (CHAOS-3092 PR-B's live reconciler crash). Add a "
        "dedicated one-row seed migration for the missing kind(s), same pattern "
        "as 0094 (system.sync_coverage_refresh), 0115 "
        "(sync.team_repo_ownership_derivation), or 0123 "
        "(metrics.remaining.work_item_attribution)."
    )
