"""PostgreSQL proof that every families.json family is accepted by the
remaining_metric_runs family CHECK constraint.

CHAOS follow-up to families.json/ck_remaining_metric_run_family drift:
internal/jobs/metrics/remaining/families.json listed "work_item_attribution"
as a valid remaining-metrics family from the day it shipped Go-native, but
the Postgres `ck_remaining_metric_run_family` CHECK constraint on
`remaining_metric_runs` (created 0058, narrowed 0110) was never updated to
match -- no migration between 0058 and 0125 touched it. Every
StartRunTx INSERT for that family failed CHECK constraint SQLSTATE 23514,
deterministically, on every environment running this schema (reproduced
live against a compose deployment's coordinator database; see 0126).

families.json's Go-side validator (validateStartRunRequest, which loads this
same file) has no idea the database disagrees -- an INSERT is the first
place the two ever have to agree, and by then the row has already been
rejected. This test is the recurrence guard: it actually runs every
migration against a real, disposable Postgres and reads the constraint's own
definition back with `pg_get_constraintdef`, so it is immune to a future
family being added to families.json without its own CHECK-constraint
migration, the same way
test_worker_job_routes_registry_coverage_postgres_migration.py guards the
matching worker_job_routes seed-row drift for the same underlying CHAOS-3092
PR-B root cause (a families.json entry with no corresponding durable-state
migration).
"""

from __future__ import annotations

import json
import os
import re
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
_FAMILIES_PATH = (
    Path(__file__).parents[1]
    / "internal"
    / "jobs"
    / "metrics"
    / "remaining"
    / "families.json"
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

    database_name = f"test_remaining_family_check_{uuid.uuid4().hex}"
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


def _families() -> set[str]:
    catalog = json.loads(_FAMILIES_PATH.read_text(encoding="utf-8"))
    return {family["name"] for family in catalog["families"]}


def _family_check_allowed_values(engine: Engine) -> set[str]:
    """The live, post-migration ck_remaining_metric_run_family definition,
    read back with pg_get_constraintdef rather than assumed from any
    migration's source -- Postgres may render `IN (...)` as `= ANY (ARRAY[...])`,
    so this extracts every single-quoted literal rather than parsing either
    surface form specifically.
    """
    with engine.connect() as connection:
        definition = connection.execute(
            sa.text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname = 'ck_remaining_metric_run_family' "
                "AND conrelid = 'remaining_metric_runs'::regclass"
            )
        ).scalar_one()
    return set(re.findall(r"'([^']*)'", definition))


def test_every_family_is_accepted_by_the_remaining_metric_run_family_check(
    migrated_to_application_schema_head: PostgresMigrationHarness,
) -> None:
    families = _families()
    allowed = _family_check_allowed_values(migrated_to_application_schema_head.engine)

    missing = families - allowed
    assert not missing, (
        "internal/jobs/metrics/remaining/families.json family(ies) rejected "
        f"by ck_remaining_metric_run_family after the full application_schema "
        f"migration chain: {sorted(missing)}. Every StartRunTx INSERT for "
        "this family will fail with SQLSTATE 23514, deterministically, on "
        "every environment running this schema (CHAOS: work_item_attribution "
        "shipped in families.json without a matching CHECK-constraint "
        "migration, fixed in 0126). Add a migration that drops and recreates "
        "ck_remaining_metric_run_family with the missing family included, "
        "same pattern as 0126."
    )
