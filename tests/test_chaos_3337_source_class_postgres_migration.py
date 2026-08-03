"""CHAOS-3337 codex full-branch review (2026-08-03): real-Postgres proof for
migrations 0081/0082 -- the NOT VALID install + separate VALIDATE CONSTRAINT
split (mirroring 0074/0075's own precedent), and 0081's downgrade
preflight-and-refuse when a row carries either newly-widened source_class.

Gated exactly like ``test_0066_celery_river_cutover_postgres.py``/
``test_canonical_incident_feature_flag_postgres_migration.py``: skips
locally without a configured Postgres admin URI, but FAILS (never silently
skips) in CI, so this real-lock-semantics proof cannot quietly disappear
from the gate that gave it its name.
"""

from __future__ import annotations

import importlib
import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session

from dev_health_ops.models.dev_persistence import (
    DevConversation,
    DevRun,
    DevRunSourceObservation,
)
from dev_health_ops.models.users import Organization, User

_TEST_ADMIN_URI_ENV = "DEV_HEALTH_TEST_POSTGRES_ADMIN_URI"
_ALEMBIC_SCRIPT_LOCATION = (
    Path(__file__).parent / "src" / "dev_health_ops" / "alembic"
).resolve()
if not _ALEMBIC_SCRIPT_LOCATION.exists():
    _ALEMBIC_SCRIPT_LOCATION = (
        Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
    )

_CONSTRAINT = "ck_dev_run_source_observations_source_class"
_TABLE = "dev_run_source_observations"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_SCRIPT_LOCATION))
    return config


@pytest.fixture
def isolated_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[PostgresMigrationHarness]:
    configured_uri = os.environ.get(_TEST_ADMIN_URI_ENV)
    if configured_uri is None:
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            pytest.fail(
                f"{_TEST_ADMIN_URI_ENV} must be configured for PostgreSQL "
                "migration tests"
            )
        pytest.skip(
            f"{_TEST_ADMIN_URI_ENV} is required for the PostgreSQL migration regression"
        )

    configured_url = make_url(configured_uri)
    if configured_url.get_backend_name() != "postgresql":
        pytest.skip(f"{_TEST_ADMIN_URI_ENV} must use PostgreSQL")
    if configured_url.database != "postgres":
        pytest.skip(f"{_TEST_ADMIN_URI_ENV} must target the postgres admin database")

    database_name = f"test_3337_{uuid.uuid4().hex}"
    admin_url = configured_url.set(
        drivername="postgresql+psycopg2", database="postgres"
    )
    admin_engine = sa.create_engine(admin_url, isolation_level="AUTOCOMMIT")
    database_created = False
    engine: Engine | None = None

    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            database_created = True

        isolated_async_url = configured_url.set(database=database_name)
        isolated_sync_url = isolated_async_url.set(drivername="postgresql+psycopg2")
        engine = sa.create_engine(isolated_sync_url)
        monkeypatch.setenv(
            "POSTGRES_URI", isolated_async_url.render_as_string(hide_password=False)
        )

        yield PostgresMigrationHarness(engine=engine)
    finally:
        if engine is not None:
            engine.dispose()
        try:
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
        finally:
            admin_engine.dispose()


def _seed_run(session: Session, *, org_id, user_id) -> uuid.UUID:
    """The minimal owner chain a ``dev_run_source_observations`` row's
    composite FK (``run_id``, ``org_id``, ``user_id`` -> ``dev_runs``)
    requires."""

    session.add(Organization(id=org_id, slug=f"chaos-3337-{org_id.hex}", name="3337"))
    session.add(User(id=user_id, email=f"chaos-3337-{user_id.hex}@example.com"))
    session.flush()  # organizations/users rows must exist before dev_conversations
    conversation_id = uuid.uuid4()
    session.add(DevConversation(id=conversation_id, org_id=org_id, user_id=user_id))
    session.flush()  # dev_conversations row must exist before dev_runs
    run_id = uuid.uuid4()
    session.add(
        DevRun(
            id=run_id,
            request_id=uuid.uuid4(),
            conversation_id=conversation_id,
            org_id=org_id,
            user_id=user_id,
            state="completed",
            started_at=datetime.now(UTC),
        )
    )
    session.commit()
    return run_id


def _insert_observation(
    session: Session, *, org_id, user_id, run_id, source_class: str
) -> None:
    session.add(
        DevRunSourceObservation(
            id=uuid.uuid4(),
            run_id=run_id,
            org_id=org_id,
            user_id=user_id,
            ordinal=0,
            observation_id=uuid.uuid4(),
            source_class=source_class,
            requirement_level="mandatory",
            observed_state="available_current",
            data_semantics="measured_zero",
            usable_fact_count=1,
            sample_count=None,
            subject_coverage=1.0,
            payload={"schema_version": "dev_source_observation.v1"},
            observed_at=datetime.now(UTC),
        )
    )
    session.commit()


def test_0081_installs_not_valid_and_0082_validates(
    isolated_postgres: PostgresMigrationHarness,
) -> None:
    """Codex HIGH finding 1's exact claim, proven against a real Postgres
    catalog: 0081 must leave the constraint UNvalidated (``pg_constraint.
    convalidated = false``, the metadata-only, no-scan state), and only
    0082's ``VALIDATE CONSTRAINT`` flips it to validated.
    """

    config = _migration_config()
    command.upgrade(config, "0081")

    with isolated_postgres.engine.connect() as connection:
        convalidated_after_0081 = connection.execute(
            sa.text("SELECT convalidated FROM pg_constraint WHERE conname = :name"),
            {"name": _CONSTRAINT},
        ).scalar_one()
    assert convalidated_after_0081 is False

    command.upgrade(config, "0082")

    with isolated_postgres.engine.connect() as connection:
        convalidated_after_0082 = connection.execute(
            sa.text("SELECT convalidated FROM pg_constraint WHERE conname = :name"),
            {"name": _CONSTRAINT},
        ).scalar_one()
    assert convalidated_after_0082 is True


def test_every_prior_source_class_still_accepted_after_the_widen(
    isolated_postgres: PostgresMigrationHarness,
) -> None:
    """Codex probe: 0081's recreate must list every previously-allowed
    value, not just the two new ones -- proved by actually inserting one
    row per PRIOR value (not just asserting the SQL string contains them)
    after the widened constraint is installed and validated.
    """

    migration_0081 = importlib.import_module(
        "dev_health_ops.alembic.versions.0081_widen_source_observation_source_class"
    )
    prior_source_classes = migration_0081._PRIOR_SOURCE_CLASSES

    config = _migration_config()
    command.upgrade(config, "0082")

    with Session(isolated_postgres.engine) as session:
        org_id, user_id = uuid.uuid4(), uuid.uuid4()
        run_id = _seed_run(session, org_id=org_id, user_id=user_id)
        for index, source_class in enumerate(prior_source_classes):
            session.add(
                DevRunSourceObservation(
                    id=uuid.uuid4(),
                    run_id=run_id,
                    org_id=org_id,
                    user_id=user_id,
                    ordinal=index,
                    observation_id=uuid.uuid4(),
                    source_class=source_class,
                    requirement_level="mandatory",
                    observed_state="available_current",
                    data_semantics="measured_zero",
                    usable_fact_count=1,
                    sample_count=None,
                    subject_coverage=1.0,
                    payload={"schema_version": "dev_source_observation.v1"},
                    observed_at=datetime.now(UTC),
                )
            )
        session.commit()  # must not raise -- every prior value still valid

    with isolated_postgres.engine.connect() as connection:
        count = connection.execute(
            sa.text(f"SELECT count(*) FROM {_TABLE} WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).scalar_one()
    assert count == len(prior_source_classes)


def test_postgres_downgrade_refuses_when_new_source_classes_present(
    isolated_postgres: PostgresMigrationHarness,
) -> None:
    """Codex HIGH finding 2, planted directly against real Postgres: a row
    with source_class='health_profile' must block the downgrade to 0080
    with the explicit refusal message, never a bare CHECK-violation
    traceback and never a silent data-mutating rollback.
    """

    config = _migration_config()
    command.upgrade(config, "0082")

    with Session(isolated_postgres.engine) as session:
        org_id, user_id = uuid.uuid4(), uuid.uuid4()
        run_id = _seed_run(session, org_id=org_id, user_id=user_id)
        _insert_observation(
            session,
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            source_class="health_profile",
        )

    with pytest.raises(Exception, match="refusing to downgrade 0081"):
        command.downgrade(config, "0080")

    with isolated_postgres.engine.connect() as connection:
        version_after_refused_downgrade = connection.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).scalar_one()
    # The refusal must abort BEFORE dropping/narrowing the constraint --
    # the schema stays at 0082, not left half-migrated.
    assert version_after_refused_downgrade == "0082"


def test_postgres_downgrade_succeeds_when_no_new_source_classes_present(
    isolated_postgres: PostgresMigrationHarness,
) -> None:
    config = _migration_config()
    command.upgrade(config, "0082")

    with Session(isolated_postgres.engine) as session:
        org_id, user_id = uuid.uuid4(), uuid.uuid4()
        run_id = _seed_run(session, org_id=org_id, user_id=user_id)
        _insert_observation(
            session,
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            source_class="status_change",
        )

    command.downgrade(config, "0080")  # must not raise

    with isolated_postgres.engine.connect() as connection:
        version_after_downgrade = connection.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).scalar_one()
    assert version_after_downgrade == "0080"

    command.upgrade(config, "0082")  # re-upgrade must still succeed

    with isolated_postgres.engine.connect() as connection:
        version_after_reupgrade = connection.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).scalar_one()
    assert version_after_reupgrade == "0082"
