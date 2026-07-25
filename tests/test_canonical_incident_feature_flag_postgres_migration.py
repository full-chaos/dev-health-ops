from __future__ import annotations

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

_FEATURE_KEY = "canonical_incident_ingestion"
_UNRELATED_FEATURE_KEY = "agent_context_runtime"
_TEST_ADMIN_URI_ENV = "DEV_HEALTH_TEST_POSTGRES_ADMIN_URI"
_ALEMBIC_SCRIPT_LOCATION = (
    Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
)


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
        pytest.skip(
            f"{_TEST_ADMIN_URI_ENV} is required for the PostgreSQL migration regression"
        )

    configured_url = make_url(configured_uri)
    if configured_url.get_backend_name() != "postgresql":
        pytest.skip(f"{_TEST_ADMIN_URI_ENV} must use PostgreSQL")
    if configured_url.database != "postgres":
        pytest.skip(f"{_TEST_ADMIN_URI_ENV} must target the postgres admin database")

    database_name = f"test_0042_{uuid.uuid4().hex}"
    admin_url = configured_url.set(
        drivername="postgresql+psycopg2",
        database="postgres",
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
            "POSTGRES_URI",
            isolated_async_url.render_as_string(hide_password=False),
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


def test_postgres_downgrade_removes_only_canonical_feature_state(
    isolated_postgres: PostgresMigrationHarness,
) -> None:
    config = _migration_config()
    command.upgrade(config, "0042")
    org_id = uuid.uuid4()
    now = datetime.now(UTC)

    with isolated_postgres.engine.begin() as connection:
        feature_rows = connection.execute(
            sa.text(
                """
                SELECT id, key
                FROM feature_flags
                WHERE key IN (:canonical_key, :unrelated_key)
                """
            ),
            {
                "canonical_key": _FEATURE_KEY,
                "unrelated_key": _UNRELATED_FEATURE_KEY,
            },
        ).all()
        feature_ids = {str(row.key): row.id for row in feature_rows}
        canonical_feature_id = feature_ids[_FEATURE_KEY]
        unrelated_feature_id = feature_ids[_UNRELATED_FEATURE_KEY]
        connection.execute(
            sa.text(
                """
                INSERT INTO organizations (id, slug, name)
                VALUES (:id, :slug, :name)
                """
            ),
            {
                "id": org_id,
                "slug": f"migration-0042-{org_id.hex}",
                "name": "Migration 0042 Regression",
            },
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO org_feature_overrides
                    (id, org_id, feature_id, is_enabled, created_at, updated_at)
                VALUES
                    (:canonical_override_id, :org_id, :canonical_feature_id,
                     TRUE, :created_at, :updated_at),
                    (:unrelated_override_id, :org_id, :unrelated_feature_id,
                     TRUE, :created_at, :updated_at)
                """
            ),
            {
                "canonical_override_id": uuid.uuid4(),
                "unrelated_override_id": uuid.uuid4(),
                "org_id": org_id,
                "canonical_feature_id": canonical_feature_id,
                "unrelated_feature_id": unrelated_feature_id,
                "created_at": now,
                "updated_at": now,
            },
        )

    command.downgrade(config, "0041")

    with isolated_postgres.engine.connect() as connection:
        version_after_downgrade = connection.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).scalar_one()
        canonical_features_after_downgrade = connection.execute(
            sa.text("SELECT COUNT(*) FROM feature_flags WHERE key = :key"),
            {"key": _FEATURE_KEY},
        ).scalar_one()
        canonical_overrides_after_downgrade = connection.execute(
            sa.text(
                "SELECT COUNT(*) FROM org_feature_overrides WHERE feature_id = :id"
            ),
            {"id": canonical_feature_id},
        ).scalar_one()
        unrelated_features_after_downgrade = connection.execute(
            sa.text("SELECT COUNT(*) FROM feature_flags WHERE key = :key"),
            {"key": _UNRELATED_FEATURE_KEY},
        ).scalar_one()
        unrelated_overrides_after_downgrade = connection.execute(
            sa.text(
                "SELECT COUNT(*) FROM org_feature_overrides WHERE feature_id = :id"
            ),
            {"id": unrelated_feature_id},
        ).scalar_one()

    assert version_after_downgrade == "0041"
    assert canonical_features_after_downgrade == 0
    assert canonical_overrides_after_downgrade == 0
    assert unrelated_features_after_downgrade == 1
    assert unrelated_overrides_after_downgrade == 1

    command.upgrade(config, "0042")

    with isolated_postgres.engine.connect() as connection:
        version_after_reupgrade = connection.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).scalar_one()
        canonical_rows_after_reupgrade = connection.execute(
            sa.text(
                """
                SELECT key, is_enabled
                FROM feature_flags
                WHERE key = :key
                """
            ),
            {"key": _FEATURE_KEY},
        ).all()
        positive_canonical_overrides_after_reupgrade = connection.execute(
            sa.text(
                """
                SELECT COUNT(*)
                FROM org_feature_overrides override_row
                JOIN feature_flags feature ON feature.id = override_row.feature_id
                WHERE feature.key = :key
                  AND override_row.is_enabled = TRUE
                """
            ),
            {"key": _FEATURE_KEY},
        ).scalar_one()

    assert version_after_reupgrade == "0042"
    assert canonical_rows_after_reupgrade == [(_FEATURE_KEY, True)]
    assert positive_canonical_overrides_after_reupgrade == 0
