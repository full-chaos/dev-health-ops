"""PostgreSQL proof for the IntegrationDataset unavailable marker (CHAOS-4048)."""

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
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.engine.interfaces import ReflectedColumn

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_TABLE = "integration_datasets"
_COLUMNS = ("unavailable_reason", "unavailable_since", "unavailable_last_seen_at")
_INTEGRATION_ID = "e4b6b1c0-8f0a-4c9a-9c1a-0f2a5b6c7d8e"
_ORG_ID = "org_chaos_4048"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture
def migrated_to_0105(
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

    database_name = f"test_chaos_4048_{uuid.uuid4().hex}"
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
        command.upgrade(_migration_config(), "0105")

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


def _revisions(engine: Engine) -> set[str]:
    with engine.connect() as connection:
        return {
            str(row[0])
            for row in connection.execute(
                sa.text("SELECT version_num FROM alembic_version")
            )
        }


def _columns(engine: Engine) -> dict[str, ReflectedColumn]:
    return {
        str(column["name"]): column for column in sa.inspect(engine).get_columns(_TABLE)
    }


def _seed_legacy_dataset(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO integrations (
                    id, org_id, provider, name, config, is_active,
                    created_at, updated_at
                ) VALUES (
                    :integration_id, :org_id, 'pagerduty', 'legacy pagerduty',
                    '{}'::jsonb, true, now(), now()
                )
                """
            ),
            {"integration_id": _INTEGRATION_ID, "org_id": _ORG_ID},
        )
        connection.execute(
            sa.text(
                f"""
                INSERT INTO {_TABLE} (
                    id, org_id, integration_id, dataset_key, is_enabled, options
                ) VALUES (
                    gen_random_uuid(), :org_id, :integration_id, 'teams', true, '{{}}'::jsonb
                )
                """
            ),
            {"integration_id": _INTEGRATION_ID, "org_id": _ORG_ID},
        )


def test_0106_adds_nullable_unavailable_marker_without_rewriting_history(
    migrated_to_0105: PostgresMigrationHarness,
) -> None:
    _seed_legacy_dataset(migrated_to_0105.engine)
    command.upgrade(_migration_config(), "0106")

    columns = _columns(migrated_to_0105.engine)
    reason_column = columns["unavailable_reason"]
    assert isinstance(reason_column["type"], postgresql.VARCHAR)
    assert reason_column["type"].length == 64
    for column_name in _COLUMNS:
        assert columns[column_name]["nullable"] is True

    with migrated_to_0105.engine.connect() as connection:
        row = connection.execute(
            sa.text(
                f"SELECT {', '.join(_COLUMNS)} FROM {_TABLE} "
                "WHERE dataset_key = 'teams' AND integration_id = :integration_id"
            ),
            {"integration_id": _INTEGRATION_ID},
        ).one()
    assert tuple(row) == (None, None, None)
    assert _revisions(migrated_to_0105.engine) == {"0106"}


def test_0106_repeated_failure_sets_and_advances_the_marker(
    migrated_to_0105: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0106")
    _seed_legacy_dataset(migrated_to_0105.engine)

    with migrated_to_0105.engine.begin() as connection:
        connection.execute(
            sa.text(
                f"""
                UPDATE {_TABLE}
                SET unavailable_reason = 'provider_dataset_unavailable',
                    unavailable_since = COALESCE(unavailable_since, :first),
                    unavailable_last_seen_at = :first
                WHERE dataset_key = 'teams' AND integration_id = :integration_id
                """
            ),
            {"first": "2026-08-01T00:00:00+00:00", "integration_id": _INTEGRATION_ID},
        )
        connection.execute(
            sa.text(
                f"""
                UPDATE {_TABLE}
                SET unavailable_reason = 'provider_dataset_unavailable',
                    unavailable_since = COALESCE(unavailable_since, :second),
                    unavailable_last_seen_at = :second
                WHERE dataset_key = 'teams' AND integration_id = :integration_id
                """
            ),
            {"second": "2026-08-21T00:00:00+00:00", "integration_id": _INTEGRATION_ID},
        )

    with migrated_to_0105.engine.connect() as connection:
        reason, since, last_seen = connection.execute(
            sa.text(
                f"SELECT {', '.join(_COLUMNS)} FROM {_TABLE} "
                "WHERE dataset_key = 'teams' AND integration_id = :integration_id"
            ),
            {"integration_id": _INTEGRATION_ID},
        ).one()
    assert reason == "provider_dataset_unavailable"
    assert since.isoformat() == "2026-08-01T00:00:00+00:00"
    assert last_seen.isoformat() == "2026-08-21T00:00:00+00:00"

    with migrated_to_0105.engine.begin() as connection:
        connection.execute(
            sa.text(
                f"""
                UPDATE {_TABLE}
                SET unavailable_reason = NULL,
                    unavailable_since = NULL,
                    unavailable_last_seen_at = NULL
                WHERE dataset_key = 'teams' AND integration_id = :integration_id
                """
            ),
            {"integration_id": _INTEGRATION_ID},
        )
    with migrated_to_0105.engine.connect() as connection:
        row = connection.execute(
            sa.text(
                f"SELECT {', '.join(_COLUMNS)} FROM {_TABLE} "
                "WHERE dataset_key = 'teams' AND integration_id = :integration_id"
            ),
            {"integration_id": _INTEGRATION_ID},
        ).one()
    assert tuple(row) == (None, None, None)


def test_0106_downgrade_and_reupgrade_converge(
    migrated_to_0105: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0106")
    command.downgrade(_migration_config(), "0105")

    remaining = _columns(migrated_to_0105.engine)
    for column_name in _COLUMNS:
        assert column_name not in remaining
    assert _revisions(migrated_to_0105.engine) == {"0105"}

    command.upgrade(_migration_config(), "application_schema@head")
    upgraded = _columns(migrated_to_0105.engine)
    for column_name in _COLUMNS:
        assert column_name in upgraded
