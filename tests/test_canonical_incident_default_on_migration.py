from __future__ import annotations

import importlib
from pathlib import Path
from types import ModuleType

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine

_FEATURE_KEY = "canonical_incident_ingestion"


def _migration() -> ModuleType:
    return importlib.import_module(
        "dev_health_ops.alembic.versions.0048_enable_canonical_incident_ingestion"
    )


def _create_feature_flags_table(connection: sa.Connection) -> None:
    connection.execute(
        sa.text(
            """
            CREATE TABLE feature_flags (
                id TEXT PRIMARY KEY,
                key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                description TEXT,
                category TEXT NOT NULL,
                min_tier TEXT NOT NULL,
                is_enabled BOOLEAN NOT NULL,
                is_beta BOOLEAN NOT NULL,
                is_deprecated BOOLEAN NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL
            )
            """
        )
    )


def test_default_on_migration_repairs_disabled_row_idempotently() -> None:
    migration = _migration()
    engine = create_engine("sqlite:///:memory:")

    try:
        with engine.connect() as connection:
            _create_feature_flags_table(connection)
            connection.execute(
                sa.text(
                    """
                    INSERT INTO feature_flags
                        (id, key, name, category, min_tier, is_enabled, is_beta,
                         is_deprecated, created_at, updated_at)
                    VALUES
                        ('existing', :key, 'Canonical Incident Ingestion',
                         'integrations', 'community', FALSE, FALSE, FALSE,
                         CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """
                ),
                {"key": _FEATURE_KEY},
            )
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                migration.upgrade()
                migration.upgrade()

            row = connection.execute(
                sa.text(
                    "SELECT key, min_tier, is_enabled FROM feature_flags WHERE key = :key"
                ),
                {"key": _FEATURE_KEY},
            ).one()

        assert row == (_FEATURE_KEY, "community", True)
    finally:
        engine.dispose()


def test_default_on_migration_inserts_missing_feature() -> None:
    migration = _migration()
    engine = create_engine("sqlite:///:memory:")

    try:
        with engine.connect() as connection:
            _create_feature_flags_table(connection)
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                migration.upgrade()

            row = connection.execute(
                sa.text(
                    "SELECT key, min_tier, is_enabled FROM feature_flags WHERE key = :key"
                ),
                {"key": _FEATURE_KEY},
            ).one()

        assert row == (_FEATURE_KEY, "community", True)
    finally:
        engine.dispose()


def test_default_on_migration_extends_current_head() -> None:
    migration = _migration()

    assert migration.revision == "0048"
    assert migration.down_revision == "0047"
    assert migration.__file__ is not None
    assert Path(migration.__file__).name == (
        "0048_enable_canonical_incident_ingestion.py"
    )
