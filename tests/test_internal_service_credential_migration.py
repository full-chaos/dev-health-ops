from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine


def test_migration_0038_creates_reversible_internal_service_credential_tables():
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0038_add_internal_service_credentials"
    )
    assert migration.revision == "0038"
    assert migration.down_revision == "0037"
    engine = create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            with Operations.context(ctx):
                migration.upgrade()
                tables = set(sa.inspect(conn).get_table_names())
                assert "internal_service_credentials" in tables
                assert "internal_service_credential_audits" in tables
                migration.downgrade()
                assert "internal_service_credentials" not in set(
                    sa.inspect(conn).get_table_names()
                )
                migration.upgrade()
                assert "internal_service_credentials" in set(
                    sa.inspect(conn).get_table_names()
                )
    finally:
        engine.dispose()
