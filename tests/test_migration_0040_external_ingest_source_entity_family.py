from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine


def test_migration_0040_adds_family_scoped_source_uniqueness() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0040_external_ingest_source_entity_family"
    )
    engine = create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    "CREATE TABLE external_ingest_sources ("
                    "id TEXT PRIMARY KEY, org_id TEXT NOT NULL, system TEXT NOT NULL, "
                    "instance TEXT NOT NULL, "
                    "CONSTRAINT uq_external_ingest_sources_org_system_instance "
                    "UNIQUE (org_id, system, instance))"
                )
            )
            ctx = MigrationContext.configure(conn)
            with Operations.context(ctx):
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert "entity_family" in {
                    column["name"]
                    for column in inspector.get_columns("external_ingest_sources")
                }
                assert {
                    constraint["name"]
                    for constraint in inspector.get_unique_constraints(
                        "external_ingest_sources"
                    )
                } == {"uq_external_ingest_sources_org_system_instance_family"}
                migration.upgrade()
                migration.downgrade()
                assert "entity_family" not in {
                    column["name"]
                    for column in sa.inspect(conn).get_columns(
                        "external_ingest_sources"
                    )
                }
    finally:
        engine.dispose()
