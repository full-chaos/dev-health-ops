from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine


def _load_migration_0036():
    return importlib.import_module(
        "dev_health_ops.alembic.versions.0036_seed_customer_push_ingest_feature_flag"
    )


def test_migration_0036_seeds_customer_push_ingest_feature_flag_idempotently():
    migration = _load_migration_0036()
    assert migration.revision == "0036"
    assert migration.down_revision == "0035"

    engine = create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    """
                    CREATE TABLE feature_flags (
                        id TEXT PRIMARY KEY,
                        key TEXT NOT NULL UNIQUE,
                        name TEXT NOT NULL,
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
            ctx = MigrationContext.configure(conn)
            with Operations.context(ctx):
                migration.upgrade()
                migration.upgrade()

                rows = conn.execute(
                    sa.text(
                        """
                        SELECT key, name, category, min_tier, is_enabled
                        FROM feature_flags
                        WHERE key = 'customer_push_ingest'
                        """
                    )
                ).all()
                assert rows == [
                    (
                        "customer_push_ingest",
                        "Customer Push Ingest",
                        "integrations",
                        "team",
                        1,
                    )
                ]

                migration.downgrade()
                rows_after_downgrade = conn.execute(
                    sa.text("SELECT key FROM feature_flags")
                ).all()
                assert rows_after_downgrade == [("customer_push_ingest",)]
    finally:
        engine.dispose()
