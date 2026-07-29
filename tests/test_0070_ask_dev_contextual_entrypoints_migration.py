from __future__ import annotations

import importlib
import uuid
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine


def test_migration_0070_is_additive_idempotent_and_preserves_base_ask_dev() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0070_seed_ask_dev_contextual_entrypoints_feature_flag"
    )
    assert migration.revision == "0070"
    assert migration.down_revision == "0069"
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
            conn.execute(
                sa.text(
                    """
                    INSERT INTO feature_flags
                        (id, key, name, category, min_tier, is_enabled,
                         is_beta, is_deprecated, created_at, updated_at)
                    VALUES
                        (:id, 'ask_dev', 'Ask Dev', 'analytics', 'community',
                         TRUE, FALSE, FALSE, :now, :now)
                    """
                ),
                {"id": str(uuid.uuid4()), "now": datetime.now(UTC)},
            )
            context = MigrationContext.configure(conn)
            with Operations.context(context):
                migration.upgrade()
                migration.upgrade()
                rows = conn.execute(
                    sa.text("SELECT key, is_enabled FROM feature_flags ORDER BY key")
                ).all()
                assert rows == [
                    ("ask_dev", 1),
                    ("ask_dev_contextual_entrypoints", 1),
                ]
                migration.downgrade()
                remaining = conn.execute(
                    sa.text("SELECT key FROM feature_flags ORDER BY key")
                ).scalars()
                assert list(remaining) == [
                    "ask_dev",
                    "ask_dev_contextual_entrypoints",
                ]
    finally:
        engine.dispose()
