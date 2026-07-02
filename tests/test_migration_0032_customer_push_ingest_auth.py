"""Migration 0032 (external_ingest_sources/external_ingest_tokens) tests.

Follows the idempotent-upgrade/downgrade harness established for migration
0031 (tests/test_rate_limit_observations.py::test_migration_0031_idempotent_upgrade).
"""

from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine


def _load_migration_0032():
    return importlib.import_module(
        "dev_health_ops.alembic.versions.0032_add_customer_push_ingest_auth"
    )


def test_migration_0032_idempotent_upgrade_downgrade():
    migration = _load_migration_0032()
    assert migration.revision == "0032"
    assert migration.down_revision == "0031"

    engine = create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            with Operations.context(ctx):
                migration.upgrade()
                inspector = sa.inspect(conn)
                table_names = inspector.get_table_names()
                assert "external_ingest_sources" in table_names
                assert "external_ingest_tokens" in table_names

                source_columns = {
                    col["name"]
                    for col in inspector.get_columns("external_ingest_sources")
                }
                assert source_columns == {
                    "id",
                    "org_id",
                    "system",
                    "instance",
                    "display_name",
                    "mode",
                    "enabled",
                    "webhook_mode",
                    "webhook_secret_id",
                    "matched_integration_source_id",
                    "created_by_user_id",
                    "created_at",
                    "updated_at",
                }

                token_columns = {
                    col["name"]
                    for col in inspector.get_columns("external_ingest_tokens")
                }
                assert token_columns == {
                    "id",
                    "org_id",
                    "source_id",
                    "name",
                    "token_hash",
                    "token_prefix",
                    "scopes",
                    "created_by_user_id",
                    "expires_at",
                    "revoked_at",
                    "last_used_at",
                    "last_used_ip",
                    "created_at",
                }

                source_index_names = {
                    ix["name"]
                    for ix in inspector.get_indexes("external_ingest_sources")
                }
                assert "ix_external_ingest_sources_org_id" in source_index_names

                token_index_names = {
                    ix["name"] for ix in inspector.get_indexes("external_ingest_tokens")
                }
                assert "ix_external_ingest_tokens_org_id" in token_index_names
                assert "ix_external_ingest_tokens_source_id" in token_index_names
                assert "ix_external_ingest_tokens_org_active" in token_index_names

                # Re-running upgrade() must be a no-op (guarded create-if-missing).
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert inspector.get_table_names().count("external_ingest_sources") == 1
                assert inspector.get_table_names().count("external_ingest_tokens") == 1

                migration.downgrade()
                inspector = sa.inspect(conn)
                remaining = inspector.get_table_names()
                assert "external_ingest_tokens" not in remaining
                assert "external_ingest_sources" not in remaining

                # downgrade() on already-absent tables is also a no-op.
                migration.downgrade()

                # And upgrade() works again from a clean slate.
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert "external_ingest_sources" in inspector.get_table_names()
                assert "external_ingest_tokens" in inspector.get_table_names()
    finally:
        engine.dispose()
