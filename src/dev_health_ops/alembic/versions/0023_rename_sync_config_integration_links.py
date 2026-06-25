"""Rename sync_config integration links to canonical names.

Forward-only rename of the ``SyncConfiguration`` -> ``Integration`` /
``IntegrationSource`` links originally created in 0016. Every sync config is now
created integration-native (there is no separate migration/bridge concept), so
the ``migrated_`` prefix is dropped:

    migrated_integration_id -> integration_id
    migrated_source_id      -> source_id

This is a pure rename: existing column values are preserved, so configs already
linked to an integration stay routable after deploy. Postgres-targeted (the
semantic DB); the legacy 0016 revision is left immutable so any database that
already applied it migrates forward through this revision instead of silently
keeping the old column names.

Revision ID: 0023
Revises: 0022
Create Date: 2026-06-25 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0023"
down_revision: str | None = "0022"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    op.execute(
        "ALTER TABLE sync_configurations "
        "RENAME COLUMN migrated_integration_id TO integration_id"
    )
    op.execute(
        "ALTER TABLE sync_configurations RENAME COLUMN migrated_source_id TO source_id"
    )
    op.execute(
        "ALTER INDEX IF EXISTS ix_sync_configurations_migrated_integration_id "
        "RENAME TO ix_sync_configurations_integration_id"
    )
    op.execute(
        "ALTER INDEX IF EXISTS ix_sync_configurations_migrated_source_id "
        "RENAME TO ix_sync_configurations_source_id"
    )
    op.execute(
        "ALTER TABLE sync_configurations "
        "RENAME CONSTRAINT fk_sync_config_migrated_integration_id_integrations "
        "TO fk_sync_config_integration_id_integrations"
    )
    op.execute(
        "ALTER TABLE sync_configurations "
        "RENAME CONSTRAINT fk_sync_config_migrated_source_id_integration_sources "
        "TO fk_sync_config_source_id_integration_sources"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE sync_configurations "
        "RENAME CONSTRAINT fk_sync_config_source_id_integration_sources "
        "TO fk_sync_config_migrated_source_id_integration_sources"
    )
    op.execute(
        "ALTER TABLE sync_configurations "
        "RENAME CONSTRAINT fk_sync_config_integration_id_integrations "
        "TO fk_sync_config_migrated_integration_id_integrations"
    )
    op.execute(
        "ALTER INDEX IF EXISTS ix_sync_configurations_source_id "
        "RENAME TO ix_sync_configurations_migrated_source_id"
    )
    op.execute(
        "ALTER INDEX IF EXISTS ix_sync_configurations_integration_id "
        "RENAME TO ix_sync_configurations_migrated_integration_id"
    )
    op.execute(
        "ALTER TABLE sync_configurations RENAME COLUMN source_id TO migrated_source_id"
    )
    op.execute(
        "ALTER TABLE sync_configurations "
        "RENAME COLUMN integration_id TO migrated_integration_id"
    )
