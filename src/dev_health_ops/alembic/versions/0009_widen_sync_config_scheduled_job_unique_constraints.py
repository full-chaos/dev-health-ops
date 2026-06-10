"""Widen unique constraints on sync_configurations and scheduled_jobs to include provider.

Revision ID: 0009
Revises: 0008
Create Date: 2026-06-10 00:00:00

Widens the unique key on sync_configurations from (org_id, name) to
(org_id, provider, name), and on scheduled_jobs from (org_id, name) to
(org_id, provider, name).  This mirrors the existing IntegrationCredential
constraint (uq_credentials_org_provider_name) and allows same-named configs
across different providers to coexist (e.g. a 'chaos' GitHub config and a
'chaos' Linear config).

Also adds the provider column to scheduled_jobs (nullable=False, server_default='')
so the new constraint can reference it.

Dedupe strategy (upgrade):
  For sync_configurations: when two rows share (org_id, name) but differ on
  provider, they are already distinct under the new key — no action needed.
  When two rows share (org_id, name, provider) the newer row (higher created_at)
  is kept and the older duplicate is deleted.

  Same logic applies to scheduled_jobs.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. Add provider column to scheduled_jobs (server_default='' so
    #    existing rows get an empty string, matching non-provider jobs).
    # ------------------------------------------------------------------
    op.add_column(
        "scheduled_jobs",
        sa.Column(
            "provider",
            sa.Text(),
            nullable=False,
            server_default="",
            comment="Provider this job belongs to (empty string for non-provider jobs)",
        ),
    )

    # ------------------------------------------------------------------
    # 2. Dedupe sync_configurations: delete older duplicates that share
    #    (org_id, provider, name).  Rows that only share (org_id, name)
    #    but differ on provider are already distinct — no action needed.
    # ------------------------------------------------------------------
    op.execute(
        """
        DELETE FROM sync_configurations
        WHERE id IN (
            SELECT id FROM (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY org_id, provider, name
                        ORDER BY created_at DESC
                    ) AS rn
                FROM sync_configurations
            ) ranked
            WHERE rn > 1
        )
        """
    )

    # ------------------------------------------------------------------
    # 3. Dedupe scheduled_jobs: same strategy.
    # ------------------------------------------------------------------
    op.execute(
        """
        DELETE FROM scheduled_jobs
        WHERE id IN (
            SELECT id FROM (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY org_id, provider, name
                        ORDER BY created_at DESC
                    ) AS rn
                FROM scheduled_jobs
            ) ranked
            WHERE rn > 1
        )
        """
    )

    # ------------------------------------------------------------------
    # 4. Drop old unique constraints.
    # ------------------------------------------------------------------
    op.drop_constraint("uq_sync_config_org_name", "sync_configurations", type_="unique")
    op.drop_constraint("uq_scheduled_job_org_name", "scheduled_jobs", type_="unique")

    # ------------------------------------------------------------------
    # 5. Create new (org_id, provider, name) unique constraints.
    # ------------------------------------------------------------------
    op.create_unique_constraint(
        "uq_sync_config_org_provider_name",
        "sync_configurations",
        ["org_id", "provider", "name"],
    )
    op.create_unique_constraint(
        "uq_scheduled_job_org_provider_name",
        "scheduled_jobs",
        ["org_id", "provider", "name"],
    )


def downgrade() -> None:
    # ------------------------------------------------------------------
    # Reverse: drop new constraints, restore old ones, drop provider col.
    # Note: rows that were deduped during upgrade cannot be recovered.
    # ------------------------------------------------------------------
    op.drop_constraint(
        "uq_sync_config_org_provider_name", "sync_configurations", type_="unique"
    )
    op.drop_constraint(
        "uq_scheduled_job_org_provider_name", "scheduled_jobs", type_="unique"
    )

    # Re-dedupe for the narrower (org_id, name) key before restoring it.
    op.execute(
        """
        DELETE FROM sync_configurations
        WHERE id IN (
            SELECT id FROM (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY org_id, name
                        ORDER BY created_at DESC
                    ) AS rn
                FROM sync_configurations
            ) ranked
            WHERE rn > 1
        )
        """
    )
    op.execute(
        """
        DELETE FROM scheduled_jobs
        WHERE id IN (
            SELECT id FROM (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY org_id, name
                        ORDER BY created_at DESC
                    ) AS rn
                FROM scheduled_jobs
            ) ranked
            WHERE rn > 1
        )
        """
    )

    op.create_unique_constraint(
        "uq_sync_config_org_name",
        "sync_configurations",
        ["org_id", "name"],
    )
    op.create_unique_constraint(
        "uq_scheduled_job_org_name",
        "scheduled_jobs",
        ["org_id", "name"],
    )

    op.drop_column("scheduled_jobs", "provider")
