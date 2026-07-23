"""Add durable state for remaining metric family partitions.

Revision ID: 0058_add_remaining_metric_river_state
Revises: 0057_add_daily_metrics_river_state
"""

from __future__ import annotations

from alembic import op

revision: str = "0058"
down_revision: str | None = "0057"
branch_labels = None
depends_on = None

_FAMILIES = (
    "capacity",
    "complexity",
    "dora",
    "extra_metrics",
    "membership_backfill",
    "recommendations",
    "release_impact",
    "team_metrics",
)


def upgrade() -> None:
    families = ", ".join(f"'{family}'" for family in _FAMILIES)
    op.execute(
        f"""
        CREATE TABLE remaining_metric_runs (
            id uuid PRIMARY KEY,
            org_id uuid NOT NULL,
            family text NOT NULL CHECK (family IN ({families})),
            generation text NOT NULL CHECK (length(generation) BETWEEN 1 AND 128),
            scope_key text NOT NULL CHECK (length(scope_key) BETWEEN 1 AND 512),
            generation_seed bigint NULL,
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending','running','succeeded','failed','canceled')),
            canceled_at timestamptz NULL,
            created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            UNIQUE (org_id, family, generation, scope_key)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE remaining_metric_partitions (
            id uuid PRIMARY KEY,
            run_id uuid NOT NULL REFERENCES remaining_metric_runs(id) ON DELETE CASCADE,
            ordinal integer NOT NULL CHECK (ordinal >= 1),
            scope jsonb NOT NULL,
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending','running','succeeded','failed','canceled')),
            claim_token uuid NULL,
            lease_expires_at timestamptz NULL,
            attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
            output_evidence text NULL CHECK (
                output_evidence IS NULL OR length(output_evidence) BETWEEN 1 AND 4096
            ),
            completed_at timestamptz NULL,
            created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            UNIQUE (run_id, ordinal),
            CHECK (
                (status = 'running' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)
                OR
                (status <> 'running' AND claim_token IS NULL AND lease_expires_at IS NULL)
            )
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_remaining_metric_partitions_claim
        ON remaining_metric_partitions (status, lease_expires_at, run_id, ordinal)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS remaining_metric_partitions")
    op.execute("DROP TABLE IF EXISTS remaining_metric_runs")
