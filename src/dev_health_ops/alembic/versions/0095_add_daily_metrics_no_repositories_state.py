"""Persist the explicit no-repository outcome for scheduled daily metrics.

Revision ID: 0095
Revises: 0094
Create Date: 2026-08-12 00:00:00

The nightly Go fan-out discovers repository identities only after the
coordinator transaction commits. An organization with no ClickHouse repository
rows is a valid, observable no-work outcome, not a completed metrics run.
"""

from __future__ import annotations

from alembic import op

revision: str = "0095"
down_revision: str | None = "0094"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "ck_daily_metrics_run_status", "daily_metrics_runs", type_="check"
    )
    op.create_check_constraint(
        "ck_daily_metrics_run_status",
        "daily_metrics_runs",
        "status IN ('pending', 'running', 'succeeded', 'failed', 'canceled', 'no_repositories')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_daily_metrics_run_status", "daily_metrics_runs", type_="check"
    )
    op.create_check_constraint(
        "ck_daily_metrics_run_status",
        "daily_metrics_runs",
        "status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')",
    )


__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]
