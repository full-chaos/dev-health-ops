"""Add durable per-kind worker job route control.

Revision ID: 0055
Revises: 0054
"""

from __future__ import annotations

from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

revision = "0055"
down_revision = "0054"
branch_labels = None
depends_on = None

_KINDS = (
    "operational.billing_notification",
    "operational.webhook_delivery",
    "report.execute_on_demand",
    "report.execute_scheduled",
    "system.heartbeat",
    "system.retention_cleanup",
)


def upgrade() -> None:
    now = datetime.now(UTC)
    op.create_table(
        "worker_job_routes",
        sa.Column("job_kind", sa.String(length=96), nullable=False),
        sa.Column("transport", sa.String(length=16), nullable=False),
        sa.Column("paused", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("generation", sa.BigInteger(), nullable=False, server_default="1"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "transport IN ('celery', 'shadow', 'river_canary', 'river')",
            name="ck_worker_job_route_transport",
        ),
        sa.CheckConstraint("generation >= 1", name="ck_worker_job_route_generation"),
        sa.PrimaryKeyConstraint("job_kind"),
    )
    routes = sa.table(
        "worker_job_routes",
        sa.column("job_kind", sa.String()),
        sa.column("transport", sa.String()),
        sa.column("paused", sa.Boolean()),
        sa.column("generation", sa.BigInteger()),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    op.bulk_insert(
        routes,
        [
            {
                "job_kind": kind,
                "transport": "celery",
                "paused": False,
                "generation": 1,
                "updated_at": now,
            }
            for kind in _KINDS
        ],
    )


def downgrade() -> None:
    op.drop_table("worker_job_routes")
