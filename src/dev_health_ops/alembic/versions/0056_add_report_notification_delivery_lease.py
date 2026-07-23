"""Make report-ready notification claims recoverable after worker loss.

Revision ID: 0056
Revises: 0055
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from dev_health_ops.models.git import GUID

revision: str = "0056"
down_revision: str | None = "0055"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "report_runs", sa.Column("notification_claim_token", GUID(), nullable=True)
    )
    op.add_column(
        "report_runs",
        sa.Column(
            "notification_lease_expires_at", sa.DateTime(timezone=True), nullable=True
        ),
    )
    # Existing delivering rows predate the fencing token. They are by
    # definition not safely owned by a running lease, so make them immediately
    # reclaimable rather than preserving the historical permanent stranding.
    op.execute(
        "UPDATE report_runs SET notification_lease_expires_at = CURRENT_TIMESTAMP "
        "WHERE notification_status = 'delivering'"
    )
    op.create_index(
        "ix_report_runs_notification_reclaim",
        "report_runs",
        ["notification_status", "notification_lease_expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_report_runs_notification_reclaim", table_name="report_runs")
    op.drop_column("report_runs", "notification_lease_expires_at")
    op.drop_column("report_runs", "notification_claim_token")
