"""Index Ask Dev user and organization admission windows.

Revision ID: 0069
Revises: 0068

The message admission path counts active and recent runs while holding the
organization and user locks. These indexes keep that serialized transaction
bounded as retained run history grows.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0069"
down_revision: str | None = "0068"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_USER_WINDOW_INDEX = "ix_dev_runs_owner_started"
_ORG_WINDOW_INDEX = "ix_dev_runs_org_started"


def upgrade() -> None:
    op.create_index(
        _USER_WINDOW_INDEX,
        "dev_runs",
        ["org_id", "user_id", "started_at"],
        unique=False,
    )
    op.create_index(
        _ORG_WINDOW_INDEX,
        "dev_runs",
        ["org_id", "started_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(_ORG_WINDOW_INDEX, table_name="dev_runs")
    op.drop_index(_USER_WINDOW_INDEX, table_name="dev_runs")
