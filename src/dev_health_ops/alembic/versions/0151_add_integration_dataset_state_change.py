"""Add ``integration_datasets.state_changed_at`` and ``state_changed_by`` -- when and by which code path a dataset was switched.

Revision ID: 0151
Revises: 0150

CHAOS-8818 (decision C7 of CHAOS-8795). Several code paths create a dataset row or
switch ``is_enabled`` (the settings save, the config create, the dataset endpoint, the
scheduler's security row and PagerDuty repair, the fixtures verb), and until now nothing
recorded which one acted or when, so "who switched this dataset on" had no answer.

Both columns are nullable with no default and no backfill: a row written before this
revision, or by a writer that does not stamp, keeps NULL, and NULL means "not recorded",
never "never changed". ``state_changed_by`` is the writer's name, not a user id.
The migration runs before the binaries that write the columns.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0151"
down_revision: str | None = "0150"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "integration_datasets"


def upgrade() -> None:
    op.add_column(
        _TABLE,
        sa.Column("state_changed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(_TABLE, sa.Column("state_changed_by", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column(_TABLE, "state_changed_by")
    op.drop_column(_TABLE, "state_changed_at")
