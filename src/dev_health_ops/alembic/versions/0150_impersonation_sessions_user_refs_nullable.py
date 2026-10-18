"""Let ``impersonation_sessions`` keep its row when a user is permanently deleted.

Revision ID: 0150
Revises: 0149

CHAOS-10019 (D6051). A permanent user delete keeps the impersonation audit rows (a security record) and
sets the user reference to null. ``admin_user_id`` and ``target_user_id`` were NOT NULL, so the delete could
not keep the row. Both columns become nullable; the foreign keys (RESTRICT) and the not-self check stay (a
check on a null column passes). The downgrade sets NOT NULL again and fails while a null reference exists:
those rows are the record, and the downgrade does not delete it.
"""

from __future__ import annotations

from alembic import op

revision: str = "0150"
down_revision: str | None = "0149"
branch_labels = None
depends_on = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "impersonation_sessions"


def upgrade() -> None:
    op.alter_column(_TABLE, "admin_user_id", nullable=True)
    op.alter_column(_TABLE, "target_user_id", nullable=True)


def downgrade() -> None:
    op.alter_column(_TABLE, "target_user_id", nullable=False)
    op.alter_column(_TABLE, "admin_user_id", nullable=False)
