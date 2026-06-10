"""Add successor_jti column to refresh_tokens for rotation grace window.

Revision ID: 0008
Revises: 0007
Create Date: 2026-06-10 00:00:00

Adds a nullable text column ``successor_jti`` to ``refresh_tokens``.
When a token is rotated, the plain JTI (UUID string) of the newly created
successor token is written here.  A concurrent request that presents the
just-rotated (now revoked) parent token within the idempotency grace window
can read ``successor_jti`` and re-issue the same successor JWT instead of
incorrectly treating the near-simultaneous second presentation as malicious
token reuse and revoking the whole family.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    op.add_column(
        "refresh_tokens",
        sa.Column("successor_jti", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("refresh_tokens", "successor_jti")
