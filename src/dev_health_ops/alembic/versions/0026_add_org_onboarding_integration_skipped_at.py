"""Add organizations.onboarding_integration_skipped_at.

First-run onboarding (CHAOS-2670 / contract C6): records when an org explicitly
skipped the first-integration onboarding step so the onboarding-state API
(CHAOS-2673) and dashboard setup surfaces (CHAOS-2678) can distinguish "skipped"
from "not yet connected". Nullable timestamp; null = not skipped. The column add
is guarded for rerun safety (mirrors 0022).

Revision ID: 0026
Revises: 0025
Create Date: 2026-06-26 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0026"
down_revision: str | None = "0025"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    _add_column_if_missing(
        "organizations",
        sa.Column(
            "onboarding_integration_skipped_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    _drop_column_if_present("organizations", "onboarding_integration_skipped_at")


def _add_column_if_missing(table_name: str, column: sa.Column) -> None:
    existing_columns = _column_names(table_name)
    if column.name not in existing_columns:
        op.add_column(table_name, column)


def _drop_column_if_present(table_name: str, column_name: str) -> None:
    existing_columns = _column_names(table_name)
    if column_name in existing_columns:
        op.drop_column(table_name, column_name)


def _column_names(table_name: str) -> set[str]:
    bind = op.get_bind()
    return {column["name"] for column in sa.inspect(bind).get_columns(table_name)}
