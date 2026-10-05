"""Add ``daily_metrics_runs.full_org`` -- whether a run computes the whole organization.

Revision ID: 0147
Revises: 0146

CHAOS-8710. The ClickHouse daily run marker may certify an (organization, day) only from a run that
computes every repository of the organization. That scope was never stored: a run started with an
explicit repository list inserts its partitions in the creating transaction, a run started without one
(the scheduled fan-out, the post-sync run, a manual run with no ``--repo-id``, the external-recompute
all-repository fallback) discovers the repository set later. The column records it at creation:
``true`` when no explicit repository list was given.

**Backfill.** Only the scheduled fan-out generation prefix becomes ``true``: that run was always the
nightly organization-wide fan-out. A ``post-sync:`` row stays ``false``. The post-sync creation site
stopped passing a repository list on 2026-08-25 (CHAOS-4263, commit 698271e7c "Resolve post-sync
daily-metrics repositories through live ClickHouse discovery"); a post-sync run created before that
carried the triggering sync's repository ids, and no stored field says which kind an old row is, so the
history cannot prove an old row org-wide. ``false`` means "never certifies a day", the safe side. A manual
or external-recompute run of the past is likewise unknowable and stays ``false``. New code writes the
column on every insert (a post-sync run created now has no repository list, so it is ``true``); an older
build that does not leaves the default ``false``.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0147"
down_revision: str | None = "0146"
branch_labels = None
depends_on = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "daily_metrics_runs"


def upgrade() -> None:
    op.add_column(
        _TABLE,
        sa.Column("full_org", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.execute(
        sa.text(
            "UPDATE daily_metrics_runs SET full_org = true "
            "WHERE starts_with(generation, 'fixed-schedule:daily_metrics_fanout:')"
        )
    )


def downgrade() -> None:
    op.drop_column(_TABLE, "full_org")
