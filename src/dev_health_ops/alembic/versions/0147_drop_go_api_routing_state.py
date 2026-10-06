"""Drop ``go_api_routing_state``.

Revision ID: 0147
Revises: 0146

CHAOS-8706. query-api serves every registered operation without a routing row (CHAOS-8702), and an MCP class
root is decided by ``go_api_class_decision`` (0146, CHAOS-8735). After CHAOS-8705 no code reads or writes
``go_api_routing_state``; this revision drops the table with its constraints.

**Rollback window.** The 0146 backfill left the old rows in place so that a build older than 0146 could still
read them. This revision ends that: after it, a pre-0146 image (one whose switch reads ``go_api_routing_state``)
fails every request that reads the table. Roll back only to a build that does not read it.

**Downgrade** recreates the table EMPTY with its final shape (0114 plus the provenance columns of 0127). The rows
are not restored: the class decisions live in ``go_api_class_decision``, and a catalog operation has no routing
state to restore. ``go_api_candidate_build`` is untouched (proof receipts still foreign-key to it).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0147"
down_revision: str | None = "0146"
branch_labels = None
depends_on = None

_TABLE = "go_api_routing_state"
_CANDIDATE_BUILD = "go_api_candidate_build"

#: Frozen at this revision, as in 0114: a migration must not read a live model vocabulary.
OWNERS = ("python", "go")
MODES = ("python", "shadow", "canary", "primary", "disabled")


def upgrade() -> None:
    op.drop_table(_TABLE)


def downgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("schema_digest", sa.Text(), nullable=False),
        sa.Column("document_digest", sa.Text(), nullable=False),
        sa.Column("selected_operation", sa.Text(), nullable=False),
        sa.Column("current_candidate_build", sa.Text(), nullable=False),
        sa.Column("owner", sa.Text(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False, server_default="python"),
        sa.Column("eligible_orgs", sa.JSON(), nullable=True),
        sa.Column(
            "rollout_percentage", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("review_evidence", sa.Text(), nullable=True),
        sa.Column("recorded_by", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint(
            "schema_digest",
            "document_digest",
            "selected_operation",
            name="pk_go_api_routing_state",
        ),
        sa.ForeignKeyConstraint(
            [
                "schema_digest",
                "document_digest",
                "selected_operation",
                "current_candidate_build",
            ],
            [
                f"{_CANDIDATE_BUILD}.schema_digest",
                f"{_CANDIDATE_BUILD}.document_digest",
                f"{_CANDIDATE_BUILD}.selected_operation",
                f"{_CANDIDATE_BUILD}.candidate_build",
            ],
            name="fk_go_api_routing_state_candidate_build",
        ),
        sa.CheckConstraint(
            f"owner IN {OWNERS!r}",
            name="ck_go_api_routing_state_owner",
        ),
        sa.CheckConstraint(
            f"mode IN {MODES!r}",
            name="ck_go_api_routing_state_mode",
        ),
        sa.CheckConstraint(
            "rollout_percentage >= 0 AND rollout_percentage <= 100",
            name="ck_go_api_routing_state_rollout_percentage",
        ),
    )
