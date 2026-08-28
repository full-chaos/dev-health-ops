"""Add the Go API operation rollout registry and proof ledger (CHAOS-4366).

Revision ID: 0114
Revises: 0113
Create Date: 2026-08-28 00:00:00

Wave 0 of the Go API epic (CHAOS-4352) requires "proof infrastructure first"
before any GraphQL resolver is ported to Go (plan doc
``.github/docs-legacy/plans/go-api-epic.md`` §5/§6/§8.3). This migration
creates the three tables that back it:

* ``go_api_candidate_build`` -- immutable, append-only. One row per
  ``(schema_digest, document_digest, selected_operation, candidate_build)``.
* ``go_api_routing_state`` -- exactly one mutable row per
  ``(schema_digest, document_digest, selected_operation)``. What the request
  router reads on every call; a rollback mutates ``current_candidate_build``
  in place instead of an image rollback.
* ``go_api_proof_run`` -- one row per proof attempt (dual-run/
  deployed-executed/shadow/canary), pinned to the exact candidate build it
  proved via a full 4-column composite FK, never a bare ``candidate_build``
  string match.

Why three tables and not one: ``mode``/``rollout_percentage``/"which build
is current" all change as a rollout progresses, but a proof must stay
pinned to the exact build it proved -- a table that were both the
append-only proof target and the mutable routing decision could not do
both jobs safely. See ``src/dev_health_ops/models/go_api_registry.py`` for
the SQLAlchemy models these tables mirror and the fuller rationale.

Deliberately no data migration: this is new infrastructure, no prior
"operation registry" existed on the Python edge to backfill from.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

from dev_health_ops.models.go_api_registry import MODES, OWNERS, STAGES, TERMINAL_STATES

revision: str = "0114"
down_revision: str | None = "0113"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_CANDIDATE_BUILD = "go_api_candidate_build"
_ROUTING_STATE = "go_api_routing_state"
_PROOF_RUN = "go_api_proof_run"


def upgrade() -> None:
    op.create_table(
        _CANDIDATE_BUILD,
        sa.Column("schema_digest", sa.Text(), nullable=False),
        sa.Column("document_digest", sa.Text(), nullable=False),
        sa.Column("selected_operation", sa.Text(), nullable=False),
        sa.Column("candidate_build", sa.Text(), nullable=False),
        sa.Column(
            "registered_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint(
            "schema_digest",
            "document_digest",
            "selected_operation",
            "candidate_build",
            name="pk_go_api_candidate_build",
        ),
    )

    op.create_table(
        _ROUTING_STATE,
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

    op.create_table(
        _PROOF_RUN,
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("schema_digest", sa.Text(), nullable=False),
        sa.Column("document_digest", sa.Text(), nullable=False),
        sa.Column("selected_operation", sa.Text(), nullable=False),
        sa.Column("candidate_build", sa.Text(), nullable=False),
        sa.Column("request_identity", sa.Text(), nullable=False),
        sa.Column("stage", sa.Text(), nullable=False),
        sa.Column("terminal_state", sa.Text(), nullable=False),
        sa.Column("baseline_response_ref", sa.Text(), nullable=True),
        sa.Column("candidate_response_ref", sa.Text(), nullable=True),
        sa.Column("side_effect_digest", sa.Text(), nullable=True),
        sa.Column("data_watermark", sa.Text(), nullable=True),
        sa.Column("org_id", sa.Text(), nullable=True),
        sa.Column(
            "observed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name="pk_go_api_proof_run"),
        sa.ForeignKeyConstraint(
            [
                "schema_digest",
                "document_digest",
                "selected_operation",
                "candidate_build",
            ],
            [
                f"{_CANDIDATE_BUILD}.schema_digest",
                f"{_CANDIDATE_BUILD}.document_digest",
                f"{_CANDIDATE_BUILD}.selected_operation",
                f"{_CANDIDATE_BUILD}.candidate_build",
            ],
            name="fk_go_api_proof_run_candidate_build",
        ),
        sa.CheckConstraint(
            f"stage IN {STAGES!r}",
            name="ck_go_api_proof_run_stage",
        ),
        sa.CheckConstraint(
            f"terminal_state IN {TERMINAL_STATES!r}",
            name="ck_go_api_proof_run_terminal_state",
        ),
        sa.CheckConstraint(
            "stage <> 'shadow' OR data_watermark IS NOT NULL",
            name="ck_go_api_proof_run_shadow_requires_watermark",
        ),
    )
    op.create_index(
        "ix_go_api_proof_run_operation",
        _PROOF_RUN,
        ["schema_digest", "document_digest", "selected_operation", "observed_at"],
    )
    op.create_index(
        "ix_go_api_proof_run_candidate_build",
        _PROOF_RUN,
        ["schema_digest", "document_digest", "selected_operation", "candidate_build"],
    )


def downgrade() -> None:
    op.drop_index("ix_go_api_proof_run_candidate_build", table_name=_PROOF_RUN)
    op.drop_index("ix_go_api_proof_run_operation", table_name=_PROOF_RUN)
    op.drop_table(_PROOF_RUN)
    op.drop_table(_ROUTING_STATE)
    op.drop_table(_CANDIDATE_BUILD)
