"""Widen dev_runs.state for the CHAOS-3292 preflight and add run diagnostics.

Revision ID: 0072
Revises: 0071
Create Date: 2026-07-31 00:00:00

The subject preflight adds two non-terminal run states (``interpreting`` and
``resolving_subjects``) between scope authorization and the first model round.
``ck_dev_runs_state`` enumerates the legal states, so writing either one fails
at persistence without this revision — the constraint is checked by the
database, not by an import.

Two nullable, content-free diagnostic columns land with them. There is no
metrics, logging, tracing or statsd facility anywhere in ``api/dev`` to publish
a counter to (a real observability stack is CHAOS-3218), so the preflight
outcome and the demoted CHAOS-3289 backstop's reason code ride on the run row
beside the existing ``safe_error_code`` / ``grounding_validation_status``
diagnostics, under the same retention policy. Both are closed vocabularies:
neither can carry question text, an entity name, or catalog content.

Additive in both directions. It touches nothing the CHAOS-3299 persistence
work owns.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0072"
down_revision: str | None = "0071"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_CONSTRAINT = "ck_dev_runs_state"
_TABLE = "dev_runs"

_PRIOR_STATES = (
    "accepted",
    "resolving_scope",
    "model_decision",
    "tool_validation",
    "tool_execution",
    "answer_validation",
    "completed",
    "insufficient_evidence",
    "refused",
    "failed",
    "cancelled",
)
_NEW_STATES = ("interpreting", "resolving_subjects")


def _state_check(states: Sequence[str]) -> str:
    values = ", ".join(f"'{state}'" for state in states)
    return f"state IN ({values})"


def upgrade() -> None:
    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    op.create_check_constraint(
        _CONSTRAINT,
        _TABLE,
        _state_check(
            (
                *_PRIOR_STATES[:2],
                *_NEW_STATES,
                *_PRIOR_STATES[2:],
            )
        ),
    )
    op.add_column(
        _TABLE, sa.Column("preflight_outcome", sa.String(length=32), nullable=True)
    )
    op.add_column(
        _TABLE, sa.Column("legacy_guard_reason", sa.String(length=64), nullable=True)
    )


def downgrade() -> None:
    op.drop_column(_TABLE, "legacy_guard_reason")
    op.drop_column(_TABLE, "preflight_outcome")
    # A run left in one of the new states would violate the narrowed
    # constraint, so clear those rows to the nearest legal predecessor rather
    # than failing the rollback on live data.
    op.execute(
        sa.text(
            "UPDATE dev_runs SET state = 'resolving_scope' "
            "WHERE state IN ('interpreting', 'resolving_subjects')"
        )
    )
    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    op.create_check_constraint(_CONSTRAINT, _TABLE, _state_check(_PRIOR_STATES))
