"""Widen dev_answer_frames.public_outcome for CHAOS-3541 (install, NOT VALID).

Revision ID: 0090
Revises: 0089
Create Date: 2026-08-07 00:00:02

CHAOS-3541: ``dev_answer_frames`` carries its OWN, separate mirror of the
closed ``PublicOutcome`` vocabulary (``ck_dev_answer_frames_public_outcome``)
-- distinct from ``dev_runs.public_outcome`` (widened in 0088/0089), enforced
by the DB independently for frame rows. Both must widen for
``PublicOutcome.REFUSED`` to be insertable at all: a run reaching the new
orchestrator branch calls ``record_frame`` before ``finish()``'s own
``update_run`` write (see orchestrator.py's ordering), so the FRAME table's
constraint is actually the first one a live request would hit.

Same NOT VALID / separate-VALIDATE split as 0088/0089 (which follow 0074/
0075's and 0081/0082's precedent) -- ``dev_answer_frames`` grows one row per
terminal run, the same unbounded-growth, actively-written profile as
``dev_runs``.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0090"
down_revision: str | None = "0089"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_CONSTRAINT = "ck_dev_answer_frames_public_outcome"
_TABLE = "dev_answer_frames"

_PRIOR_OUTCOMES = (
    "answered",
    "answered_with_gaps",
    "needs_clarification",
    "not_found",
    "temporarily_unavailable",
    "unsupported",
    "denied",
    "failed",
)
_NEW_OUTCOMES = ("refused",)


def _public_outcome_check(outcomes: Sequence[str]) -> str:
    values = ", ".join(f"'{outcome}'" for outcome in outcomes)
    return f"public_outcome IN ({values})"


def upgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.dialect.name == "postgresql"

    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    if is_postgres:
        # Constraint name/body are module-level literals; DDL cannot take bound parameters.
        op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            f"ALTER TABLE {_TABLE} ADD CONSTRAINT {_CONSTRAINT} CHECK "
            f"({_public_outcome_check((*_PRIOR_OUTCOMES, *_NEW_OUTCOMES))}) "
            "NOT VALID"
        )
    else:
        op.create_check_constraint(
            _CONSTRAINT,
            _TABLE,
            _public_outcome_check((*_PRIOR_OUTCOMES, *_NEW_OUTCOMES)),
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.has_table(bind, _TABLE):
        new_outcomes_list = ", ".join(f"'{outcome}'" for outcome in _NEW_OUTCOMES)
        new_outcome_rows = bind.execute(
            sa.text(
                f"SELECT count(*) FROM {_TABLE} WHERE public_outcome IN "
                f"({new_outcomes_list})"
            )
        ).scalar()
        if new_outcome_rows:
            raise RuntimeError(
                f"refusing to downgrade {revision}: {_TABLE} has "
                f"{new_outcome_rows} row(s) with public_outcome in "
                f"{_NEW_OUTCOMES} -- narrowing the CHECK constraint back "
                "would either abort on a violation or require this "
                "migration to delete/mutate that data, which is not this "
                "migration's call to make"
            )
    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    op.create_check_constraint(
        _CONSTRAINT, _TABLE, _public_outcome_check(_PRIOR_OUTCOMES)
    )
