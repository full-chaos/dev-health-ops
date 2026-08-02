"""Reserve narrative mode/failure-code columns on dev_runs (CHAOS-3297 stack #1).

Revision ID: 0078
Revises: 0077
Create Date: 2026-08-01 00:00:00

CHAOS-3297's plan of record calls this "0078_add_ask_dev_frame_synthesis_state.py"
with two purposes: (1) new orchestrator stage run states
(``synthesizing_frame``/``narrating``) and (2) narrative mode / safe
narrative-failure code on ``dev_runs``. The plan itself pre-authorizes
dropping (1) if review prefers zero new run states before anything actually
transitions into them: "If review prefers zero new run states, item 1
drops and 0078 shrinks to item 2; it does not disappear."

Stack #1 (this changeset) does not drive any synthesis/narration stage --
the pre-CHAOS-3295 model-tool-choice loop still runs through the existing
``model_decision``/``tool_execution`` states unchanged, and no code
anywhere transitions into ``synthesizing_frame``/``narrating``. Shipping
those states now would be dead run-state vocabulary with zero reachable
transitions until stack #3/#4 land. So this revision carries only item 2;
CHAOS-3297 stacks #3-5 (or a dedicated migration) add the states + the
``ck_dev_runs_state`` widening when something actually writes them.

No CHECK constraint on either new column, matching 0072's precedent for
the same kind of diagnostic-only nullable string column
(``preflight_outcome``/``legacy_guard_reason``): the closed vocabulary for
``narrative_mode`` (``provider``/``deterministic_fallback``, mirroring
``ck_dev_run_narratives_mode``) exists today but nothing in stack #1 ever
writes a value (no narrative synthesis exists yet); ``narrative_failure_code``'s
vocabulary is CHAOS-3297 stack #4's (narrative-fallback) to define. Both
stay NULL for every run stack #1 produces.

Additive in both directions.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0078"
down_revision: str | None = "0077"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "dev_runs"


def upgrade() -> None:
    op.add_column(
        _TABLE, sa.Column("narrative_mode", sa.String(length=24), nullable=True)
    )
    op.add_column(
        _TABLE,
        sa.Column("narrative_failure_code", sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column(_TABLE, "narrative_failure_code")
    op.drop_column(_TABLE, "narrative_mode")
