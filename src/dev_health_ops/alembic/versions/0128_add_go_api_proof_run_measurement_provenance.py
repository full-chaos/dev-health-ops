"""Add measurement provenance to ``go_api_proof_run``.

Revision ID: 0128
Revises: 0127

CHAOS-5425. The ``prove`` verb (``cmd/go-api-prove``) writes the first
``stage='deployed_executed'`` receipts this system has ever had. Three
facts about a receipt are load-bearing and had nowhere to live:

* ``measurement_route`` -- ``edge`` or ``proof``. A canary/primary
  operation is measured through the real product edge; a shadow-mode one
  can only be measured through ``/query/proof``, the measurement-only
  handler that ``PostgresSwitch``'s canary|primary-only reachability
  otherwise makes impossible. Both are legitimate evidence and they are
  NOT the same claim -- a proof-route observation must never be read as
  served traffic. Without this column that distinction survives only in a
  chat message, which is exactly how the ruling behind the 2026-09-07
  ``--acknowledge-unproven`` enablement was lost (see 0127).

* ``baseline_defect`` -- the tickets whose declared field paths cover this
  comparison's differences, for divergences where PYTHON is wrong and Go
  is right: CHAOS-5448 (Python omits FINAL on ``work_item_cycle_times``
  and counts superseded row versions) and CHAOS-5450 (Python's list path
  emits a naive timestamp). This annotates a mismatch; it never converts
  one. ``terminal_state`` stays ``mismatch`` and the operation is not
  promoted -- the column exists so "we know why" is recorded WITHOUT
  becoming "it passed".

* ``differences_outside_baseline_defect`` -- how many differences NO
  declared defect covers. NOT NULL with a server default of 0 rather than
  nullable, because the explicit zero IS the claim: "every difference here
  is a known Python defect" and "there were no differences" are different
  facts, and a NULL would let a reader guess which one happened.

``measurement_route`` and ``baseline_defect`` are nullable: every row
written before this migration has neither, and backfilling a guess about
how an older proof was measured would be worse than a NULL. Downgrade
drops all three.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0128"
down_revision: str | None = "0127"
branch_labels = None
depends_on = None

_TABLE = "go_api_proof_run"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("measurement_route", sa.Text(), nullable=True))
    op.add_column(
        _TABLE, sa.Column("baseline_defect", sa.ARRAY(sa.Text()), nullable=True)
    )
    op.add_column(
        _TABLE,
        sa.Column(
            "differences_outside_baseline_defect",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    # The vocabulary is closed and small, and a receipt whose route is
    # neither of these cannot be interpreted at all -- the same reason
    # stage and terminal_state carry CHECKs rather than being trusted to
    # application code. NULL stays legal for the pre-0128 rows.
    op.create_check_constraint(
        "ck_go_api_proof_run_measurement_route",
        _TABLE,
        "measurement_route IS NULL OR measurement_route IN ('edge', 'proof')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_go_api_proof_run_measurement_route", _TABLE, type_="check")
    op.drop_column(_TABLE, "differences_outside_baseline_defect")
    op.drop_column(_TABLE, "baseline_defect")
    op.drop_column(_TABLE, "measurement_route")
