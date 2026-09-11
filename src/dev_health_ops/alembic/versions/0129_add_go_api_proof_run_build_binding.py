"""Add ``build_binding`` to ``go_api_proof_run``.

Revision ID: 0129
Revises: 0128

CHAOS-5484. 0128 recorded WHICH ROUTE measured a receipt. This records
how strongly that measurement bound the SERVING BUILD to the request --
the fact ``enable``'s new canary->primary rule leans on.

* ``per_request`` -- the response that was compared carried the serving
  build on itself (``x-dev-health-build``): ``/query/proof`` stamps it,
  and the Python edge passes it through (go_api_dispatcher.py's
  pass-through list, CHAOS-5479).
* ``absent`` -- nothing tied the build to THIS response: an edge response
  from a process or replica that did not carry the header.

Why ``absent`` and not ``run_level``. An earlier draft named the weak case
for the evidence behind it: the build WAS established for the run, by an
authenticated ``/buildinfo`` read before and after agreeing with every
routing row's ``current_candidate_build``. That is true -- and it is true
of every weak row, because under R70 ``VerifyCandidateBuild`` is a HARD
refusal, so no proof row is ever written without it. A ``run_level`` value
would therefore never distinguish one row from another, and a third value
would name a state no writer can produce. The column answers exactly one
question -- was the build bound PER RESPONSE? -- and the run-level
evidence is implied by the row existing at all and by ``candidate_build``
matching. (Team-lead ruling, 2026-09-10.)

Why a column rather than a derivation. Today the value is a function of
``measurement_route``: proof implies per-request, edge implies absent.
That makes the column look redundant, and it is -- until #2365 deletes
the Python edge and the edge route starts carrying the header too. On
that day the derivation silently becomes wrong, and every edge row
written before it becomes indistinguishable from a strongly-bound one.
Recorded now, the weak rows are named weak by the run that knew they were
weak. It is the same argument 0128 made for the explicit zero in
``differences_outside_baseline_defect``: a claim is worth writing down at
the moment the writer can still vouch for it.

Nullable, because every pre-0129 row genuinely carries no such assertion
and backfilling one from its route would manufacture exactly the evidence
this column exists to stop being assumed. Downgrade drops it.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0129"
down_revision: str | None = "0128"
branch_labels = None
depends_on = None

_TABLE = "go_api_proof_run"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("build_binding", sa.Text(), nullable=True))
    # Closed vocabulary, same reasoning as measurement_route's CHECK: a
    # binding outside these two cannot be interpreted, and a reader must
    # never have to guess whether an unknown value is stronger or weaker
    # than the ones the rule is written against. Two values, not three --
    # see the module docstring for why "run_level" is not one of them.
    op.create_check_constraint(
        "ck_go_api_proof_run_build_binding",
        _TABLE,
        "build_binding IS NULL OR build_binding IN ('per_request', 'absent')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_go_api_proof_run_build_binding", _TABLE, type_="check")
    op.drop_column(_TABLE, "build_binding")
