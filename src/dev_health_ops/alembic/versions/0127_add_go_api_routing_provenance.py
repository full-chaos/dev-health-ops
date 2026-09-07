"""Add review_evidence / recorded_by to the Go-API registry tables.

Revision ID: 0127
Revises: 0126

CHAOS-5446. A routing row is a DECISION -- "these operations now go to Go",
"this one goes back to Python" -- and until now the tables recorded only its
effect, never who made it or why.

That gap has already cost something concrete. On 2026-09-07 an operator
enabled all 15 registered operations with ``--acknowledge-unproven``, on an
explicit ruling, because no ``deployed_executed`` proof harness exists yet.
The rows record ``mode=canary`` and nothing else; the ruling that justified
them lived only in a chat message. Six weeks later the table cannot answer
"why is this on, and who decided?" -- the same complaint that produced the
UNPROVEN marker in ``dev-hops go-api routing status``.

Both tables get the pair, deliberately:

* ``go_api_proof_run``      -- who ran the proof, and against what review.
* ``go_api_routing_state``  -- who changed the mode, and why.

``recorded_by`` is separate from ``review_evidence`` on purpose: "who" must
be a resolved identity the tool fills in, not something inferred from prose
an operator happened to type.

Both columns are nullable on both tables, because every row written before
this migration genuinely has neither and backfilling a guess would be worse
than a NULL. Downgrade drops all four.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0127"
down_revision: str | None = "0126"
branch_labels = None
depends_on = None

_TABLES = ("go_api_proof_run", "go_api_routing_state")
_COLUMNS = ("review_evidence", "recorded_by")


def upgrade() -> None:
    for table in _TABLES:
        for column in _COLUMNS:
            op.add_column(table, sa.Column(column, sa.Text(), nullable=True))


def downgrade() -> None:
    for table in _TABLES:
        for column in _COLUMNS:
            op.drop_column(table, column)
