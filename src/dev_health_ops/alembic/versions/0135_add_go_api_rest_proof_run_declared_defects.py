"""Add go_api_rest_proof_run.baseline_defect_declared.

Revision ID: 0135
Revises: 0134

``go_api_rest_proof_run.baseline_defect`` (0134) names which declared
tickets a comparison actually MATCHED that run. It carries no record of
which tickets were DECLARED for that request that run -- only which of
them, if any, covered a real difference. A reader of a past row can
therefore tell a citation FIRED, but cannot tell a silent citation was
declared-and-quiet from not-yet-declared-at-all: both read identically,
absent from a column that was never asked the question.

``baseline_defect_declared`` answers it: the full set of tickets this
request's own corpus entry declared at the moment this receipt was
written, alongside (never instead of) the existing matched-only column.
Nullable, no backfill, no CHECK constraint (mirrors ``baseline_defect``'s
own, un-constrained shape) -- every row written before this column
existed carries NULL here, meaning UNKNOWN, not "declared nothing". A
NULL row must never be read as evidence either way; the Go-side
determination that reads this column treats NULL as excluded from
consideration entirely, counting neither toward "known live" nor toward
"known silent". Every row written after this column exists carries a
real array, possibly empty, so "known" is exact from here forward.

Idempotent: re-running ``upgrade()`` against a database that already has
the column is a no-op, not a second attempt that fails on a duplicate
column -- checked via a live introspection of the table, not assumed from
the revision history, so this migration is safe to run through the
existing migrate hook more than once against the same database.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0135"
down_revision: str | None = "0134"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "go_api_rest_proof_run"
_COLUMN = "baseline_defect_declared"


def _has_column(conn: sa.Connection, table: str, column: str) -> bool:
    inspector = sa.inspect(conn)
    if table not in inspector.get_table_names():
        # The table itself does not exist yet on this database (0134 has
        # not been applied). Nothing for THIS migration to add; 0134's
        # own upgrade is what creates the table, and alembic will not
        # reach 0135 before 0134 on a normal chained upgrade.
        return True
    return any(col["name"] == column for col in inspector.get_columns(table))


def upgrade() -> None:
    conn = op.get_bind()
    if _has_column(conn, _TABLE, _COLUMN):
        return
    op.add_column(
        _TABLE,
        sa.Column(_COLUMN, sa.ARRAY(sa.Text()), nullable=True),
    )


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    if _TABLE not in inspector.get_table_names():
        return
    if any(col["name"] == _COLUMN for col in inspector.get_columns(_TABLE)):
        op.drop_column(_TABLE, _COLUMN)
