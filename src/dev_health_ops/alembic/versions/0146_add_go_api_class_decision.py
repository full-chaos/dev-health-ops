"""Add ``go_api_class_decision`` -- the MCP class root decision, keyed by operation only.

Revision ID: 0146
Revises: 0145

CHAOS-8735 (owner ruling D4797). The decision for an MCP class root (is ``mcp:<root>`` served, held in
shadow for the proof route, or dark) used to live in ``go_api_routing_state`` rows keyed
``(schema_digest, document_digest, selected_operation)``. The switch, ``status``, ``enable``/``disable``/
``seed``, ``repoint`` and ``prove`` each read a different slice of that digest-keyed set, so any rule that
ordered the rows (any digest, newest) left one reader on the old slice. This table holds ONE row per
class operation, so the digest cannot matter by construction: ``decided_at`` is set by the database only
when ``enable``/``disable``/``seed`` change the mode, and ``repoint`` rewrites ``current_candidate_build``
and nothing else.

**Backfill.** Each class operation gets the NEWEST of its ``go_api_routing_state`` rows (``updated_at``).
On an equal timestamp the pick is deterministic and fails closed: a row that is not in a served mode
(canary or primary) beats one that is, then the greater ``schema_digest``. The source rows are left in
place (not deleted), so rolling the build back to one that reads them still finds them; nothing reads them
after this change and ``go_api_routing_state`` is dropped separately.

**No foreign key** to ``go_api_candidate_build``: that table is keyed by schema digest, which is exactly
what this table must not depend on. ``schema_digest`` here is the live digest of the verb run that wrote
the row, kept for the audit reader and never read to decide.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0146"
down_revision: str | None = "0145"
branch_labels = None
depends_on = None

_TABLE = "go_api_class_decision"

#: The modes of go_api_routing_state (0114): the same vocabulary, so a backfilled row is valid as it is.
MODES = ("python", "shadow", "canary", "primary", "disabled")


def _quoted(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("operation", sa.Text(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("current_candidate_build", sa.Text(), nullable=False),
        sa.Column("schema_digest", sa.Text(), nullable=False),
        sa.Column("review_evidence", sa.Text(), nullable=True),
        sa.Column("recorded_by", sa.Text(), nullable=True),
        sa.Column(
            "decided_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("operation"),
        sa.CheckConstraint(
            f"mode IN ({_quoted(MODES)})",
            name="ck_go_api_class_decision_mode",
        ),
        sa.CheckConstraint(
            "left(operation, 4) = 'mcp:'",
            name="ck_go_api_class_decision_operation",
        ),
    )
    op.execute(
        """
        INSERT INTO go_api_class_decision
            (operation, mode, current_candidate_build, schema_digest, review_evidence, recorded_by, decided_at)
        SELECT DISTINCT ON (selected_operation)
               selected_operation, mode, current_candidate_build, schema_digest,
               review_evidence, recorded_by, updated_at
          FROM go_api_routing_state
         WHERE left(selected_operation, 4) = 'mcp:'
         ORDER BY selected_operation,
                  updated_at DESC,
                  (mode IN ('canary', 'primary')) ASC,
                  schema_digest DESC
        """
    )


def downgrade() -> None:
    op.drop_table(_TABLE)
