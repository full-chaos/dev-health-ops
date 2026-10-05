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

**Backfill (D4819).** Each class operation gets exactly the ``go_api_routing_state`` row the RUNNING (old)
image serves: the row at that image's schema digest under the class document digest. That is the table's primary
key, so there is at most one row per operation and no timestamp or tie-break takes part (a newer row at another
digest, which the old image never read, is never copied). The old image's digest comes from the environment,
``DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST`` (the roll reads it from the old image's ``GET /registry``), set as the
session setting ``dho.class_decision_live_schema_digest`` -- the same setting ``dho migrate postgres upgrade``
sets. An operation with no row at that digest gets no decision: dark, as the old image answers. With class rows
present, a missing or malformed digest, or a digest that holds no class row, fails the upgrade (the walk rolls back
whole). The source rows are left in place (not deleted), so rolling the build back to one that reads them still
finds them; nothing reads them after this change and ``go_api_routing_state`` is dropped separately.

**No foreign key** to ``go_api_candidate_build``: that table is keyed by schema digest, which is exactly
what this table must not depend on. ``schema_digest`` here is the live digest of the verb run that wrote
the row, kept for the audit reader and never read to decide.
"""

from __future__ import annotations

import os

import sqlalchemy as sa
from alembic import op

revision: str = "0146"
down_revision: str | None = "0145"
branch_labels = None
depends_on = None

_TABLE = "go_api_class_decision"

#: The environment variable and session setting that carry the old image's live schema digest (D4819).
LIVE_DIGEST_ENV = "DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST"
LIVE_DIGEST_SETTING = "dho.class_decision_live_schema_digest"

#: The MCP class document digest (internal/mcpclass DocumentDigest(): sha256 of its DocumentKey).
CLASS_DOCUMENT_DIGEST = (
    "9c509c3594856bed7f4896345d687c0fca3a1e298b65b440ae519938bac7ed92"
)

#: Refuses the backfill, with class rows present, when the live digest is unset, empty or malformed, or holds no class
#: row under the class document digest. Static text (a DO block takes no bind parameters), the chain file's own.
_CLASS_ROW_GUARD = """
DO $$
DECLARE
    live_digest text := coalesce(current_setting('dho.class_decision_live_schema_digest', true), '');
    class_rows bigint;
    live_rows bigint;
BEGIN
    SELECT count(*) INTO class_rows FROM go_api_routing_state WHERE left(selected_operation, 4) = 'mcp:';
    IF class_rows = 0 THEN
        RETURN;
    END IF;
    IF live_digest !~ '^sha256:[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'go_api_class_decision backfill: % MCP class rows exist and dho.class_decision_live_schema_digest is empty or not sha256:<64 hex>; set DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST to the schema_digest of the running query-api (GET /registry)', class_rows;
    END IF;
    SELECT count(*) INTO live_rows FROM go_api_routing_state
     WHERE left(selected_operation, 4) = 'mcp:'
       AND schema_digest = live_digest
       AND document_digest = '9c509c3594856bed7f4896345d687c0fca3a1e298b65b440ae519938bac7ed92';
    IF live_rows = 0 THEN
        RAISE EXCEPTION 'go_api_class_decision backfill: % MCP class rows exist and none is at schema digest % under the class document digest', class_rows, live_digest;
    END IF;
END
$$
"""

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
    live_digest = os.environ.get(LIVE_DIGEST_ENV)
    if live_digest is not None:
        op.execute(
            sa.text("SELECT set_config(:name, :value, true)").bindparams(
                name=LIVE_DIGEST_SETTING, value=live_digest
            )
        )
    # A DO block takes no bind parameters, so the guard is a static statement: the same text as the chain file's
    # (internal/pgmigrate/sql/0146_add_go_api_class_decision.sql), which reads the live digest from the setting above.
    op.execute(_CLASS_ROW_GUARD)
    op.execute(
        sa.text(
            """
            INSERT INTO go_api_class_decision
                (operation, mode, current_candidate_build, schema_digest, review_evidence, recorded_by, decided_at)
            SELECT selected_operation, mode, current_candidate_build, schema_digest,
                   review_evidence, recorded_by, updated_at
              FROM go_api_routing_state
             WHERE left(selected_operation, 4) = 'mcp:'
               AND schema_digest = current_setting(:live_digest_setting, true)
               AND document_digest = :class_document_digest
            """
        ).bindparams(
            live_digest_setting=LIVE_DIGEST_SETTING,
            class_document_digest=CLASS_DOCUMENT_DIGEST,
        )
    )


def downgrade() -> None:
    op.drop_table(_TABLE)
