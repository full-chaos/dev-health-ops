"""Add dev_run_qua_shadow (CHAOS-3389 Question Understanding Agent shadow mode).

Revision ID: 0086
Revises: 0085
Create Date: 2026-08-05 00:00:00

Additive only: one new table, FK'd to ``dev_runs`` exactly like every other
Wave 3.1 per-run audit table (0074). Audit-only -- nothing in ``api/dev``
reads this table back to affect a live run; see
``dev_health_ops.api.dev.qua_shadow`` and
``dev_health_ops.models.dev_persistence.DevRunQuaShadow`` for the shape and
the non-influence guarantee.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0086"
down_revision: str | None = "0085"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

_STATUS_VALUES = (
    "'evaluated', 'skipped_disabled', 'skipped_no_provider', "
    "'skipped_no_mentions', 'skipped_budget_exhausted', "
    "'skipped_catalog_unavailable', 'skipped_timeout', "
    "'skipped_provider_error', 'skipped_unexpected_decision', "
    "'skipped_invalid_output'"
)


def _owner_fk() -> sa.ForeignKeyConstraint:
    return sa.ForeignKeyConstraint(
        ["run_id", "org_id", "user_id"],
        ["dev_runs.id", "dev_runs.org_id", "dev_runs.user_id"],
        name="fk_dev_run_qua_shadow_run_owner",
        ondelete="CASCADE",
    )


def upgrade() -> None:
    op.create_table(
        "dev_run_qua_shadow",
        sa.Column("id", _UUID, nullable=False),
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("org_id", _UUID, nullable=False),
        sa.Column("user_id", _UUID, nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("deterministic_decision", sa.String(length=16), nullable=False),
        sa.Column("cardinality_corroborated", sa.Boolean(), nullable=True),
        sa.Column("latency_ms", sa.Float(), nullable=False, server_default="0"),
        sa.Column("model_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("payload", _JSON, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            f"status IN ({_STATUS_VALUES})", name="ck_dev_run_qua_shadow_status"
        ),
        sa.CheckConstraint(
            "deterministic_decision IN ('proceed', 'terminate')",
            name="ck_dev_run_qua_shadow_deterministic_decision",
        ),
        _owner_fk(),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", name="uq_dev_run_qua_shadow_run"),
    )
    op.create_index(
        "ix_dev_run_qua_shadow_owner_run",
        "dev_run_qua_shadow",
        ["org_id", "user_id", "run_id"],
    )


def downgrade() -> None:
    # Codex adversarial review round 1 (HIGH, confirmed): an unconditional
    # drop here would silently erase real, already-persisted QUA shadow
    # evidence on a routine downgrade -- exactly the failure mode 0074's own
    # downgrade already refuses to allow for its Wave 3.1 tables. Same
    # guard, same posture: refuse (raise) rather than discard data. This
    # migration adds no other schema, so -- unlike 0074, which had to check
    # both a tagging column and several sibling tables -- one row-count
    # check on this table alone is the whole guard.
    bind = op.get_bind()
    if bind.dialect.has_table(bind, "dev_run_qua_shadow"):
        row_count = bind.execute(
            sa.select(sa.func.count()).select_from(sa.table("dev_run_qua_shadow"))
        ).scalar()
        if row_count:
            raise RuntimeError(
                f"refusing to downgrade 0086: dev_run_qua_shadow has "
                f"{row_count} row(s); this downgrade is for pre-release "
                "rehearsal only"
            )
    op.drop_table("dev_run_qua_shadow")
