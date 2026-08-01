"""Add CHAOS-3302 health rule governance persistence.

Revision ID: 0077
Revises: 0076
Create Date: 2026-08-01 00:00:00

Renumbered 0076 -> 0077 (re-parented onto ``0076_add_sync_coverage_
projections.py``, PR #1365, which landed first on main and also claimed
revision 0076 on this application_schema branch): stacks on top of it
rather than re-forking the branch.

Two new, empty tables -- ``health_rule_calibrations`` and
``health_rule_version_fingerprints`` -- for the "rule-version telemetry
and persisted fingerprints" and "calibration record and owner decision
for each launch rule" deliverables. Both are code-owned governance
metadata, not tenant data: unlike the rest of the Ask Dev v2 persistence
schema (0074), neither is FK'd to ``dev_runs``/``organizations``/``users``,
because a ``HealthRuleDefinition`` is global, not scoped to a run, org, or
user.

Created empty, like 0074's own new tables, so their CHECK constraints need
no NOT VALID/VALIDATE split (that split matters only for a constraint
added to an existing, actively-written table -- see 0074/0075's own
docstrings).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0077"
down_revision: str | None = "0076"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)


def upgrade() -> None:
    op.create_table(
        "health_rule_calibrations",
        sa.Column("id", _UUID, nullable=False),
        sa.Column("calibration_id", sa.String(length=160), nullable=False),
        sa.Column("rule_id", sa.String(length=160), nullable=False),
        sa.Column("rule_version", sa.String(length=160), nullable=False),
        sa.Column("calibration_state", sa.String(length=32), nullable=False),
        sa.Column("sample_size", sa.Integer(), nullable=False),
        sa.Column("distribution_summary", sa.Text(), nullable=False),
        sa.Column("false_positive_review", sa.Text(), nullable=False),
        sa.Column("false_negative_review", sa.Text(), nullable=False),
        sa.Column("small_cohort_behavior", sa.Text(), nullable=False),
        sa.Column("owner", sa.String(length=256), nullable=False),
        sa.Column("decided_at", sa.Date(), nullable=False),
        sa.Column("evidence_ref", sa.String(length=160), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "calibration_state IN "
            "('provisional', 'product_approved', 'data_derived', 'policy_driven')",
            name="ck_health_rule_calibrations_state",
        ),
        sa.CheckConstraint(
            "sample_size >= 0", name="ck_health_rule_calibrations_sample_size"
        ),
        sa.CheckConstraint(
            "(calibration_state = 'provisional' AND evidence_ref IS NULL) OR "
            "(calibration_state != 'provisional' AND evidence_ref IS NOT NULL)",
            name="ck_health_rule_calibrations_evidence_ref",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("calibration_id", name="uq_health_rule_calibrations_id"),
    )
    op.create_index(
        "ix_health_rule_calibrations_rule",
        "health_rule_calibrations",
        ["rule_id", "rule_version"],
    )

    op.create_table(
        "health_rule_version_fingerprints",
        sa.Column("id", _UUID, nullable=False),
        sa.Column("rule_id", sa.String(length=160), nullable=False),
        sa.Column("rule_version", sa.String(length=160), nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "times_seen", sa.BigInteger(), nullable=False, server_default=sa.text("1")
        ),
        sa.CheckConstraint(
            "times_seen >= 1", name="ck_health_rule_version_fingerprints_times_seen"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "rule_id", "rule_version", name="uq_health_rule_version_fingerprints_rule"
        ),
    )


def downgrade() -> None:
    op.drop_table("health_rule_version_fingerprints")
    op.drop_index(
        "ix_health_rule_calibrations_rule", table_name="health_rule_calibrations"
    )
    op.drop_table("health_rule_calibrations")
