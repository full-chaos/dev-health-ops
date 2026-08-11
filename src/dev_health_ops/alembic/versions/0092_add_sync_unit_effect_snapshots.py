"""Add bounded prepared effect snapshots for exact Go worker recovery.

Revision ID: 0092
Revises: 0091
Create Date: 2026-08-04 00:00:00

The payload is an opaque, normalized, sink-ready manifest owned by the Go
provider worker. It deliberately does not reuse provider credential storage
or its encryption key. Tenant, unit, and generation form the durable identity;
the unit foreign key provides lifecycle cleanup if the owning run is deleted.

This revision was originally developed as 0088 on the feature branch. Main
subsequently assigned revisions 0088 through 0091, so the feature integration
merge renumbered it to 0092 and placed it directly after the current
application-schema head. The migration-head and duplicate-revision tests pin
that single lineage alongside the separate 0066 River cutover head.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0092"
down_revision: str | None = "0091"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "sync_run_unit_effect_snapshots"
_UNIT_INDEX = "ix_sync_run_unit_effect_snapshots_unit"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("sync_run_unit_id", UUID(as_uuid=True), nullable=False),
        sa.Column("generation", sa.Text(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("dataset_key", sa.Text(), nullable=False),
        sa.Column("schema_version", sa.Text(), nullable=False),
        sa.Column("content_digest", sa.String(length=64), nullable=False),
        sa.Column("payload_bytes", sa.Integer(), nullable=False),
        sa.Column("payload", sa.LargeBinary(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "payload_bytes >= 1 AND payload_bytes <= 67108864",
            name="ck_sync_run_unit_effect_snapshots_payload_bytes",
        ),
        sa.CheckConstraint(
            "length(payload) = payload_bytes",
            name="ck_sync_run_unit_effect_snapshots_payload_length",
        ),
        # 'v1' here is the PAYLOAD schema, which is a different version
        # namespace from the effect LEDGER schema stored under the
        # go_effect_ledger_v1 result key -- that one is at v2 once a snapshot
        # reference is present. The two advance independently: the ledger went
        # to v2 to carry the reference, while the payload's own shape has not
        # changed. Reading this constraint as "the snapshot feature is v1" and
        # bumping it to match the ledger would reject every row the current
        # encoder writes.
        sa.CheckConstraint(
            "schema_version = 'v1'",
            name="ck_sync_run_unit_effect_snapshots_schema_version",
        ),
        sa.ForeignKeyConstraint(
            ["sync_run_unit_id"], ["sync_run_units.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("org_id", "sync_run_unit_id", "generation"),
    )
    op.create_index(_UNIT_INDEX, _TABLE, ["sync_run_unit_id"])


def downgrade() -> None:
    op.drop_index(_UNIT_INDEX, table_name=_TABLE)
    op.drop_table(_TABLE)
