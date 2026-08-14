"""Add fenced resumable provider-unit chunk persistence.

Revision ID: 0102
Revises: 0101
Create Date: 2026-08-14 00:00:00

The tables are additive. Existing atomic provider routes do not read them.
Checkpoint rows contain only bounded cursor and aggregate state. Normalized
sink-ready payloads live in one bounded sidecar row per ordinal and are
deleted with the owning unit after successful completion.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "0102"
down_revision: str | None = "0101"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_CHECKPOINTS = "sync_run_unit_chunk_checkpoints"
_CHUNKS = "sync_run_unit_effect_chunks"
_JSONB = sa.JSON().with_variant(JSONB(), "postgresql")


def upgrade() -> None:
    bind = op.get_bind()
    json_type = "jsonb_typeof" if bind.dialect.name == "postgresql" else "json_type"
    op.create_table(
        _CHECKPOINTS,
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("sync_run_unit_id", UUID(as_uuid=True), nullable=False),
        sa.Column("schema_version", sa.Text(), nullable=False, server_default="v1"),
        sa.Column("generation", sa.Text(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("dataset_key", sa.Text(), nullable=False),
        sa.Column("route_version", sa.Text(), nullable=False),
        sa.Column("normalized_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("next_cursor", sa.Text(), nullable=False, server_default=""),
        sa.Column(
            "inventory_complete",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("next_ordinal", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("prepared_chunks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_chunks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("final_ordinal", sa.Integer(), nullable=False, server_default="-1"),
        sa.Column("aggregate_result", _JSONB, nullable=True),
        sa.Column("aggregate_digest", sa.String(length=64), nullable=True),
        sa.Column("owner", sa.Text(), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "next_ordinal >= 0", name="ck_sync_chunk_checkpoint_next_ordinal"
        ),
        sa.CheckConstraint(
            "prepared_chunks >= 0", name="ck_sync_chunk_checkpoint_prepared_chunks"
        ),
        sa.CheckConstraint(
            "total_chunks >= 0", name="ck_sync_chunk_checkpoint_total_chunks"
        ),
        sa.CheckConstraint(
            "final_ordinal >= -1", name="ck_sync_chunk_checkpoint_final_ordinal"
        ),
        sa.CheckConstraint(
            "length(next_cursor) <= 4096", name="ck_sync_chunk_checkpoint_cursor"
        ),
        sa.CheckConstraint(
            "inventory_complete = false OR (total_chunks > 0 AND next_ordinal = total_chunks AND prepared_chunks = total_chunks)",
            name="ck_sync_chunk_checkpoint_complete_fence",
        ),
        sa.ForeignKeyConstraint(
            ["org_id", "sync_run_unit_id"],
            ["sync_run_units.org_id", "sync_run_units.id"],
            ondelete="CASCADE",
            name="fk_sync_chunk_checkpoint_tenant_unit",
        ),
        sa.PrimaryKeyConstraint("org_id", "sync_run_unit_id", "generation"),
    )
    op.create_index(
        "ix_sync_run_unit_chunk_checkpoints_unit",
        _CHECKPOINTS,
        ["org_id", "sync_run_unit_id"],
    )

    op.create_table(
        _CHUNKS,
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("sync_run_unit_id", UUID(as_uuid=True), nullable=False),
        sa.Column("schema_version", sa.Text(), nullable=False, server_default="v1"),
        sa.Column("generation", sa.Text(), nullable=False),
        sa.Column("route_version", sa.Text(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("total_chunks", sa.Integer(), nullable=False),
        sa.Column("cursor_before", sa.Text(), nullable=False, server_default=""),
        sa.Column("cursor_after", sa.Text(), nullable=False, server_default=""),
        sa.Column(
            "inventory_complete",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("payload", _JSONB, nullable=False),
        sa.Column("ledger", _JSONB, nullable=False),
        sa.Column("payload_bytes", sa.Integer(), nullable=False),
        sa.Column("manifest_digest", sa.String(length=64), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("ordinal >= 0", name="ck_sync_chunk_ordinal"),
        # total_chunks is zero until the streaming producer has observed the
        # end of inventory. Finalization fills the exact count in one
        # transaction before the checkpoint can publish a watermark.
        sa.CheckConstraint(
            "total_chunks = 0 OR ordinal < total_chunks", name="ck_sync_chunk_total"
        ),
        sa.CheckConstraint(
            "length(cursor_before) <= 4096 AND length(cursor_after) <= 4096",
            name="ck_sync_chunk_cursors",
        ),
        sa.CheckConstraint(
            "payload_bytes >= 1 AND payload_bytes <= 2097152",
            name="ck_sync_chunk_payload_bytes",
        ),
        sa.CheckConstraint(
            f"{json_type}(payload) = 'object'", name="ck_sync_chunk_payload_object"
        ),
        sa.CheckConstraint(
            f"{json_type}(ledger) = 'object'", name="ck_sync_chunk_ledger_object"
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'writing', 'committed')", name="ck_sync_chunk_status"
        ),
        sa.ForeignKeyConstraint(
            ["org_id", "sync_run_unit_id", "generation"],
            [
                _CHECKPOINTS + ".org_id",
                _CHECKPOINTS + ".sync_run_unit_id",
                _CHECKPOINTS + ".generation",
            ],
            ondelete="CASCADE",
            name="fk_sync_chunk_checkpoint",
        ),
        sa.PrimaryKeyConstraint("org_id", "sync_run_unit_id", "generation", "ordinal"),
    )
    op.create_index(
        "ix_sync_run_unit_effect_chunks_unit",
        _CHUNKS,
        ["org_id", "sync_run_unit_id", "generation", "ordinal"],
    )


def downgrade() -> None:
    op.drop_index("ix_sync_run_unit_effect_chunks_unit", table_name=_CHUNKS)
    op.drop_table(_CHUNKS)
    op.drop_index("ix_sync_run_unit_chunk_checkpoints_unit", table_name=_CHECKPOINTS)
    op.drop_table(_CHECKPOINTS)
