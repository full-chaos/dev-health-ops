"""Add external_ingest_batches, external_ingest_rejections, and
external_ingest_batch_payloads (CHAOS-2694).

Durable Postgres status store for customer-push ingestion batches
(CHAOS-2690). Postgres, not ClickHouse -- deliberately, mirroring the
provider_rate_limit_observations precedent (0031): transactional, joins the
ingest-token/source-registration model (CHAOS-2696) in the same database, and
must support a strongly-consistent read-after-write status for CLI polling
(dev-hops push batch --poll, CHAOS-2700) immediately after 202 Accepted.

Unlike provider_rate_limit_observations, external_ingest_rejections IS a
child of external_ingest_batches (FK, ON DELETE CASCADE): rejection rows have
no independent retention requirement, so a single prune sweep on the parent
table is sufficient (see workers/external_ingest_reconciler.py).

external_ingest_batch_payloads is CHAOS-2693's transient raw-payload table --
hosted here (DDL + declarative model) so wave 3 (2693) needs no migration of
its own; 2693 owns the payload_store.py accessors and orphan-prune sweep.

Retry-safe / guarded per the 0025/0020/0031/0032 create-if-missing convention.

Revision ID: 0033
Revises: 0032
Create Date: 2026-07-02 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0033"
down_revision: str | None = "0032"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_BATCHES_TABLE = "external_ingest_batches"
_REJECTIONS_TABLE = "external_ingest_rejections"
_PAYLOADS_TABLE = "external_ingest_batch_payloads"

_IDEM_INDEX = "uq_external_ingest_batches_idem"
_ORG_STATUS_INDEX = "ix_external_ingest_batches_org_status"
_ORG_CREATED_INDEX = "ix_external_ingest_batches_org_created"
_ORG_SOURCE_INDEX = "ix_external_ingest_batches_org_source"
_REJ_ORDER_INDEX = "uq_external_ingest_rejections_ingestion_order"
_REJ_ORG_INDEX = "ix_external_ingest_rejections_org_id"
_PAYLOADS_ORG_INDEX = "ix_external_ingest_batch_payloads_org_id"


def upgrade() -> None:
    if not _table_exists(_BATCHES_TABLE):
        op.create_table(
            _BATCHES_TABLE,
            sa.Column("ingestion_id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("idempotency_key", sa.Text(), nullable=False),
            sa.Column("payload_hash", sa.Text(), nullable=False),
            sa.Column("source_system", sa.Text(), nullable=False),
            sa.Column("source_instance", sa.Text(), nullable=False),
            sa.Column("producer", sa.Text(), nullable=True),
            sa.Column("producer_version", sa.Text(), nullable=True),
            sa.Column("schema_version", sa.Text(), nullable=False),
            sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("window_ended_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("status", sa.Text(), nullable=False, server_default="accepted"),
            sa.Column("attempts", sa.Integer(), nullable=False, server_default="1"),
            sa.Column(
                "items_received", sa.Integer(), nullable=False, server_default="0"
            ),
            sa.Column(
                "items_accepted", sa.Integer(), nullable=False, server_default="0"
            ),
            sa.Column(
                "items_rejected", sa.Integer(), nullable=False, server_default="0"
            ),
            sa.Column("record_counts", sa.JSON(), nullable=True),
            sa.Column("error_summary", sa.JSON(), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.PrimaryKeyConstraint("ingestion_id"),
        )
    _create_index_if_missing(_ORG_STATUS_INDEX, _BATCHES_TABLE, ["org_id", "status"])
    _create_index_if_missing(
        _ORG_CREATED_INDEX, _BATCHES_TABLE, ["org_id", "created_at"]
    )
    _create_index_if_missing(
        _ORG_SOURCE_INDEX,
        _BATCHES_TABLE,
        ["org_id", "source_system", "source_instance"],
    )
    _create_unique_index_if_missing(
        _IDEM_INDEX,
        _BATCHES_TABLE,
        ["org_id", "source_system", "source_instance", "idempotency_key"],
    )

    if not _table_exists(_REJECTIONS_TABLE):
        op.create_table(
            _REJECTIONS_TABLE,
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column(
                "ingestion_id",
                UUID(as_uuid=True),
                sa.ForeignKey(
                    f"{_BATCHES_TABLE}.ingestion_id",
                    ondelete="CASCADE",
                    name="fk_external_ingest_rejections_ingestion_id",
                ),
                nullable=False,
            ),
            sa.Column("record_index", sa.Integer(), nullable=False),
            sa.Column("record_kind", sa.Text(), nullable=False),
            sa.Column("external_id", sa.Text(), nullable=True),
            sa.Column("code", sa.Text(), nullable=False),
            sa.Column("message", sa.Text(), nullable=False),
            sa.Column("path", sa.Text(), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.PrimaryKeyConstraint("id"),
        )
    # UNIQUE, not just an ordering index (adversarial-review defense-in-depth):
    # a single position in a customer's submitted batch is rejected at most
    # once, so this also acts as a DB-level backstop against complete_batch()
    # ever double-inserting diagnostics for the same record.
    _create_unique_index_if_missing(
        _REJ_ORDER_INDEX, _REJECTIONS_TABLE, ["ingestion_id", "record_index"]
    )
    _create_index_if_missing(_REJ_ORG_INDEX, _REJECTIONS_TABLE, ["org_id"])

    if not _table_exists(_PAYLOADS_TABLE):
        op.create_table(
            _PAYLOADS_TABLE,
            sa.Column("ingestion_id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("schema_version", sa.Text(), nullable=False),
            sa.Column("payload_json", sa.LargeBinary(), nullable=False),
            sa.Column("byte_size", sa.Integer(), nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.PrimaryKeyConstraint("ingestion_id"),
        )
    _create_index_if_missing(_PAYLOADS_ORG_INDEX, _PAYLOADS_TABLE, ["org_id"])


def downgrade() -> None:
    if _table_exists(_PAYLOADS_TABLE):
        op.drop_table(_PAYLOADS_TABLE)
    if _table_exists(_REJECTIONS_TABLE):
        op.drop_table(_REJECTIONS_TABLE)
    if _table_exists(_BATCHES_TABLE):
        op.drop_table(_BATCHES_TABLE)


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    return table_name in sa.inspect(bind).get_table_names()


def _create_index_if_missing(
    index_name: str, table_name: str, columns: list[str]
) -> None:
    bind = op.get_bind()
    existing = {ix["name"] for ix in sa.inspect(bind).get_indexes(table_name)}
    if index_name not in existing:
        op.create_index(index_name, table_name, columns)


def _create_unique_index_if_missing(
    index_name: str, table_name: str, columns: list[str]
) -> None:
    bind = op.get_bind()
    existing = {ix["name"] for ix in sa.inspect(bind).get_indexes(table_name)}
    if index_name not in existing:
        op.create_index(index_name, table_name, columns, unique=True)
