"""Add external_ingest_sources and external_ingest_tokens (CHAOS-2696/2712).

Why Postgres, not ClickHouse: this is transactional config/authz state (source
ownership + bearer-credential validity) consulted synchronously on every
external-ingest request, mirroring ProviderRateLimitObservation's
justification (migration 0031) -- ClickHouse is a separate analytics cluster
with no transactional read path suitable for per-request auth checks.

``webhook_mode``/``webhook_secret_id`` are reserved columns for CHAOS-2715
(webhook-assisted ingestion)'s must-not-foreclose contract -- unused in v1,
the admin API 400s on ``webhook_mode=fullchaos_hosted``.
``matched_integration_source_id`` stores the managed ``integration_sources``
row resolved by per-provider ownership matching at registration time
(post-critique CC5; see docs/architecture/customer-push-authz.md).

Guarded individually (create-if-missing / add-column-if-missing / per the
0025/0031 convention) so a partially-applied run can be re-run safely.

Revision ID: 0033
Revises: 0032
Create Date: 2026-07-01 00:00:00

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

_SOURCES_TABLE = "external_ingest_sources"
_TOKENS_TABLE = "external_ingest_tokens"


def upgrade() -> None:
    if not _table_exists(_SOURCES_TABLE):
        op.create_table(
            _SOURCES_TABLE,
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("system", sa.Text(), nullable=False),
            sa.Column("instance", sa.Text(), nullable=False),
            sa.Column("display_name", sa.Text(), nullable=True),
            sa.Column("mode", sa.Text(), nullable=False, server_default="disabled"),
            sa.Column(
                "enabled", sa.Boolean(), nullable=False, server_default=sa.true()
            ),
            sa.Column(
                "webhook_mode", sa.Text(), nullable=False, server_default="disabled"
            ),
            sa.Column("webhook_secret_id", UUID(as_uuid=True), nullable=True),
            sa.Column(
                "matched_integration_source_id", UUID(as_uuid=True), nullable=True
            ),
            sa.Column("created_by_user_id", UUID(as_uuid=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "org_id",
                "system",
                "instance",
                name="uq_external_ingest_sources_org_system_instance",
            ),
        )
    _add_column_if_missing(
        _SOURCES_TABLE,
        sa.Column("webhook_mode", sa.Text(), nullable=False, server_default="disabled"),
    )
    _add_column_if_missing(
        _SOURCES_TABLE,
        sa.Column("webhook_secret_id", UUID(as_uuid=True), nullable=True),
    )
    _add_column_if_missing(
        _SOURCES_TABLE,
        sa.Column("matched_integration_source_id", UUID(as_uuid=True), nullable=True),
    )
    _create_index_if_missing(
        "ix_external_ingest_sources_org_id", _SOURCES_TABLE, ["org_id"]
    )

    if not _table_exists(_TOKENS_TABLE):
        op.create_table(
            _TOKENS_TABLE,
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column(
                "source_id",
                UUID(as_uuid=True),
                sa.ForeignKey(f"{_SOURCES_TABLE}.id"),
                nullable=True,
            ),
            sa.Column("name", sa.Text(), nullable=False),
            sa.Column("token_hash", sa.Text(), nullable=False),
            sa.Column("token_prefix", sa.Text(), nullable=False),
            sa.Column("scopes", sa.JSON(), nullable=False),
            sa.Column("created_by_user_id", UUID(as_uuid=True), nullable=True),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_used_ip", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "token_hash", name="uq_external_ingest_tokens_token_hash"
            ),
        )
    _create_index_if_missing(
        "ix_external_ingest_tokens_org_id", _TOKENS_TABLE, ["org_id"]
    )
    _create_index_if_missing(
        "ix_external_ingest_tokens_source_id", _TOKENS_TABLE, ["source_id"]
    )
    _create_index_if_missing(
        "ix_external_ingest_tokens_org_active", _TOKENS_TABLE, ["org_id", "revoked_at"]
    )


def downgrade() -> None:
    if _table_exists(_TOKENS_TABLE):
        op.drop_table(_TOKENS_TABLE)
    if _table_exists(_SOURCES_TABLE):
        op.drop_table(_SOURCES_TABLE)


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    return table_name in sa.inspect(bind).get_table_names()


def _column_names(table_name: str) -> set[str]:
    bind = op.get_bind()
    return {column["name"] for column in sa.inspect(bind).get_columns(table_name)}


def _add_column_if_missing(table_name: str, column: sa.Column) -> None:
    if column.name not in _column_names(table_name):
        op.add_column(table_name, column)


def _create_index_if_missing(
    index_name: str, table_name: str, columns: list[str]
) -> None:
    bind = op.get_bind()
    existing = {ix["name"] for ix in sa.inspect(bind).get_indexes(table_name)}
    if index_name not in existing:
        op.create_index(index_name, table_name, columns)
