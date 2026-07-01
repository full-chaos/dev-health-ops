"""Add run-auth freeze credential stamp columns to sync_runs (CHAOS-2755).

Stamps the credential resolved once at plan time onto the run so every later
phase (reference discovery, BudgetGuard, unit execution) reads the run-frozen
credential instead of re-resolving the mutable ``Integration.credential_id``.

Three nullable columns are added to ``sync_runs``:
  * ``credential_id`` — plain UUID, deliberately NO foreign key so deleting a
    stamped credential mid-run is not blocked and instead surfaces as the
    existing "Credential not found" unit failure (the honest outcome).
  * ``credential_fingerprint`` — safe-scope content witness (no raw secret).
  * ``auth_source`` — 'integration_credential' | 'environment'; NULL marks a
    legacy/pre-migration or in-flight-at-deploy run.

These are RUN-level columns; per CHAOS-2755 no credential field is added to
``sync_run_units`` (credentials are auth state, never dispatch capacity).

Retry safety: each column is guarded individually (per revision 0022) so a
rerun after a partial failure resumes instead of failing on duplicate columns.

Revision ID: 0030
Revises: 0029
Create Date: 2026-07-01 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0030"
down_revision: str | None = "0029"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    _add_column_if_missing(
        "sync_runs",
        sa.Column("credential_id", UUID(as_uuid=True), nullable=True),
    )
    _add_column_if_missing(
        "sync_runs",
        sa.Column("credential_fingerprint", sa.Text(), nullable=True),
    )
    _add_column_if_missing(
        "sync_runs",
        sa.Column("auth_source", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    _drop_column_if_present("sync_runs", "auth_source")
    _drop_column_if_present("sync_runs", "credential_fingerprint")
    _drop_column_if_present("sync_runs", "credential_id")


def _add_column_if_missing(table_name: str, column: sa.Column) -> None:
    existing_columns = _column_names(table_name)
    if column.name not in existing_columns:
        op.add_column(table_name, column)


def _drop_column_if_present(table_name: str, column_name: str) -> None:
    existing_columns = _column_names(table_name)
    if column_name in existing_columns:
        op.drop_column(table_name, column_name)


def _column_names(table_name: str) -> set[str]:
    bind = op.get_bind()
    return {column["name"] for column in sa.inspect(bind).get_columns(table_name)}
