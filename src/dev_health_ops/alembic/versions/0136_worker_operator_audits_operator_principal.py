"""Allow the operator principal in worker_operator_audits.

Revision ID: 0136
Revises: 0135

The operator CLI (``dho workers``) no longer authenticates with a service
credential: exec access to a worker pod plus its database DSNs is the
operator boundary, and every audited mutation is recorded with principal
``operator/dho-workers`` and a NULL ``credential_id``. The principal-type
check from 0047 allowed only ``service_credential``; this widens it to also
allow ``operator``. Check constraint only; no data changes.

Downgrade restores the 0047 check. It fails while rows with
``principal_type = 'operator'`` exist, which is the honest outcome: those
rows would violate the restored constraint.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0136"
down_revision: str | None = "0135"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CONSTRAINT = "ck_worker_operator_audits_principal_type"


def _replace(condition: str) -> None:
    with op.batch_alter_table("worker_operator_audits") as batch_op:
        batch_op.drop_constraint(_CONSTRAINT, type_="check")
        batch_op.create_check_constraint(_CONSTRAINT, condition)


def upgrade() -> None:
    _replace("principal_type IN ('service_credential', 'operator')")


def downgrade() -> None:
    _replace("principal_type = 'service_credential'")
