"""Seed the go-api-prove proof service principal users row.

Revision ID: 0133
Revises: 0132

CHAOS-5727: go-api-prove's edge access token is minted in the tools image by
``mint-edge-token`` (``internal/edgetokenmint``) for a DEDICATED service
principal, never a human user. The minter reads this row by its fixed id and
refuses unless it is a service identity (``auth_provider = 'service'`` and no
password hash), active, and not a superuser. The Python edge then re-reads
``is_active`` and ``token_version`` for it on every request, exactly as for
any other access token.

The row is created with NO membership. The minter also refuses a principal
that holds no read-level membership in the org being proven, so this revision
alone grants no access to any org's data. An operator grants a ``viewer``
membership per proven org (see the go-api Wave 0 proof infrastructure doc).

The email uses the reserved ``.invalid`` top-level domain, so no OAuth or SSO
provider can ever verify it, and the NULL password hash means no local login
path can reach the row.

DATA CHANGE. Inserts at most one row. An existing row with this id is
accepted only if it already has the service-identity shape; any other row
with this id or this email is a conflict and the revision refuses.

downgrade() deletes the row only while it still has the service-identity
shape. ``memberships.user_id`` is ``ON DELETE CASCADE``, so any membership an
operator granted to it goes too.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql.selectable import TableClause

revision: str = "0133"
down_revision: str | None = "0132"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

# Must match internal/edgetokenmint.ProvePrincipalID and ServiceAuthProvider.
PRINCIPAL_ID = "00000000-0000-4000-8000-00000000e0e1"
PRINCIPAL_EMAIL = "go-api-prove@service.dev-health.invalid"
SERVICE_AUTH_PROVIDER = "service"


def _users() -> TableClause:
    return sa.table(
        "users",
        sa.column("id", UUID(as_uuid=False)),
        sa.column("email", sa.Text()),
        sa.column("password_hash", sa.Text()),
        sa.column("auth_provider", sa.Text()),
        sa.column("is_active", sa.Boolean()),
        sa.column("is_verified", sa.Boolean()),
        sa.column("is_superuser", sa.Boolean()),
        sa.column("token_version", sa.Integer()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )


def upgrade() -> None:
    users = _users()
    bind = op.get_bind()
    existing = (
        bind.execute(
            sa.select(
                users.c.id,
                users.c.email,
                users.c.auth_provider,
                users.c.password_hash,
            )
            .where(sa.or_(users.c.id == PRINCIPAL_ID, users.c.email == PRINCIPAL_EMAIL))
            .with_for_update()
        )
        .mappings()
        .all()
    )
    for row in existing:
        if (
            str(row["id"]) != PRINCIPAL_ID
            or row["email"] != PRINCIPAL_EMAIL
            or row["auth_provider"] != SERVICE_AUTH_PROVIDER
            or row["password_hash"] is not None
        ):
            raise RuntimeError(
                "a users row conflicts with the go-api-prove service principal "
                f"(id {PRINCIPAL_ID}, email {PRINCIPAL_EMAIL})"
            )
    if existing:
        return
    now = datetime.now(UTC)
    bind.execute(
        users.insert().values(
            id=PRINCIPAL_ID,
            email=PRINCIPAL_EMAIL,
            password_hash=None,
            auth_provider=SERVICE_AUTH_PROVIDER,
            is_active=True,
            is_verified=False,
            is_superuser=False,
            token_version=0,
            created_at=now,
            updated_at=now,
        )
    )


def downgrade() -> None:
    users = _users()
    op.get_bind().execute(
        users.delete().where(
            users.c.id == PRINCIPAL_ID,
            users.c.auth_provider == SERVICE_AUTH_PROVIDER,
            users.c.password_hash.is_(None),
        )
    )
