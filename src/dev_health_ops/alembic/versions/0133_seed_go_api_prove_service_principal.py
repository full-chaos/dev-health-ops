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
alone grants no access to any org's data. The per-org grant is an idempotent
operator statement in the query-api bootstrap runbook's prod-proof step, not
part of this revision.

The email uses the reserved ``.invalid`` top-level domain, so no OAuth or SSO
provider can ever verify it, and the NULL password hash means no local login
path can reach the row.

DATA CHANGE. Inserts at most one row, ``ON CONFLICT DO NOTHING``: an existing
row with this id or this email is left exactly as it is. If that row is not a
service identity, the minter refuses it, so a conflict can never become a
mintable principal.

downgrade() deletes the row only while it is still a service identity AND
holds no membership. A row an operator has granted access to, or has changed,
records a later decision this revision cannot infer belongs to it.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql, sqlite
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
        sa.column("id", sa.Uuid(as_uuid=False)),
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


def _memberships() -> TableClause:
    return sa.table("memberships", sa.column("user_id", sa.Uuid(as_uuid=False)))


def upgrade() -> None:
    bind = op.get_bind()
    now = datetime.now(UTC)
    values = {
        "id": PRINCIPAL_ID,
        "email": PRINCIPAL_EMAIL,
        "password_hash": None,
        "auth_provider": SERVICE_AUTH_PROVIDER,
        "is_active": True,
        "is_verified": False,
        "is_superuser": False,
        "token_version": 0,
        "created_at": now,
        "updated_at": now,
    }
    if bind.dialect.name == "postgresql":
        bind.execute(
            postgresql.insert(_users()).values(**values).on_conflict_do_nothing()
        )
    elif bind.dialect.name == "sqlite":
        bind.execute(sqlite.insert(_users()).values(**values).on_conflict_do_nothing())
    else:
        raise RuntimeError(f"0133 does not support the {bind.dialect.name} dialect")


def downgrade() -> None:
    users = _users()
    memberships = _memberships()
    op.get_bind().execute(
        users.delete().where(
            users.c.id == PRINCIPAL_ID,
            users.c.auth_provider == SERVICE_AUTH_PROVIDER,
            users.c.password_hash.is_(None),
            ~sa.exists().where(memberships.c.user_id == users.c.id),
        )
    )
