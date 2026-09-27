"""Add saml_assertion_replays (CHAOS-6659, D2744).

Revision ID: 0144
Revises: 0143

The Go SAML ACS port (internal/api/sso/saml.go) verifies signature,
issuer, audience, subject confirmation and timestamps on every assertion,
but nothing marked a validly-signed, still-unexpired assertion as
CONSUMED: the same captured SAMLResponse could be replayed to /acs
repeatedly within its NotOnOrAfter window and mint a fresh token pair
each time (r1 review finding, D2744 ruling). Python has the identical
gap (process_saml_response never persists anything either), so this is
a class of pre-existing risk in a deployable SAML flow, not a new one --
closed here rather than shipped, per the ruling.

One row per (provider, assertion) pair the SP has accepted; a second
presentation with the same pair is refused before any token is minted.
Scoped per-provider (not globally) since two providers could legitimately
receive assertions whose IdP-chosen IDs collide (different IdPs, no
coordination). No foreign key to sso_providers: a replay-guard row must
outlive a provider's own deletion long enough to still refuse a captured
assertion presented after the provider row itself is gone, rather than
silently losing the guard on a cascade delete.

created_at exists for operational visibility only (SELECT * WHERE
created_at is old); the actual guard is the primary key precondition
combined with a WHERE expires_at > now() at read time, so no scheduled
prune job is required for correctness -- expired rows can be reaped by a
separate maintenance job later (not scoped here) purely to bound table
growth.

No data changes. Downgrade drops the table.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0144"
down_revision: str | None = "0143"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "saml_assertion_replays"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("provider_id", postgresql.UUID(as_uuid=True), nullable=False),
        # The SAML Assertion's own ID attribute (not the SP's AuthnRequest
        # ID): the value that identifies THIS specific accepted assertion,
        # scoped per-provider since two IdPs' ID spaces are independent.
        sa.Column("assertion_id", sa.Text(), nullable=False),
        # The assertion's own Conditions.NotOnOrAfter: once past, a repeat
        # presentation already fails timestamp validation on its own, so
        # the guard row is no longer load-bearing after this point.
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint(
            "provider_id", "assertion_id", name="pk_saml_assertion_replays"
        ),
    )
    # A future prune job's read pattern: rows whose guard window has
    # closed, in age order.
    op.create_index(
        "ix_saml_assertion_replays_expires_at",
        _TABLE,
        ["expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_saml_assertion_replays_expires_at", table_name=_TABLE)
    op.drop_table(_TABLE)
