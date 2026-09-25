"""Let a failed OAuth setup keep a durable revocation record.

Revision ID: 0141
Revises: 0140

A PagerDuty OAuth callback that exchanges a code and then does not keep the
grant (missing scopes, the account cannot be proved, the local write fails)
must revoke the token it was just issued. The Go callback records that token
in ``provider_oauth_revocations`` right after the exchange, under the new
purpose ``setup``, and keeps the row until PagerDuty has accepted the
revoke. This widens the purpose check from ``replacement`` and ``disconnect``
to those two and ``setup``. No data changes.

Downgrade restores the 0044 check. It fails while ``setup`` rows exist.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0141"
down_revision: str | None = "0140"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CONSTRAINT = "ck_provider_oauth_revocations_purpose"

_PREVIOUS_PURPOSES = ("replacement", "disconnect")
_PURPOSES = ("replacement", "disconnect", "setup")


def _replace(purposes: tuple[str, ...]) -> None:
    quoted = ", ".join(f"'{purpose}'" for purpose in purposes)
    with op.batch_alter_table("provider_oauth_revocations") as batch_op:
        batch_op.drop_constraint(_CONSTRAINT, type_="check")
        batch_op.create_check_constraint(_CONSTRAINT, f"purpose IN ({quoted})")


def upgrade() -> None:
    _replace(_PURPOSES)


def downgrade() -> None:
    _replace(_PREVIOUS_PURPOSES)
