"""Add ``go_api_proof_orgs`` -- the org allowlist for ``/query/proof-write``.

Revision ID: 0145
Revises: 0144

CHAOS-7096 (Option 3 of the CHAOS-6098 bootstrap write-proof design,
D2944/D2945). ``/query/proof-write`` executes a registered MUTATION
document, through the real deployed HTTP/gqlgen/auth stack, without the
operation being routed to Go for real traffic -- exactly the capability
CHAOS-6098 found missing (a saved-report mutation could never get a
bootstrap write-proof receipt because ``prove-write -via query-api`` needs
an existing routed operation and ``enable`` circularly needs a receipt at
the target mode first). Because it runs a real mutation on an internal-only
route, "which orgs may use it" is decided by data, not by a build flag or an
env var an operator could forget: an EMPTY table refuses every org, which is
this feature's safe default on every deploy until an operator opts one in.

**LIVE ALLOWLIST, PLUS ITS OWN APPEND-ONLY AUDIT TABLE -- not a shared
one.** ``go_api_routing_audits`` (alembic 0130) was built, and chris ruled
against widening, for exactly this reason: its columns
(``schema_digest``/``document_digest``/``selected_operation``/
``mode_before``/``mode_after``) describe a ROUTING ROW, and a proof-org
add/remove has none of those things to report. Forcing this event into
that shape would either violate its CHECK constraints or leave most of its
columns meaninglessly NULL. A proof-org event needs exactly two facts
(``org_id``, ``action``) plus who and why -- its own table says that
directly instead of overloading a routing-shaped one.

**Why the live table is separate from the audit table**, matching
``go_api_routing_state`` (mutable, read on every request) vs
``go_api_routing_audits`` (append-only, read only for history): the
allowlist is a hot READ for query-api's per-request org check and must stay
a single small ``WHERE org_id = $1`` row lookup; the audit trail is
append-only and irrelevant to that lookup's read path. Wiring both
into one table would mean query-api's grant on it must also cover history
rows it never needs to see, and a DELETE (removing an org) would destroy
the very audit trail an operator wanted kept.

**Credential class.** The ``dho goapi routing proof-org`` verb is an
operator-run CLI command with no deployed-process envelope to read (unlike
``enable``/``repoint``, which verify ``/buildinfo``'s authenticated
envelope) -- so every row is ``operator_direct``
(``goapiproof.CredentialClassOperatorDirect``), the same class ``disable``
already uses for the same reason (no credential to present).

**No foreign key between the two tables.** ``go_api_proof_orgs`` is
mutable (an org can be removed, later re-added); the audit table is
append-only and must outlive any row it describes.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0145"
down_revision: str | None = "0144"
branch_labels = None
depends_on = None

_ORGS_TABLE = "go_api_proof_orgs"
_AUDITS_TABLE = "go_api_proof_org_audits"

#: The only two verbs. There is no "update" -- removing then re-adding an
#: org is two rows, each independently reasoned about.
ACTIONS = ("add", "remove")

#: Matches goapiproof.CredentialClassOperatorDirect. Only one class exists
#: for this verb today (see the module docstring); the column is still
#: bounded rather than a bare boolean so a future envelope-backed variant
#: does not need a column rename.
CREDENTIAL_CLASSES = ("operator_direct",)

#: An operator-typed reason, same bound as go_api_routing_audits.review_evidence
#: (0130) for the same reason: this is append-only, so an accidental paste of
#: a whole log has no later remedy.
REASON_MAX = 2000


def _quoted(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def upgrade() -> None:
    op.create_table(
        _ORGS_TABLE,
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("added_by", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("org_id"),
        sa.CheckConstraint(
            "char_length(added_by) BETWEEN 1 AND 128",
            name="ck_go_api_proof_orgs_added_by_bounded",
        ),
        sa.CheckConstraint(
            f"char_length(reason) BETWEEN 1 AND {REASON_MAX}",
            name="ck_go_api_proof_orgs_reason_bounded",
        ),
    )

    op.create_table(
        _AUDITS_TABLE,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("credential_class", sa.Text(), nullable=False),
        sa.Column("recorded_by", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            f"action IN ({_quoted(ACTIONS)})",
            name="ck_go_api_proof_org_audits_action",
        ),
        sa.CheckConstraint(
            f"credential_class IN ({_quoted(CREDENTIAL_CLASSES)})",
            name="ck_go_api_proof_org_audits_credential_class",
        ),
        sa.CheckConstraint(
            "char_length(recorded_by) BETWEEN 1 AND 128",
            name="ck_go_api_proof_org_audits_recorded_by_bounded",
        ),
        sa.CheckConstraint(
            f"char_length(reason) BETWEEN 1 AND {REASON_MAX}",
            name="ck_go_api_proof_org_audits_reason_bounded",
        ),
    )
    op.create_index(
        "ix_go_api_proof_org_audits_org_recorded",
        _AUDITS_TABLE,
        ["org_id", sa.text("recorded_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_go_api_proof_org_audits_org_recorded", table_name=_AUDITS_TABLE)
    op.drop_table(_AUDITS_TABLE)
    op.drop_table(_ORGS_TABLE)
