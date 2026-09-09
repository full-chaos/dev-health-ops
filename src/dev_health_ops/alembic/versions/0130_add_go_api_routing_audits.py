"""Add ``go_api_routing_audits`` -- the append-only record of routing writes.

Revision ID: 0130
Revises: 0129

CHAOS-5505. Three verbs (``enable``, ``disable``, ``repoint``) change which
plane serves production traffic, and none of them left a durable record.
The only trace was ``recorded_by`` + ``review_evidence`` ON THE ROUTING ROW
ITSELF, and that row is mutable: the next write overwrites both. So the
history of "who moved this, and when" was exactly one entry deep, and the
entry was whoever touched it last.

**WHY A NEW TABLE AND NOT ``worker_operator_audits``.** The first draft of
this migration widened that table's ``action`` and ``principal_type``
CHECKs. chris ruled against it (2026-09-09): "this column is definitely for
syncs to pass to workers" -- ``worker_operator_audits`` belongs to the
sync -> worker operator plane (alembic 0047, CHAOS-3033, the Go WORKER
migration epic), and its ``principal_type`` is pinned to
``service_credential``, a credential class issued and validated on that
plane. Routing writes are a different plane with a different credential
class, and borrowing a table because its column names happened to fit is
how two unrelated things end up impossible to aggregate separately later.

**THE CREDENTIAL VOCABULARY IS THE AUTH CONTROL PLANE'S, NOT A NEW ONE.**
``contracts/auth/v1/credential-classes.schema.json`` is a closed registry:
30 classes, each carrying issuer, validator, lifecycle authority and
allowed route set. ``credential_class`` here uses its names:

* ``effective_principal_envelope`` -- ``enable`` and ``repoint``. Both read
  the deployed process's authenticated ``/buildinfo``, which VERIFIES the
  envelope (``cmd/query-api/internal/principal/verifier.go``), so by the
  time a row is written the credential has been checked by the verifier
  that owns it. This is the exact class_id the Auth Control Plane threat
  model (Wave 0, §11.1 and §12 recommendation 2) proposes for it; the
  envelope is not yet REGISTERED in that contract, and registering it is
  deliberately NOT part of this change -- it is filed against the paused
  Auth Control Plane project.
* ``operator_direct`` -- ``disable``. It presents NO credential, by
  contract: the off-ramp must work when the planes disagree and the
  deployed process is down, so there is nothing to verify. Calling it an
  envelope to reuse an existing value would put an unverified claim in an
  append-only table.

``status`` writes no row at all. It is a read-only diagnostic, and an
audit entry for a read would make "this operation was touched on the 9th"
untrue.

**principal_id vs recorded_by.** They answer different questions and
conflating them makes both unreliable. ``principal_id`` is WHO THE
CREDENTIAL SAYS is acting -- the envelope's ``sub``, a user id, since the
effective-principal envelope is a USER principal (``principal_envelope.py``
mints it only for a user that already survived
``AuthService.authenticate_access_token``, and its claims carry role,
permissions, superuser state and the impersonation chain). ``recorded_by``
is what the OPERATOR typed about themselves, verified by nothing, and is
required on every row. The pairing CHECK makes the distinction structural:
an envelope-class row MUST name the subject its credential carried, and an
``operator_direct`` row MUST NOT, because there is no credential to have
carried one.

**No foreign key to ``go_api_routing_state``.** Deliberate. This table is
append-only and must outlive the row it describes; an FK would make
deleting a routing row either impossible or silently destructive to the
record of what happened to it.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0130"
down_revision: str | None = "0129"
branch_labels = None
depends_on = None

_TABLE = "go_api_routing_audits"

#: The three WRITE verbs. `status` is read-only and writes no row.
ACTIONS = ("enable", "disable", "repoint")

#: Auth Control Plane credential-class ids. See the module docstring.
CREDENTIAL_CLASSES = ("effective_principal_envelope", "operator_direct")

#: Mirrors ck_go_api_routing_state_mode exactly. A mode this table can
#: record but that table cannot hold would let an audit row describe a
#: state the system cannot be in.
MODES = ("python", "shadow", "canary", "primary", "disabled")

#: review_evidence is bounded rather than unbounded text: it is written by
#: an operator on the command line and this table is append-only, so an
#: accidental paste of a whole log has no later remedy. Long enough for a
#: real explanation, short enough that the row stays readable.
REVIEW_EVIDENCE_MAX = 2000


def _quoted(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        # One invocation writes one row PER OPERATION -- `resource_id`
        # semantics: the resource is one routing row, not "the fifteen of
        # them". correlation_id is what re-groups an invocation, so
        # "which rows did that one command touch" stays answerable.
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("credential_class", sa.Text(), nullable=False),
        # NULL exactly when the credential class carries no subject.
        sa.Column("principal_id", sa.Text(), nullable=True),
        sa.Column("recorded_by", sa.Text(), nullable=False),
        sa.Column("review_evidence", sa.Text(), nullable=False),
        # The routing row's full key. schema_digest is not optional: it is
        # what CHAOS-5416 moved, and an audit row that did not carry it
        # could not tell a live write from one against a dead digest.
        sa.Column("schema_digest", sa.Text(), nullable=False),
        sa.Column("document_digest", sa.Text(), nullable=False),
        sa.Column("selected_operation", sa.Text(), nullable=False),
        # NULL before, when no row existed and the verb created one.
        sa.Column("candidate_build_before", sa.Text(), nullable=True),
        sa.Column("candidate_build_after", sa.Text(), nullable=False),
        sa.Column("mode_before", sa.Text(), nullable=True),
        sa.Column("mode_after", sa.Text(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            f"action IN ({_quoted(ACTIONS)})",
            name="ck_go_api_routing_audits_action",
        ),
        sa.CheckConstraint(
            f"credential_class IN ({_quoted(CREDENTIAL_CLASSES)})",
            name="ck_go_api_routing_audits_credential_class",
        ),
        # The distinction between "the credential named a subject" and
        # "there was no credential" is the whole reason both classes
        # exist. Written as an equivalence so neither shape can be
        # recorded as the other.
        sa.CheckConstraint(
            "(credential_class = 'effective_principal_envelope')"
            " = (principal_id IS NOT NULL)",
            name="ck_go_api_routing_audits_principal_pairing",
        ),
        sa.CheckConstraint(
            f"mode_before IS NULL OR mode_before IN ({_quoted(MODES)})",
            name="ck_go_api_routing_audits_mode_before",
        ),
        sa.CheckConstraint(
            f"mode_after IN ({_quoted(MODES)})",
            name="ck_go_api_routing_audits_mode_after",
        ),
        # An empty reason is the same failure as no reason. Bounded above
        # for the reason in REVIEW_EVIDENCE_MAX's comment.
        sa.CheckConstraint(
            f"char_length(review_evidence) BETWEEN 1 AND {REVIEW_EVIDENCE_MAX}",
            name="ck_go_api_routing_audits_review_evidence_bounded",
        ),
        sa.CheckConstraint(
            "char_length(recorded_by) BETWEEN 1 AND 128",
            name="ck_go_api_routing_audits_recorded_by_bounded",
        ),
    )
    op.create_index(
        "ix_go_api_routing_audits_correlation",
        _TABLE,
        ["correlation_id"],
    )
    # The question this table is asked in an incident: "what happened to
    # THIS operation, most recent first".
    op.create_index(
        "ix_go_api_routing_audits_operation_recorded",
        _TABLE,
        ["schema_digest", "selected_operation", sa.text("recorded_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_go_api_routing_audits_operation_recorded", table_name=_TABLE)
    op.drop_index("ix_go_api_routing_audits_correlation", table_name=_TABLE)
    op.drop_table(_TABLE)
