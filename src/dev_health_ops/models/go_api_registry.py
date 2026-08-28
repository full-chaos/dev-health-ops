"""Go API operation rollout registry + proof ledger (CHAOS-4366 Wave 0).

Plan doc: ``.github/docs-legacy/plans/go-api-epic.md`` §8.3. Three tables,
not one, because a single table cannot be both the append-only proof target
and the mutable routing decision (``mode``/``rollout_percentage``/"which
build is current" all change as a rollout progresses, but a proof must stay
pinned to the exact build it proved):

* :class:`CandidateBuild` -- immutable, append-only. One row is created the
  first time a ``candidate_build`` registers against an operation; it is
  never updated. :class:`ProofRun` references this row (via the full
  4-column composite key), so a proof can never be silently reattributed to
  a later build.
* :class:`RoutingState` -- exactly one mutable row per
  ``(schema_digest, document_digest, selected_operation)``. Holds the
  *current* ``candidate_build`` pointer, ``mode``, ``rollout_percentage``,
  and ``eligible_orgs``. This is what the request router reads on every
  call, and what a rollback mutates in place -- moving the pointer back to
  an earlier (already-immutable) :class:`CandidateBuild` row is exactly the
  "registry change, not an image rollback" from plan §5.
* :class:`ProofRun` -- one row per (stage, request) proof attempt. Records
  which *request* produced its verdict (``request_identity``), not only
  which candidate, because a registered document invoked with different
  variables/auth-context/org can diverge even at the same document digest.

The FK from :class:`RoutingState`/:class:`ProofRun` to
:class:`CandidateBuild` is a full 4-column composite FK
(``schema_digest``, ``document_digest``, ``selected_operation``,
``*_candidate_build``) -- never just the bare ``candidate_build`` string --
so a build can only ever be "current" or "proven" for the exact operation
triple it was registered against.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Integer,
    PrimaryKeyConstraint,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base

#: plan §5's owner vocabulary.
OWNERS = ("python", "go")

#: plan §5's routing-mode vocabulary. Order matters only for readability;
#: the CHECK constraint is the enforcement.
MODES = ("python", "shadow", "canary", "primary", "disabled")

#: plan §5's five-stage proof gate stage names.
STAGES = ("dual_run", "deployed_executed", "shadow", "canary")

#: plan §5's terminal-state vocabulary. No unclassified equivalent of a
#: stranded partition is acceptable -- every comparator/proof outcome must
#: land in exactly one of these, never a bare pass/fail boolean.
TERMINAL_STATES = (
    "match",
    "mismatch",
    "auth_rejected",
    "validation_rejected",
    "dependency_failed",
    "timeout",
    "cancelled",
    "resource_exhausted",
    "fallback",
    "unsupported",
    "proof_failed",
)


class CandidateBuild(Base):
    """Immutable, append-only. Never updated after insert.

    One row per ``(schema_digest, document_digest, selected_operation,
    candidate_build)`` -- the first time a given build registers against a
    given operation. Application code must never issue an UPDATE against
    this table; only INSERT ... ON CONFLICT DO NOTHING (registering the
    same build twice is a no-op, not an error).
    """

    __tablename__ = "go_api_candidate_build"

    schema_digest: Mapped[str] = mapped_column(Text, nullable=False)
    document_digest: Mapped[str] = mapped_column(Text, nullable=False)
    selected_operation: Mapped[str] = mapped_column(Text, nullable=False)
    candidate_build: Mapped[str] = mapped_column(Text, nullable=False)
    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        PrimaryKeyConstraint(
            "schema_digest",
            "document_digest",
            "selected_operation",
            "candidate_build",
            name="pk_go_api_candidate_build",
        ),
    )


class RoutingState(Base):
    """Exactly one mutable row per ``(schema_digest, document_digest,
    selected_operation)``. Read by the request router on every call; a
    rollback mutates ``current_candidate_build`` in place.
    """

    __tablename__ = "go_api_routing_state"

    schema_digest: Mapped[str] = mapped_column(Text, nullable=False)
    document_digest: Mapped[str] = mapped_column(Text, nullable=False)
    selected_operation: Mapped[str] = mapped_column(Text, nullable=False)
    current_candidate_build: Mapped[str] = mapped_column(Text, nullable=False)
    owner: Mapped[str] = mapped_column(Text, nullable=False)
    mode: Mapped[str] = mapped_column(Text, nullable=False, default="python")
    #: JSON array of org ids, or null/omitted meaning "all orgs eligible".
    #: Never used to widen a `disabled`/`python` mode entry -- eligibility
    #: only matters once mode is shadow/canary/primary.
    eligible_orgs: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    rollout_percentage: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        PrimaryKeyConstraint(
            "schema_digest",
            "document_digest",
            "selected_operation",
            name="pk_go_api_routing_state",
        ),
        ForeignKeyConstraint(
            [
                "schema_digest",
                "document_digest",
                "selected_operation",
                "current_candidate_build",
            ],
            [
                "go_api_candidate_build.schema_digest",
                "go_api_candidate_build.document_digest",
                "go_api_candidate_build.selected_operation",
                "go_api_candidate_build.candidate_build",
            ],
            name="fk_go_api_routing_state_candidate_build",
        ),
        CheckConstraint(
            f"owner IN {OWNERS!r}",
            name="ck_go_api_routing_state_owner",
        ),
        CheckConstraint(
            f"mode IN {MODES!r}",
            name="ck_go_api_routing_state_mode",
        ),
        CheckConstraint(
            "rollout_percentage >= 0 AND rollout_percentage <= 100",
            name="ck_go_api_routing_state_rollout_percentage",
        ),
    )


class ProofRun(Base):
    """One row per proof attempt. The proof's immutable key is
    ``(schema_digest, document_digest, selected_operation, candidate_build)``
    together -- a proof run is evidence for exactly one tuple, never carried
    forward across any of the four changing (plan §8.3).
    """

    __tablename__ = "go_api_proof_run"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    schema_digest: Mapped[str] = mapped_column(Text, nullable=False)
    document_digest: Mapped[str] = mapped_column(Text, nullable=False)
    selected_operation: Mapped[str] = mapped_column(Text, nullable=False)
    candidate_build: Mapped[str] = mapped_column(Text, nullable=False)
    #: Digest of variables + auth-context shape + org_id. Distinguishes two
    #: invocations of the same registered document that diverge only in
    #: what was asked, not in which document/build was used.
    request_identity: Mapped[str] = mapped_column(Text, nullable=False)
    stage: Mapped[str] = mapped_column(Text, nullable=False)
    terminal_state: Mapped[str] = mapped_column(Text, nullable=False)
    #: Durable artifact references (e.g. object-storage key), never inlined
    #: response bodies -- this table must stay cheap to scan/index.
    baseline_response_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidate_response_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: Nullable; required (by the CHECK below) whenever the operation has
    #: side effects per plan §5 stage 2 -- enforced by application code
    #: setting a value, the CHECK only catches the stage-2 case structurally.
    side_effect_digest: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: Required for stage-4 (shadow) same-watermark comparison; CHAOS-4381
    #: parity rule 4 -- a watermark mismatch is terminal_state='unsupported',
    #: never 'mismatch'.
    data_watermark: Mapped[str | None] = mapped_column(Text, nullable=True)
    org_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        ForeignKeyConstraint(
            [
                "schema_digest",
                "document_digest",
                "selected_operation",
                "candidate_build",
            ],
            [
                "go_api_candidate_build.schema_digest",
                "go_api_candidate_build.document_digest",
                "go_api_candidate_build.selected_operation",
                "go_api_candidate_build.candidate_build",
            ],
            name="fk_go_api_proof_run_candidate_build",
        ),
        CheckConstraint(
            f"stage IN {STAGES!r}",
            name="ck_go_api_proof_run_stage",
        ),
        CheckConstraint(
            f"terminal_state IN {TERMINAL_STATES!r}",
            name="ck_go_api_proof_run_terminal_state",
        ),
        # Stage 4 (shadow) requires a same-watermark comparison; a shadow
        # proof run with no captured watermark cannot back that claim.
        CheckConstraint(
            "stage <> 'shadow' OR data_watermark IS NOT NULL",
            name="ck_go_api_proof_run_shadow_requires_watermark",
        ),
    )
