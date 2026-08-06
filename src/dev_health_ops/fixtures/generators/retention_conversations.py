"""CHAOS-3219: retention-aged conversations + validation packets for the
ask-dev-world (Postgres ``dev_conversations``/``dev_runs``/
``dev_run_subject_sets`` -- ``models/dev_persistence.py``).

Scope deliberately narrow: only ``DevConversation``, ``DevRun``, and
``DevRunSubjectSet`` rows are built here, never ``DevMessage`` -- that model
carries a role-conditional ``CheckConstraint`` (user vs assistant shape) this
lane does not need to satisfy to realize retention aging, the stale-context
subject class, or a validation-status "packet". Every row uses a
deterministic ``uuid.uuid5``-derived id (never ``uuid.uuid4()``) so a second
generation run ``session.merge()``s the SAME rows instead of duplicating
them -- the seed-idempotency guarantee ``fixtures world`` proves against a
live scratch database.

No wall-clock ``datetime.now()``/``utcnow()`` anywhere (CHAOS-3392 lesson):
every timestamp is derived from a caller-supplied ``pinned_now``.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dev_health_ops.models.dev_persistence import (
        DevConversation,
        DevRun,
        DevRunSubjectSet,
    )

FIXTURE_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

#: The real ``grounding_validation_status`` vocabulary this world seeds.
#: ``dev_runs.grounding_validation_status`` carries no DB-level CHECK
#: constraint (free String(32)) -- these three values are the ones
#: ``scope_service.py``'s own commentary and the ``trust.*`` corpus family
#: name explicitly (validated / a detected self-contradiction / insufficient
#: evidence to validate at all).
VALIDATION_STATUSES: tuple[str, ...] = (
    "validated",
    "contradiction_detected",
    "insufficient_evidence",
)


def _uuid5(*parts: str) -> uuid.UUID:
    return uuid.uuid5(FIXTURE_NAMESPACE, ":".join(parts))


@dataclass(frozen=True, slots=True)
class ConversationBundle:
    """A conversation plus its runs and (optionally) one subject set, ready
    to hand to ``world.py``'s ``session.merge()`` loop."""

    conversation: DevConversation
    runs: tuple[DevRun, ...]
    subject_sets: tuple[DevRunSubjectSet, ...]


def build_retention_aged_conversation(
    *,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    id_seed: str,
    retention_days: int,
    age_days: int,
    pinned_now: datetime,
    title: str,
) -> ConversationBundle:
    """One conversation created ``age_days`` before ``pinned_now``.

    ``retention_days`` in ``{0, 30}`` (the DB CHECK constraint's only legal
    values) drives ``expires_at``; ``age_days`` independently controls how
    far in the PAST ``created_at``/``updated_at`` sit, so a caller can seed
    both an already-expired-by-policy row (``age_days > retention_days``,
    ``retention_days > 0``) and a fresh one for the SAME policy.
    """

    from dev_health_ops.models.dev_persistence import DevConversation

    created_at = pinned_now - timedelta(days=age_days)
    expires_at = (
        created_at + timedelta(days=retention_days) if retention_days > 0 else None
    )
    conversation = DevConversation(
        id=_uuid5("conversation", id_seed),
        org_id=org_id,
        user_id=user_id,
        title=title,
        current_scope={},
        retention_days=retention_days,
        created_at=created_at,
        updated_at=created_at,
        expires_at=expires_at,
        deleted_at=None,
    )
    return ConversationBundle(conversation=conversation, runs=(), subject_sets=())


def build_stale_context_conversation(
    *,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    id_seed: str,
    repo_full_name: str,
    pinned_now: datetime,
    stale_after_days: int = 9,
) -> ConversationBundle:
    """A conversation whose turn-1 committed subject predates turn 2 by
    ``stale_after_days`` -- realizes ``subjects.json``'s ``stale-context``
    class (mention: "it", referring to the prior turn's subject).
    """

    from dev_health_ops.models.dev_persistence import (
        DevConversation,
        DevRun,
        DevRunSubjectSet,
    )

    early_at = pinned_now - timedelta(days=stale_after_days)
    late_at = pinned_now - timedelta(hours=1)

    conversation = DevConversation(
        id=_uuid5("conversation", id_seed),
        org_id=org_id,
        user_id=user_id,
        title="stale-context probe",
        current_scope={"repository": repo_full_name},
        retention_days=30,
        created_at=early_at,
        updated_at=late_at,
        expires_at=early_at + timedelta(days=30),
        deleted_at=None,
    )

    early_run = DevRun(
        id=_uuid5("run", id_seed, "early"),
        request_id=_uuid5("request", id_seed, "early"),
        conversation_id=conversation.id,
        org_id=org_id,
        user_id=user_id,
        state="completed",
        contract_generation="v2",
        public_outcome="answered",
        started_at=early_at,
        ended_at=early_at,
        created_at=early_at,
        tool_call_count=1,
        citation_count=1,
    )
    subject_set = DevRunSubjectSet(
        id=_uuid5("subject-set", id_seed, "early"),
        run_id=early_run.id,
        org_id=org_id,
        user_id=user_id,
        set_id=_uuid5("subject-set-committed", id_seed, "early"),
        entity_kind="repository",
        cohort_complete=True,
        fingerprint=f"stale-context:{repo_full_name}",
        payload={"repo_full_name": repo_full_name},
        created_at=early_at,
    )
    late_run = DevRun(
        id=_uuid5("run", id_seed, "late"),
        request_id=_uuid5("request", id_seed, "late"),
        conversation_id=conversation.id,
        org_id=org_id,
        user_id=user_id,
        state="completed",
        contract_generation="v2",
        public_outcome="needs_clarification",
        started_at=late_at,
        ended_at=late_at,
        created_at=late_at,
        tool_call_count=0,
        citation_count=0,
    )
    return ConversationBundle(
        conversation=conversation,
        runs=(early_run, late_run),
        subject_sets=(subject_set,),
    )


def build_validation_packet(
    *,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    id_seed: str,
    pinned_now: datetime,
) -> ConversationBundle:
    """One conversation with one run per :data:`VALIDATION_STATUSES` value.

    A "packet" in the sense the Lane 1a task description names explicitly:
    a bounded, versioned set of runs exercising the full
    ``grounding_validation_status`` vocabulary a single answer can carry,
    for ``trust.*``/``scope.refused-with-grounding`` corpus cases to assert
    against.
    """

    from dev_health_ops.models.dev_persistence import DevConversation, DevRun

    created_at = pinned_now - timedelta(hours=2)
    conversation = DevConversation(
        id=_uuid5("conversation", id_seed),
        org_id=org_id,
        user_id=user_id,
        title="validation packet probe",
        current_scope={},
        retention_days=30,
        created_at=created_at,
        updated_at=created_at,
        expires_at=created_at + timedelta(days=30),
        deleted_at=None,
    )
    runs = []
    for offset, status in enumerate(VALIDATION_STATUSES):
        run_at = created_at + timedelta(minutes=offset)
        public_outcome = "answered" if status == "validated" else "answered_with_gaps"
        runs.append(
            DevRun(
                id=_uuid5("run", id_seed, status),
                request_id=_uuid5("request", id_seed, status),
                conversation_id=conversation.id,
                org_id=org_id,
                user_id=user_id,
                state="completed",
                contract_generation="v2",
                public_outcome=public_outcome,
                grounding_validation_status=status,
                started_at=run_at,
                ended_at=run_at,
                created_at=run_at,
                tool_call_count=1,
                citation_count=2 if status != "insufficient_evidence" else 0,
            )
        )
    return ConversationBundle(
        conversation=conversation, runs=tuple(runs), subject_sets=()
    )
