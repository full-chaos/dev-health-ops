"""Shared primitives for the Ask Dev Wave 3.1 v2 contract package.

These re-export the provider-neutral scalar types from the v1 module
(``dev_health_ops.api.dev.contracts``) so v2 contracts share the exact same
wire-level string/ID constraints, and add the small set of v2-only enums
(``EntityKind`` — a v1 ``EntityType`` superset that adds ``TEAM`` as a
first-class kind at the contract level without mutating the frozen v1
enum — and ``QuestionIntentID``, the closed Wave 3.1 launch-intent
registry).

See ``docs/contribute/architecture/ask-dev-contracts-v2.md`` for the full
contract map and the TRD/PRD cross-references.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints

from dev_health_ops.api.dev.contracts import (
    Label,
    LongText,
    OpaqueID,
    RelativePath,
    SchemaVersion,
    ShortText,
    TimezoneName,
    Version,
)

__all__ = [
    "Cardinality",
    "ContractModelV2",
    "EntityKind",
    "EvidenceHandle",
    "Label",
    "LongText",
    "OpaqueID",
    "PlatformVersionToken",
    "PublicOutcome",
    "QuestionIntentID",
    "RelativePath",
    "SchemaVersion",
    "ServerHandle",
    "ShortText",
    "SourceClass",
    "SourceRequirementState",
    "TimezoneName",
    "Version",
]


class ContractModelV2(BaseModel):
    """Strict immutable base for every public v2 contract object.

    Identical posture to v1's ``ContractModel``: unknown fields are a hard
    validation error (``extra="forbid"``) and instances are immutable
    (``frozen=True``) once constructed. Immutability is also the structural
    mechanism behind the resolution ledger's append-only guarantee — a
    ``dev_resolution_ledger.v1`` instance cannot be mutated in place, only
    superseded by constructing a new instance whose ``entries`` extend the
    previous one (enforced by
    ``dev_health_ops.api.dev.contracts_v2.subject.validate_ledger_extends``).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)


class EntityKind(StrEnum):
    """Wave 3.1 subject-kind registry.

    Supersets v1's ``EntityType`` with ``TEAM`` as a first-class kind, per
    Amendment TRD v2 §4.1 ("Add TEAM to EntityType and DirectScope").
    Defined here rather than by mutating the frozen v1 ``EntityType`` enum,
    because giving ``TEAM`` real v1 ``DevScope``/``DirectScope`` semantics
    (a matching entity_scope branch, a required team ref, orchestrator/
    scope_service wiring) is v1 *runtime* behavior, which CHAOS-3294 is
    explicitly scoped to leave untouched. v2 subject/resolution/plan
    contracts use this enum instead of the v1 ``EntityType``.
    """

    REPOSITORY = "repository"
    PROJECT = "project"
    WORK_UNIT = "work_unit"
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"
    TEAM = "team"


class QuestionIntentID(StrEnum):
    """The twelve Wave 3.1 launch intents (Amendment TRD v2 §4.1)."""

    ENTITY_STATUS = "entity_status"
    PORTFOLIO_STATUS = "portfolio_status"
    REMAINING_WORK = "remaining_work"
    OBSERVED_CHANGE = "observed_change"
    REGISTERED_STATISTICS = "registered_statistics"
    METRIC_COMPARISON = "metric_comparison"
    DATA_TRUST = "data_trust"
    PROJECT_HEALTH = "project_health"
    TEAM_HEALTH = "team_health"
    TEAM_WORKLOAD_BALANCE = "team_workload_balance"
    OPERATIONAL_DEFICIENCY_INVENTORY = "operational_deficiency_inventory"
    BOUNDED_INVESTIGATION = "bounded_investigation"


class Cardinality(StrEnum):
    SINGULAR = "singular"
    PLURAL_COHORT = "plural_cohort"
    ORGANIZATION_WIDE = "organization_wide"


class SourceRequirementState(StrEnum):
    """Per-source fulfillment state (CHAOS-3294 deliverable list, verbatim).

    Distinct from ``dev_source_requirement.v1.requirement_level`` (the
    a-priori ``mandatory | conditional | optional | not_applicable``
    declaration): this enum is the *observed*, executed outcome recorded on
    ``dev_source_observation.v1.observed_state`` once a plan step actually
    ran. Kept in ``base`` because both the plan and result contract modules
    reference it (the plan's applicability rules describe which states are
    acceptable; the result records which state was actually observed).
    """

    AVAILABLE_CURRENT = "available_current"
    AVAILABLE_STALE = "available_stale"
    AVAILABLE_UNKNOWN = "available_unknown"
    UNCONFIGURED = "unconfigured"
    UNAVAILABLE = "unavailable"
    UNAUTHORIZED_OR_NOT_VISIBLE = "unauthorized_or_not_visible"
    NOT_APPLICABLE = "not_applicable"
    TRUNCATED = "truncated"


class SourceClass(StrEnum):
    """The closed platform vocabulary of investigable source classes.

    A *source class* names a kind of platform data an investigation step can
    draw on. It is deliberately a closed enum rather than an ``OpaqueID``,
    because the same token is what ``dev_coverage``'s
    ``unavailable_required_sources``/``stale_required_sources`` disclose on a
    **denied** answer (see ``validators.NO_ANSWER_FRAME_FIELD_POLICY``):
    adversarial review round 3 used the free-form ``OpaqueID`` shape to smuggle
    a subject-derived identifier (``"private/Nightfall"``) out through a frame
    whose every other field was correctly empty. A closed vocabulary means the
    coverage counts stay answerable for a denial while carrying nothing a
    producer chose.

    Adding a source class is therefore a deliberate contract change, not an
    incidental string: a new adapter must add its member here (and regenerate
    the exported schemas) before it can appear on the wire.
    """

    STATUS_CHANGE = "status_change"
    WORK_ITEM = "work_item"
    WORK_GRAPH = "work_graph"
    PULL_REQUEST = "pull_request"
    CODE_CHANGE = "code_change"
    REVIEW = "review"
    CI_RUN = "ci_run"
    TEST_REPORT = "test_report"
    DEPLOYMENT = "deployment"
    INCIDENT = "incident"
    OPERATIONAL_CONTROL = "operational_control"
    SOURCE_HEALTH = "source_health"


#: A **server-minted opaque handle**: the canonical hyphenated UUID form.
#:
#: Pinned to what the Ask Dev persistence layer actually mints and stores.
#: Every correlation identifier in ``models/dev_persistence.py`` is a ``GUID``
#: column with ``default=uuid.uuid4`` — ``DevConversation.id`` (line 45),
#: ``DevMessage.id``/``client_message_id``/``answer_id`` (106-113),
#: ``DevRun.id``/``request_id``/``retry_of_run_id``/``answer_id`` (171-182) —
#: and the router serializes them with ``str(...)``
#: (``router.py:305,318,483,607``), while ``orchestrator_persistence.py:149``
#: parses ``uuid.UUID(answer.answer_id)`` on the way back in. A non-UUID
#: ``answer_id`` was therefore already a runtime failure; this makes the wire
#: contract say so.
#:
#: Why it matters beyond tidiness: an identifier is the last field class on a
#: **denied** answer that a producer could otherwise fill with a
#: subject-derived string. Round 3 left ``frame_id``/``run_id`` as a documented
#: residual and round 4 found the same hole one level out, on the answer
#: envelope's ``answer_id``/``conversation_id``. Hex digits and hyphens cannot
#: spell a project name, so this closes the class rather than the instance.
#:
#: Applied **uniformly across every outcome**, deliberately: a run ID is minted
#: at ``run.started`` before the outcome is known, so a grammar that applied
#: only to no-answer outcomes would make a legal server behaviour
#: unrepresentable depending on how the run happened to end.
#:
#: Case-insensitive because ``uuid.UUID()`` accepts either and a re-serialized
#: handle must not fail; the mint itself is lowercase.
ServerHandle = Annotated[
    str,
    StringConstraints(
        min_length=36,
        max_length=36,
        pattern=(
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
            r"-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
        ),
    ),
]

#: An **evidence handle**: ``ev1_`` followed by 40 lowercase hex characters.
#:
#: Not a UUID, and deliberately so — this one is a keyed HMAC, not a mint.
#: ``evidence_service.EvidenceHandleService.issue`` returns
#: ``f"ev1_{digest.hexdigest()[:40]}"`` over an org-scoped payload, and
#: ``verify`` recomputes it and ``hmac.compare_digest``s the result, so the
#: handle is the authorization token for dereferencing evidence. Pinning the
#: exact shape here means a handle that could never verify cannot reach the
#: wire, and — like ``ServerHandle`` — hex digits cannot spell a subject name.
EvidenceHandle = Annotated[
    str,
    StringConstraints(min_length=44, max_length=44, pattern=r"^ev1_[0-9a-f]{40}$"),
]

#: A platform provenance token: a dotted, lowercase, version-suffixed
#: identifier such as ``intent_interpreter.v1``, ``status.entity.v2.1`` or
#: ``ask_dev_queries.v1``. Every string on ``DevFrameVersions`` uses this
#: rather than the free-form ``Version`` (1-128 arbitrary characters) so a
#: provenance block cannot carry producer-authored copy at all — the round-3
#: counterexample put ``"private/Nightfall"`` in ``versions.plan_id``, which
#: the whitespace-free ``IDENTIFIER`` predicate admitted.
PlatformVersionToken = Annotated[
    str,
    StringConstraints(
        min_length=3,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v\d+(?:\.\d+)*$",
    ),
]


class PublicOutcome(StrEnum):
    """The exact dev_answer.v2 public outcome vocabulary (PRD v2 §8).

    ``refused`` is deliberately absent: it is reserved for genuinely
    prohibited requests (writes, arbitrary execution, secret extraction,
    cross-tenant access) and is never a Wave 3.1 answer-frame outcome for a
    normal status/health question that could not be resolved.
    """

    ANSWERED = "answered"
    ANSWERED_WITH_GAPS = "answered_with_gaps"
    NEEDS_CLARIFICATION = "needs_clarification"
    NOT_FOUND = "not_found"
    TEMPORARILY_UNAVAILABLE = "temporarily_unavailable"
    UNSUPPORTED = "unsupported"
    DENIED = "denied"
    FAILED = "failed"


#: Outcomes that MUST carry zero answer content (no sections/facts) because
#: no useful frame exists. Used by ``validators.validate_outcome_consistency``.
EMPTY_CONTENT_OUTCOMES = frozenset(
    {
        PublicOutcome.NOT_FOUND,
        PublicOutcome.TEMPORARILY_UNAVAILABLE,
        PublicOutcome.UNSUPPORTED,
        PublicOutcome.DENIED,
        PublicOutcome.FAILED,
        PublicOutcome.NEEDS_CLARIFICATION,
    }
)

#: Outcomes that represent a genuine, server-owned answer frame.
ANSWERED_OUTCOMES = frozenset(
    {PublicOutcome.ANSWERED, PublicOutcome.ANSWERED_WITH_GAPS}
)


IdempotencyKey = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:/#-]{0,127}$",
    ),
]
