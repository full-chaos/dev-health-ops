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
    "Label",
    "LongText",
    "OpaqueID",
    "PublicOutcome",
    "QuestionIntentID",
    "RelativePath",
    "SchemaVersion",
    "ShortText",
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
