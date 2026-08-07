"""Wire shapes the CHAOS-3499 shadow trial measures against.

These mirror the amended PRD/TRD §6 (temporal fact), §10 (internal query
contract) and §11.2 (packet integration) closely enough for the trial's
oracles to assert against them, but they are **trial-local**: nothing here is
a product contract, and nothing here may be imported by ``src/``.

Every arm (native increments, episode readback, Graphiti, direct graph store)
adapts its own output into :class:`ArmResponse`. That adapter is the only
arm-specific code the oracles ever see, which is what keeps a single oracle
comparable across arms.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

# --------------------------------------------------------------------------
# Enumerations
# --------------------------------------------------------------------------


class TimeAxis(str, Enum):
    """PRD §10: ``as_of`` alone does not say which axis it queries.

    ``VALID_TIME``    -- when the fact was true in the world.
    ``OBSERVED_TIME`` -- when Dev Health first knew the fact (belief time).

    A backfilled blocker has different correct answers on the two axes; that
    divergence is corpus case 19 (axis pairs) and is the single cheapest way
    to catch an arm that silently ignores the axis field.
    """

    VALID_TIME = "valid_time"
    OBSERVED_TIME = "observed_time"


class ClaimKind(str, Enum):
    """PRD §7.2. ``INFERRED`` may never satisfy a canonical requirement."""

    OBSERVED = "observed"
    INFERRED = "inferred"


class QueryMode(str, Enum):
    TIMELINE = "timeline"
    PRIOR_ATTEMPTS = "prior_attempts"
    SUPERSEDED_DECISIONS = "superseded_decisions"
    RELATED_CHANGES = "related_changes"
    RECURRING_PATTERNS = "recurring_patterns"
    CONFLICTS = "conflicts"
    AS_OF = "as_of"


class QuestionClass(str, Enum):
    """PRD §15.2 -- the ADR must report every gate per class, not aggregated.

    An aggregate score hides the only result that matters: whether the graph
    helps anywhere other than class (c).
    """

    NATIVE_ANSWERABLE = "a"
    NEEDS_DECLARED_STATE_HISTORY = "b"
    NEEDS_EXTRACTION_OR_ASSOCIATION = "c"


class ArmOutcome(str, Enum):
    """Why a response looks the way it does.

    ``NOT_RUN`` exists so that a measurement which never happened is
    representable, and therefore assertable. The runner converts it into a
    hard failure; there is deliberately no ``SKIPPED`` member, because a skip
    that reads as a pass is the failure mode this trial is most exposed to.
    """

    ANSWERED = "answered"
    UNAVAILABLE = "unavailable"
    NOT_RUN = "not_run"


# --------------------------------------------------------------------------
# Value objects
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class EntityRef:
    """A canonical Dev Health entity. The graph must never mint its own id."""

    kind: str
    id: str

    def __str__(self) -> str:  # pragma: no cover - debug aid
        return f"{self.kind}:{self.id}"


@dataclass(frozen=True)
class FactFlags:
    conflicting: bool = False
    stale: bool = False
    untrusted_content: bool = False


@dataclass(frozen=True)
class Invalidation:
    """PRD §6.3: who closed the validity window, and on what claim kind.

    Required whenever ``valid_to`` is non-null and did not come from an
    explicit canonical record. Without it an ``observed`` fact can carry an
    LLM-judged endpoint, which dodges the §16 inferred-fact gate through the
    endpoint instead of through the claim.
    """

    refs: tuple[str, ...]
    invalidation_claim_kind: ClaimKind


@dataclass(frozen=True)
class TemporalFact:
    fact_id: str
    subject_ref: EntityRef
    predicate: str
    object_ref: EntityRef
    observed_at: datetime
    claim_kind: ClaimKind
    projection_version: str
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    reference_time: datetime | None = None
    invalidated_by: Invalidation | None = None
    evidence_refs: tuple[str, ...] = ()
    source_event_refs: tuple[str, ...] = ()
    confidence: float | None = None
    flags: FactFlags = field(default_factory=FactFlags)


@dataclass(frozen=True)
class SourceCoverage:
    """Whether a source could contribute, and if not, why.

    The distinction between "no history exists" and "the source that would
    hold the history is unavailable or empty" is the whole point of corpus
    case 16 (squash-merge org).
    """

    source: str
    available: bool
    reason: str | None = None
    row_estimate: int | None = None


@dataclass(frozen=True)
class TemporalContextQuery:
    """PRD §10 ``temporal_context_query.v1``."""

    subjects: tuple[EntityRef, ...]
    query_mode: QueryMode
    as_of: datetime | None = None
    axis: TimeAxis | None = None
    time_range_start: datetime | None = None
    time_range_end: datetime | None = None
    allowed_relation_types: tuple[str, ...] = ()
    max_results: int = 20
    schema_version: str = "temporal_context_query.v1"

    def __post_init__(self) -> None:
        if self.query_mode is QueryMode.AS_OF and self.axis is None:
            raise ValueError(
                "PRD §10: `axis` is required whenever query_mode is `as_of`; "
                "an unpinned as-of question has two different correct answers."
            )


@dataclass(frozen=True)
class ArmResponse:
    """What an arm returned, normalised. Oracles see only this."""

    arm: str
    outcome: ArmOutcome
    query: TemporalContextQuery | None = None
    facts: tuple[TemporalFact, ...] = ()
    warnings: tuple[str, ...] = ()
    degraded_reasons: tuple[str, ...] = ()
    source_coverage: Mapping[str, SourceCoverage] = field(default_factory=dict)
    indexed_through: datetime | None = None
    versions: Mapping[str, str] = field(default_factory=dict)
    truncated: bool = False

    @classmethod
    def not_run(cls, arm: str, reason: str) -> ArmResponse:
        """Build the response that represents *no measurement happened*.

        Callers must use this instead of skipping, so the absence of a
        measurement is carried into the report as a failure rather than
        vanishing into a green summary line.
        """
        return cls(
            arm=arm,
            outcome=ArmOutcome.NOT_RUN,
            degraded_reasons=(f"measurement_not_run:{reason}",),
        )


def evidence_refs_of(facts: Sequence[TemporalFact]) -> frozenset[str]:
    return frozenset(ref for fact in facts for ref in fact.evidence_refs)
