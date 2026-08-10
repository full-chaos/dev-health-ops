"""CHAOS-3502/CHAOS-3660: the production consumer-side graph-investigation seam.

**Status: PROPOSAL, posted on CHAOS-3660 for Lane A review** (Wave 3.2 §8
shared-contract protocol -- "graph query request/result contract" is a named
stable seam). Lane B (this orchestrator/routing lane) owns the *consumer*
side and is the one with an immediate need to call something; Lane A
(CHAOS-3500) owns the real production implementation. Per the ratified plan
(CHAOS-3660, team-lead), Lane B may bind orchestrator routing to this
``Protocol`` and its own fake now; Lane A implements against it or
counter-proposes, and team-lead arbitrates disagreement. Nothing here is a
final spec.

Why this shape, briefly (full reasoning in the CHAOS-3660 proposal comment):

* The **result** reuses :class:`~.investigation_contract.AskDevInvestigationPacket`
  wholesale rather than inventing a fourth output shape. It is already
  documented (``investigation_contract/__init__.py``) as "the shared, frozen
  wire shape every arm must emit... the input the Ask Dev frame reasons
  over" -- exactly what a production caller needs, and reusing it is what
  "no duplicate hand-maintained schemas" (handoff §8) asks for. Consuming it
  from a real caller is NOT "trial construction copied into production":
  the packet contract was built for cross-arm interop from the start.
* The **request** deliberately does NOT expose the frozen trial's
  ``QuestionFamilyID``/``AnalyticalJob`` vocabulary at this seam. That
  registry is documented as "the frozen question-family registry for the
  corrected trial" (``question_families.py``) with ten fixed families tied
  to the trial's own exact/natural phrasings -- coupling production routing
  to it directly would silently import trial-scoped assumptions into a
  production contract. Instead the request carries what production's own
  interpreter already computed (``QuestionIntentID``, ``Cardinality``,
  resolved/unresolved mentions) and leaves any internal family/job
  classification the graph service needs as Lane A's own implementation
  detail, on Lane A's side of the seam.
* **Authorization is supplied, never derived, at this seam.** Mirrors
  ``graph_arm.readback.derive_authorized_entity_ids``'s own documented gap
  (raises ``AuthorizationDerivationNotImplementedError`` today) -- the
  orchestrator already knows the authorized/committed scope from
  ``subject_preflight``/``scope_service`` before it would ever call this
  seam, so there is no reason for the graph side to re-derive it, and doing
  so would create two independent authorization computations that could
  disagree.
* **Deadline is absolute, not a duration.** CHAOS-3631 flagged unbounded
  graph transport with no timeout; an absolute ``datetime`` deadline removes
  the ambiguity a relative "give me 3 seconds" has once it crosses a queue,
  a retry, or a slow caller.
* **Transport/availability failure is a distinct axis from investigation
  outcome.** ``AskDevInvestigationPacket.outcome`` (``InvestigationOutcome``)
  only has semantic states (supported / needs_clarification / no_match /
  unsupported) -- nothing for "the store was unreachable" or "the deadline
  passed before this finished". Conflating the two would make a transport
  failure indistinguishable from a legitimate no-match, which the handoff's
  "missing/stale/unavailable/... remain distinct" guardrail forbids.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from .contracts_v2.base import Cardinality, QuestionIntentID
from .contracts_v2.subject import DevSubjectMention
from .investigation_contract import AskDevInvestigationPacket

__all__ = [
    "CohortDiscoveryFamily",
    "GraphInvestigationRequest",
    "GraphQueryOutcome",
    "GraphQueryResult",
    "GraphInvestigationQuery",
]


class CohortDiscoveryFamily(StrEnum):
    """The production-side classification signal for a ``DISCOVERED_COHORT``
    question -- CHAOS-3689 design round (proposed/signed off on CHAOS-3660).

    ``cohort_discovery.discover_cohort`` requires a ``QuestionFamilyID`` to
    select its candidate-kind universe (``FAMILY_CANDIDATE_KINDS``) and
    corroboration metric set (``FAMILY_PRESSURE_METRICS``, CHAOS-3667's
    held-out gate) -- a single ``QuestionIntentID`` value cannot carry that.
    This is production's OWN closed vocabulary for the signal, deliberately
    not a reuse of the trial's ten-member ``QuestionFamilyID`` -- Lane A's
    ``ProductionGraphInvestigationQuery`` maps each member here to a
    ``QuestionFamilyID`` representative via its own closed, exhaustive
    table (never a default branch).

    Deliberately two members, not five. Of the five subjectless-capable
    trial families, ``STRUGGLING_TEAMS`` and ``PRESSURE_SIGNALS`` are
    structurally identical to ``discover_cohort`` -- same candidate kind
    (``{TEAM}``), same seven-metric corroboration set, and for a
    ``DISCOVERED_COHORT``/``ORGANIZATION_WIDE`` request both permit
    ``(DISCOVERED_COHORT, PORTFOLIO_WIDE)`` -- so collapsing them here loses
    no observable behavior; it recognizes a real equivalence class, not a
    guess. ``PORTFOLIO_DEPENDENCY_RISK``/``DECLARED_VERSUS_ACTUAL`` have NO
    lexical coverage in ``question_interpreter``'s ``cohort.discovery``
    recognizer today -- adding anchors for them is a separate, future,
    measured pass (explicitly out of this round's "no corpus tuning"
    scope), not represented here. A question that would need one of those
    two families classifies as ``None`` (unclassifiable) today, exactly
    like any other question this recognizer doesn't yet cover.
    """

    #: STRUGGLING_TEAMS / PRESSURE_SIGNALS (equivalent for this call shape,
    #: see class docstring). Team-kind candidates, pressure-metric
    #: corroboration.
    TEAM_PRESSURE = "team_pressure"
    #: PROJECT_CAPACITY. Project-kind candidates, capacity-metric
    #: corroboration.
    PROJECT_CAPACITY = "project_capacity"


class GraphQueryOutcome(StrEnum):
    """The transport/availability axis -- distinct from the packet's own
    semantic ``InvestigationOutcome``. A caller must be able to tell "the
    graph route is unavailable, fall back" apart from "the graph route ran
    and found nothing", and this is the only field that distinction lives
    on.
    """

    #: The call completed before the deadline; ``GraphQueryResult.packet``
    #: is populated and its own ``outcome`` field carries the semantic
    #: result (which may itself be a refusal shape -- NO_MATCH,
    #: NEEDS_CLARIFICATION, UNSUPPORTED -- that is still a completed call).
    COMPLETED = "completed"
    #: Projection/read is disabled (organization or runtime flag off).
    #: Distinct from UNAVAILABLE: this is an intentional, configured state,
    #: not a failure.
    DISABLED = "disabled"
    #: The graph store or service could not be reached at all.
    UNAVAILABLE = "unavailable"
    #: The projection is reachable but known stale beyond the caller's
    #: tolerance (watermark lag). Distinct from UNAVAILABLE -- data exists,
    #: it is just not current enough to answer from.
    STALE = "stale"
    #: The absolute deadline passed before the call completed.
    DEADLINE_EXCEEDED = "deadline_exceeded"
    #: The caller's own cancellation token fired (e.g. the parent Ask Dev
    #: run was cancelled) before the call completed.
    CANCELLED = "cancelled"
    #: Any other provider-side failure the query service could not recover
    #: from. Named rather than silently mapped to UNAVAILABLE so a caller
    #: can distinguish "never reachable" from "reached, then broke".
    PROVIDER_FAILURE = "provider_failure"


@dataclass(frozen=True, slots=True)
class GraphInvestigationRequest:
    """What the orchestrator already knows before it would call the graph
    seam -- nothing here is derived BY the graph side; everything is
    supplied.
    """

    org_id: str
    run_id: str
    #: Production's own interpreted intent (now including
    #: ``QuestionIntentID.DISCOVERED_COHORT``, CHAOS-3652) and its cardinality.
    #: For everything except cohort-family selection, the graph service
    #: classifies its own internal job/shape from these two fields alone --
    #: see ``cohort_discovery_family`` below for the one signal these two
    #: cannot carry (CHAOS-3689).
    intent_id: QuestionIntentID
    cardinality: Cardinality
    #: Already-extracted mentions (resolved or not -- resolution status is
    #: NOT communicated here; see ``authorized_entity_ids`` below for what
    #: the graph route may actually touch). Empty for
    #: ``Cardinality.ORGANIZATION_WIDE`` (structurally, per
    #: ``DevQuestionIntent.validate_intent_invariants``).
    mentions: tuple[DevSubjectMention, ...]
    #: The bounded (<=8KiB per ``DevMessageRequestV2``), already-validated
    #: question text. Server-side/internal only -- never echoed back
    #: verbatim into a packet field a consumer renders untrusted (mirrors
    #: the existing "no raw question text in logs/traces/labels" guardrail;
    #: this is a same-process/internal call, not a log). Per the CHAOS-3660
    #: determination, the query service does not inspect this to classify
    #: (see ``mechanism_for``'s own docstring) -- it is carried for the
    #: mechanism's own downstream matching needs, not for classification.
    question_text: str
    #: Supplied, never derived (see module docstring). The graph route must
    #: never return, rank, or count toward truncation any entity outside
    #: this set -- mirrors ``graph_arm.discover_cohort``'s own "authorization
    #: applied on the way IN" invariant.
    authorized_entity_ids: frozenset[str]
    #: The bounded analytical window the run itself is scoped to --
    #: CHAOS-3678 follow-up: the graph seam must never invent a product time
    #: policy of its own, so this is supplied, exactly like
    #: ``authorized_entity_ids`` above. The orchestrator populates both from
    #: the SAME ``DevScope.time_range`` that already bounds every other
    #: tool/metric execution for this run (``StepContext(scope=
    #: authorized_scope, ...)`` at the plan-executor call site) -- never a
    #: separately-invented window, so a graph-assisted answer and a
    #: native-arm answer for the same run are bounded identically.
    window_start: datetime
    window_end: datetime
    #: The one production-derived classification signal this seam carries
    #: (CHAOS-3689/CHAOS-3660) -- present only because ``(intent_id,
    #: cardinality)`` cannot select a ``discover_cohort`` candidate-kind/
    #: metric-family pair by itself. Supplied by the orchestrator's own
    #: interpreter-level classification
    #: (``question_interpreter.classify_cohort_discovery_family``), never
    #: derived here. There is no "unclassifiable" member on
    #: ``CohortDiscoveryFamily`` -- the orchestrator never constructs a
    #: request for a question it could not classify (see the routing
    #: branch's own gate), so this field is never ambiguous by the time it
    #: reaches this seam.
    cohort_discovery_family: CohortDiscoveryFamily
    #: Absolute wall-clock deadline (CHAOS-3631). The query service must
    #: return ``GraphQueryOutcome.DEADLINE_EXCEEDED`` rather than block past
    #: this, under any internal retry/backoff it performs.
    deadline: datetime
    #: Bounded per the handoff's "max nodes, paths, depth, rows, bytes,
    #: time, output budgets" requirement. Left as a generic mapping here
    #: deliberately -- the concrete budget shape (node/path/hop/byte caps)
    #: is exactly the kind of closed vocabulary CHAOS-3500 should define and
    #: own; this proposal does not presume it.
    budget_hints: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GraphQueryResult:
    outcome: GraphQueryOutcome
    #: Populated iff ``outcome is GraphQueryOutcome.COMPLETED``. Its own
    #: ``.outcome: InvestigationOutcome`` field carries the semantic result.
    packet: AskDevInvestigationPacket | None = None
    #: A short, content-safe (no raw question/entity text) diagnostic for
    #: non-COMPLETED outcomes -- logged and telemetered, never shown to a
    #: user verbatim.
    diagnostic: str | None = None

    def __post_init__(self) -> None:
        if (self.outcome is GraphQueryOutcome.COMPLETED) != (self.packet is not None):
            raise ValueError(
                "GraphQueryResult.packet must be set if and only if "
                "outcome is COMPLETED"
            )


class GraphInvestigationQuery(Protocol):
    """The one call orchestrator routing makes into the graph-assisted seam.

    Implementations MUST NOT raise for an ordinary unavailable/stale/
    timeout/cancelled condition -- those are ``GraphQueryOutcome`` values,
    not exceptions, so a caller can never accidentally let an unhandled
    exception from this seam take down an otherwise-valid Ask Dev run
    (mirrors ``investigation_shadow``'s "total exception containment"
    posture, but for a route that DOES affect the answer, not a shadow).
    An implementation MAY raise for a genuine programming error (a
    malformed request), which is a bug in the caller, not a runtime
    condition to model.
    """

    async def investigate(self, request: GraphInvestigationRequest) -> GraphQueryResult:
        pass
