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

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    FiniteFloat,
    StringConstraints,
)

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
    "ANSWERED_OUTCOMES",
    "EMPTY_CONTENT_OUTCOMES",
    "Cardinality",
    "ContractModelV2",
    "DevRelationshipPath",
    "EntityKind",
    "EvidenceHandle",
    "FactDisclosure",
    "IdempotencyKey",
    "Label",
    "LongText",
    "NarrativeFailureCode",
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
    """The twelve Wave 3.1 launch intents (Amendment TRD v2 §4.1), plus the
    CHAOS-3652 graph-assisted subjectless-cohort-discovery intent.
    """

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
    #: CHAOS-3652: a question that names zero subjects but lexically
    #: describes a bounded team/project cohort-discovery job ("which teams
    #: are currently struggling?"), as distinct from a genuinely
    #: unbounded/ambiguous zero-mention question (which stays
    #: ``BOUNDED_INVESTIGATION``). Requires ``Cardinality.ORGANIZATION_WIDE``
    #: (``DevQuestionIntent.validate_intent_invariants``), but that is a
    #: **closed-universe** discovery job, never organization-wide sweep
    #: authorization: the graph-assisted route this intent triggers resolves
    #: it against a bounded, question-family-specific candidate universe
    #: derived server-side from the authorized scope (CHAOS-3645's
    #: ``graph_arm.cohort_discovery.discover_cohort`` proved this shape:
    #: family -> closed candidate-kind set -> authorized universe -> narrow
    #: via graph relationships/measurements -- never an unrestricted
    #: enumeration). ``ORGANIZATION_WIDE`` here only encodes "zero named
    #: mentions were extracted"; it must never be read, by any consumer, as
    #: authorization to sweep every entity in the organization. See
    #: CHAOS-3652/CHAOS-3660 for the full guardrail discussion.
    DISCOVERED_COHORT = "discovered_cohort"


class Cardinality(StrEnum):
    SINGULAR = "singular"
    PLURAL_COHORT = "plural_cohort"
    ORGANIZATION_WIDE = "organization_wide"


class FactDisclosure(StrEnum):
    """Per-fact disclosure flags — the v2-native, closed-vocabulary
    equivalent of v1 ``DevClaimFlags`` (``stale``/``uncertain``/
    ``conflicting``/``untrusted_source``, ``contracts.py:615-619``).

    Ratified on CHAOS-3297 ("DevAnswerFact flags gap — design ratified
    2026-08-02"): option (a), a per-fact field, because all four disclosures
    are per-claim in v1 and every existing frame channel is per-frame
    (``DevAnswerFrame.limitations``) or per-evidence
    (``DevFrameConflict``/``DevEvidenceRefV2``'s inherited v1
    ``DevEvidenceFlags``) — none of them is per-fact, so
    ``compat._project_answered`` could never reconstruct ``DevClaim.flags``
    from frame-level prose alone.

    Declared here rather than in ``frame.py``: ``no_answer_policy`` imports
    only ``base`` (the leaf that broke the cyclic-import graph — see that
    module's docstring), and ``compat.py`` asserts this vocabulary is a
    name-level bijection with ``DevClaimFlags.model_fields`` at import time,
    so both sides of the seam read it from the same leaf module rather than
    from ``frame``.

    Member order is the canonical declaration order ``DevAnswerFact.disclosures``
    enforces (strictly ascending, no duplicates) — the tuple is isomorphic to
    a 4-bit set, so two producers that disclose the same facts always emit
    byte-identical frames regardless of the order they discovered them in.

    Deliberately **not** derived from ``DevEvidenceFlags.untrusted_content``:
    that field defaults ``True`` on every evidence ref (see ``embedded.py``),
    so deriving ``UNTRUSTED_SOURCE`` from it would fire on nearly every fact
    and make ``answered`` unreachable. A disclosure is a claim about the
    *fact's own trustworthiness* (mirroring v1's model-authored
    ``DevClaimFlags``), never a mechanical fold over its evidence's flags.
    """

    STALE = "stale"
    UNCERTAIN = "uncertain"
    CONFLICTING = "conflicting"
    UNTRUSTED_SOURCE = "untrusted_source"


class NarrativeFailureCode(StrEnum):
    """The closed, run-persisted vocabulary for
    ``dev_runs.narrative_failure_code`` (CHAOS-3297 stack #4, migration
    0078's docstring names this module's package as the vocabulary's
    owner).

    Declared here rather than in ``answer_frames.narrative_fallback``
    (where the classification logic that produces it lives): both
    ``contracts_v2.stream`` (the ``answer.narrative_fallback`` SSE event's
    ``narrative_failure_code`` field) and ``persistence.service``
    (``update_run``'s own closed-vocabulary check, alongside its DB CHECK
    constraint) need to validate against this exact set, and neither may
    depend on ``answer_frames`` -- an orchestration-layer package that
    itself depends on ``contracts_v2``. Same leaf-module reasoning as
    ``FactDisclosure`` above. ``answer_frames.narrative_fallback``
    re-exports this name for backward compatibility rather than defining
    its own copy.

    Seven members are the issue's own acceptance-criteria failure modes
    (timeout, refusal, empty content, schema violation, output-budget
    exhaustion, unsafe prose, narrative grounding failure); the eighth,
    ``PROVIDER_UNKNOWN_FAILURE``, is the closed-vocabulary totality guard's
    catch-all for a provider exception outside the six typed
    ``NarrativeProviderError`` subclasses.
    """

    PROVIDER_TIMEOUT = "provider_timeout"
    PROVIDER_REFUSED = "provider_refused"
    PROVIDER_EMPTY_CONTENT = "provider_empty_content"
    PROVIDER_SCHEMA_VIOLATION = "provider_schema_violation"
    PROVIDER_OUTPUT_BUDGET_EXCEEDED = "provider_output_budget_exceeded"
    PROVIDER_UNSAFE_CONTENT = "provider_unsafe_content"
    NARRATIVE_GROUNDING_FAILED = "narrative_grounding_failed"
    PROVIDER_UNKNOWN_FAILURE = "provider_unknown_failure"


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
    #: CHAOS-3304: per-developer/per-team cognitive-load rollups
    #: (``user_metrics_daily``/``team_metrics_daily`` -- interruption load,
    #: context spread, review-request load, after-hours/weekend ratios).
    COGNITIVE_LOAD = "cognitive_load"
    #: CHAOS-3304: classified completed-work investment mix
    #: (``investment_metrics_daily`` -- new-value/KTLO/security/infra/
    #: unclassified delivery units).
    INVESTMENT_ALLOCATION = "investment_allocation"
    #: CHAOS-3297 stack #3: CHAOS-3302 code-owned health-rule evaluation
    #: (``health_rule_registry.evaluate_registry``) synthesized by
    #: ``ProjectHealthService``/``TeamHealthService``/``PortfolioStatusService``/
    #: ``TeamWorkloadService`` -- a *derived* judgment over several other
    #: source classes' already-canonical facts, never a primary source of
    #: its own. Shared by all four services: ``TeamWorkloadService`` returns
    #: the exact same ``HealthProfileResult`` shape ``TeamHealthService``
    #: does (its own docstring: "structurally this is TeamHealthService plus
    #: two extra sources"), and a portfolio batch is just several
    #: HEALTH_PROFILE observations flattened into one, each still tagged
    #: with its own subject_id -- no separate source class per plan.
    HEALTH_PROFILE = "health_profile"
    #: CHAOS-3297 stack #3: CHAOS-3305 ``OperationalDeficiencyService``'s
    #: ``deficiency.operational.v1`` inventory -- kept distinct from
    #: ``HEALTH_PROFILE`` because ``DeficiencyFinding``'s own wire shape
    #: (category/severity/remediation/verification, plus
    #: ``DeficiencyCategoryStatus``'s per-category evaluated/unevaluated
    #: split) has no ``HealthRuleFinding`` equivalent.
    DEFICIENCY_INVENTORY = "deficiency_inventory"
    #: CHAOS-3567 (flag-off scaffold, no runtime behavior change): reserves
    #: the wire vocabulary for the optional temporal-context source class
    #: CHAOS-3502 (amended 2026-08-07) authorizes designing. Deliberately
    #: inert as of this member's introduction: no ``DevInvestigationPlan``
    #: declares a ``DevSourceRequirement`` against it (verified exactly,
    #: not merely asserted -- see
    #: ``tests/api/dev/test_chaos_3567_temporal_context_source_class_stub.py``'s
    #: ``test_temporal_context_is_not_referenced_by_any_registered_plan``),
    #: which in turn makes it structurally impossible for any registered
    #: ``StepRegistry`` entry to carry this source class either --
    #: ``registry_validation.validate_registry`` rejects a step whose
    #: ``(source_class, adapter_id)`` doesn't match a declared requirement
    #: on its own plan (``registry_validation.py:118-135``,
    #: ``StepRequirementMismatchError``; that same test module's
    #: ``test_no_step_can_register_against_temporal_context_without_a_
    #: declared_requirement`` exercises the rejection directly), and
    #: ``data_health_service.NATIVE_EVIDENCE_SOURCES`` does not name it
    #: either (exact-tuple-pinned by the same test module). Full
    #: registry-impact design: Linear project doc "CHAOS-3567: Ask Dev
    #: temporal source — design + registry-impact map" (Context Fabric
    #: project). Real recognizer/plan/source wiring (the acr-precedented
    #: ``required=False`` ``DataHealthService`` branch, a dedicated plan
    #: document, registered steps) stays blocked on the CHAOS-3499 ADR and
    #: CHAOS-3500/3501 contracts.
    TEMPORAL_CONTEXT = "temporal_context"


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
#: Lowercase only. ``str(uuid.uuid4())`` — the mint, on every path — emits
#: lowercase, so accepting mixed case would be a grammar that describes
#: something the server never produces. Not a disclosure channel either way
#: (nothing non-hex fits), but a contract that admits values its own producer
#: cannot emit invites a second, divergent notion of "a valid handle".
ServerHandle = Annotated[
    str,
    StringConstraints(
        min_length=36,
        max_length=36,
        pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
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


class DevRelationshipPath(ContractModelV2):
    """One verifiable hop chain from a committed subject to supporting data.

    Lives in this leaf module (not ``result.py``, where it originated)
    because ``contracts_v2.deficiency`` needs it and also needs
    ``contracts_v2.result`` to import ``DeficiencyFinding`` in the other
    direction (CHAOS-3297 stack #3, ``DevSourceContent.deficiency_findings``)
    -- the same "both sides of the seam read from the same leaf module"
    reasoning ``FactDisclosure`` above was placed here for. ``result.py``
    re-exports this name unchanged, so no existing import site elsewhere in
    the package needed to change.
    """

    path_id: OpaqueID
    source_entity_id: OpaqueID
    relationship: OpaqueID
    target_entity_id: OpaqueID
    provenance: ShortText
    confidence: FiniteFloat = Field(ge=0, le=1)
    observed_at: AwareDatetime
    evidence_ref_ids: tuple[EvidenceHandle, ...] = Field(
        default_factory=tuple, max_length=25
    )


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

    ``REFUSED`` (CHAOS-3541) is reserved for genuinely prohibited requests
    (writes, arbitrary execution, secret extraction, cross-tenant access) --
    distinct from ``DENIED`` (an authorization claim: this scope is not
    yours) and from every evidence-gap outcome (``NOT_FOUND`` et al: ask
    again with more to go on). A refusal asserts neither -- the requester
    has access, and no amount of additional evidence changes the answer.
    Was previously never producible from any answer-frame outcome; this
    docstring itself predates the implementation that finally reaches it.
    """

    ANSWERED = "answered"
    ANSWERED_WITH_GAPS = "answered_with_gaps"
    NEEDS_CLARIFICATION = "needs_clarification"
    NOT_FOUND = "not_found"
    TEMPORARILY_UNAVAILABLE = "temporarily_unavailable"
    UNSUPPORTED = "unsupported"
    DENIED = "denied"
    FAILED = "failed"
    REFUSED = "refused"


#: Outcomes that MUST carry zero answer content (no sections/facts) because
#: no useful frame exists. Used by ``validators.validate_outcome_consistency``.
EMPTY_CONTENT_OUTCOMES = frozenset(
    {
        PublicOutcome.NOT_FOUND,
        PublicOutcome.TEMPORARILY_UNAVAILABLE,
        PublicOutcome.UNSUPPORTED,
        PublicOutcome.DENIED,
        PublicOutcome.FAILED,
        PublicOutcome.REFUSED,
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
