"""CHAOS-3297 stack #1 -- frame construction for the orchestrator's own terminals.

``orchestrator.finish()`` (P1: frame-mandatory terminal) must persist a
``dev_answer_frame.v1`` on every terminal path, not only the CHAOS-3292
preflight TERMINATE branch C0 already covers. This module builds that frame
for the two shapes ``finish()`` sees before CHAOS-3297's stacks #3/#4 land a
real plan-executor-driven builder pipeline:

* an orchestrator-level ``error(code, message, retryable=...)`` result (the
  ~30 non-preflight terminal call sites in ``orchestrator.run()``), via
  ``build_error_frame``;
* a fully-validated legacy v1 ``DevAnswer`` from today's pre-CHAOS-3295
  model-tool-choice loop (``orchestrator.py:1389,1440``), via
  ``wrap_legacy_answer_as_frame``.

Plan amendment, ratified on CHAOS-3297 (see the Linear comment this
changeset adds): the plan's ``finish()`` description says it "derives the
v1 DevAnswer/DevError from [the frame] via the *one* projector"
(``contracts_v2/compat.project_answer_v2_to_v1``). That is NOT what this
module does, deliberately. ``project_answer_v2_to_v1``'s error table
(``compat._ERROR_OUTCOME_CODES``) is closed over five fixed v1 codes; routing
every orchestrator error through it would silently rewrite roughly seven of
the ten codes the orchestrator emits today (e.g. today's ``scope_forbidden``
would become ``forbidden``), which is a v1 wire-vocabulary change the plan's
own non-goals section rules out. So the frame this module builds is
structural metadata -- it makes ``record_frame`` / ``contract_generation =
'v2'`` / the CHAOS-3299 replay gate universally reachable -- but the v1
``DevAnswer``/``DevError`` object ``finish()`` hands to ``terminal()`` is
still the exact object the call site already built. See
``router._replayed_result``'s companion guard (CHAOS-3297) for the replay
side of the same fidelity requirement: an orchestrator-native error code must
keep replaying via ``run.safe_error_code`` (today's exact mechanism), never
via this module's frame, or live and replay would diverge on retry.

``DevClaimFlags`` (stale/uncertain/conflicting/untrusted_source) round-trips
through ``wrap_legacy_answer_as_frame`` via ``DevAnswerFact.disclosures``
(CHAOS-3297 flags gap, ratified 2026-08-02): each claim's set flags are
folded into the fact's disclosure tuple in canonical (enum) order. See
``contracts_v2.base.FactDisclosure`` and ``contracts_v2.compat``'s
import-time bijection assertion against ``DevClaimFlags.model_fields``.

Forward guidance for stack #4/#5's real frame-only builders (not a build
order for this module today -- CHAOS-3297 stack #3 never constructs a NEW
``DevAnswerFact`` from ``DevInvestigationResult`` content; it only embeds
``HealthRuleFinding``/``DeficiencyFinding`` objects directly on
``DevAnswerFrame.health_findings``/``deficiency_findings``, neither of
which has a ``disclosures``-equivalent field): when a future builder DOES
construct a ``DevAnswerFact`` from a ``DevSourceObservation``/
``DevSourceContent`` fact (a status fact, a pull request, a CI check, ...),
its ``disclosures`` must derive from that observation's own
``SourceRequirementState`` (``AVAILABLE_STALE`` -> ``FactDisclosure.STALE``,
``AVAILABLE_UNKNOWN`` -> ``FactDisclosure.UNCERTAIN``, etc.) -- NEVER from
``DevEvidenceRefV2.flags.untrusted_content``, which defaults ``True`` on
every minted handle (see ``production_runtime._mint_evidence``) and would
mark nearly every fact ``UNTRUSTED_SOURCE`` if used this way, making
``answered`` structurally unreachable for any observation-backed fact.
Package placement note: this rule belongs in THIS module (not a new
``answer_frames`` package) -- lane-3297-s4 already created that package on
its own branch for the narrative-fallback work, so minting a second one
here would collide.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .contracts import (
    ClaimKind,
    DevAnswer,
    DevClaimFlags,
    DevContractVersions,
    DevMetricRef,
)
from .contracts import ToolID as _ToolID
from .contracts_v2.base import (
    FactDisclosure,
    PlatformVersionToken,
    PublicOutcome,
    SourceClass,
)
from .contracts_v2.deficiency import DeficiencyCategoryStatus, DeficiencyFinding
from .contracts_v2.embedded import (
    DevCoverageV2,
    DevEvidenceRefV2,
    DevMetricRefV2,
    MetricEvidenceClassification,
)
from .contracts_v2.frame import (
    DevAnswerFact,
    DevAnswerFrame,
    DevAnswerSection,
    DevCompletionBlock,
    DevFrameConflict,
    DevFrameVersions,
)
from .contracts_v2.health_rules import HealthRuleFinding
from .contracts_v2.no_answer_policy import CANONICAL_NO_ANSWER_COPY, NO_ANSWER_OUTCOMES
from .contracts_v2.result import DevInvestigationResult, DevSourceContent
from .investigation_plans.wave_3_1_plans import (
    capped_deficiency_findings,
    capped_health_findings,
)

__all__ = [
    "LEGACY_ANSWER_PLAN_ID",
    "ORCHESTRATOR_ERROR_CODES",
    "PUBLIC_OUTCOME_BY_ERROR_CODE",
    "SOURCE_CLASS_BY_TOOL_ID",
    "UnregisteredTerminalCode",
    "build_error_frame",
    "tolerant_parse_legacy_frame_payload",
    "wrap_legacy_answer_as_frame",
]


class UnregisteredTerminalCode(ValueError):
    """A terminal error code outside ``ORCHESTRATOR_ERROR_CODES`` (CHAOS-3297
    Codex review round 2 MEDIUM #2).

    Deliberately a distinct type from a bare ``ValueError``, raised only by
    ``build_error_frame``'s own registry check: ``orchestrator.finish()``
    catches exactly this to fall back to the always-registered
    ``"internal_error"`` bucket. Catching a bare ``ValueError`` there would
    also silently swallow an unrelated construction failure (e.g. a genuine
    data problem inside ``wrap_legacy_answer_as_frame``) into the same
    fallback, masking a different bug behind the same safe-looking
    degradation.
    """


#: Stable namespace for every UUID5 this module mints -- distinct from
#: ``preflight_outcomes._HANDLE_NAMESPACE`` so a frame/section/fact id
#: minted here can never collide with one minted there for the same input
#: string (P3: deterministic identity).
_NAMESPACE = uuid.UUID("2c8e9a34-6a63-5b8b-9a5b-6f9e9c9d9a11")

#: The clarification copy an orchestrator-level ``scope_ambiguous``
#: termination renders. Distinct constant (not imported from
#: ``preflight_outcomes.CLARIFICATION_COPY``) because that table is keyed by
#: a *preflight* per-mention disambiguation reason
#: (``ambiguous``/``uninterpretable``) that does not apply to a scope- or
#: disambiguation-level ambiguity raised by the legacy model loop.
_SCOPE_AMBIGUOUS_COPY = (
    "More than one authorized scope matches this request. Please ask again "
    "naming exactly which one you mean."
)

#: The exact v1 error codes ``orchestrator.run()``'s own ``error(...)``
#: closure constructs, **plus** every code ``DevOrchestrator._provider_error``
#: can produce (verified by ``test_terminal_frames.py`` against the live
#: source of ``orchestrator.py`` -- P2 totality). Both producers feed the
#: same ``finish()`` call (CHAOS-3297 Codex review HIGH #2): a provider
#: failure classified by ``_provider_error`` reaches ``finish()`` exactly
#: like a local ``error(...)`` call, and before this registry covered its
#: codes too, a routine provider failure (e.g. ``OUTPUT_EXHAUSTED`` ->
#: ``model_not_supported``) made ``build_error_frame`` raise on an
#: "unregistered code", which ``finish()``'s broad exception handler
#: silently swallowed -- committing a FAILED terminal run with zero frames,
#: violating the frame-mandatory invariant on an unexceptional failure mode.
#:
#: Never the preflight's *own* codes when they originate from
#: ``preflight_outcomes.project_preflight_error`` (``source_unavailable``,
#: ``forbidden``): that termination records its frame *before* ``finish()``
#: is called (``frame_already_recorded=True``), so it never reaches
#: ``build_error_frame`` regardless of this registry. ``feature_not_enabled``
#: is the one code both producers can emit -- from the preflight (frame
#: already recorded, never reaches here) and from ``_provider_error`` via
#: ``AgentProviderErrorCode.DISABLED`` (frame *not* already recorded, does
#: reach here) -- registered for the latter origin.
ORCHESTRATOR_ERROR_CODES = frozenset(
    {
        "answer_validation_failed",
        "cancelled",
        "cost_limit_reached",
        "feature_not_enabled",
        "insufficient_evidence",
        "internal_error",
        "invalid_request",
        "model_not_supported",
        "provider_contract_violation",
        "provider_not_configured",
        "provider_unavailable",
        "rate_limited",
        "scope_ambiguous",
        "scope_forbidden",
        "scope_not_found",
        "tool_limit_reached",
        "tool_unavailable",
    }
)

#: Total over ``ORCHESTRATOR_ERROR_CODES``: which frame-level bucket each
#: orchestrator error code carries for replay-gate/outcome-tagging purposes.
#: This bucket is NEVER what decides the v1 wire code or message -- the
#: call site's own ``error(code, message, ...)`` object is what ``finish()``
#: hands to ``terminal()`` unchanged. See the module docstring.
PUBLIC_OUTCOME_BY_ERROR_CODE: Mapping[str, PublicOutcome] = {
    "scope_ambiguous": PublicOutcome.NEEDS_CLARIFICATION,
    "scope_not_found": PublicOutcome.NOT_FOUND,
    "insufficient_evidence": PublicOutcome.NOT_FOUND,
    "scope_forbidden": PublicOutcome.DENIED,
    "tool_unavailable": PublicOutcome.TEMPORARILY_UNAVAILABLE,
    "provider_unavailable": PublicOutcome.TEMPORARILY_UNAVAILABLE,
    "provider_not_configured": PublicOutcome.TEMPORARILY_UNAVAILABLE,
    "rate_limited": PublicOutcome.TEMPORARILY_UNAVAILABLE,
    "feature_not_enabled": PublicOutcome.UNSUPPORTED,
    "model_not_supported": PublicOutcome.UNSUPPORTED,
    "cancelled": PublicOutcome.FAILED,
    "cost_limit_reached": PublicOutcome.FAILED,
    "tool_limit_reached": PublicOutcome.FAILED,
    "answer_validation_failed": PublicOutcome.FAILED,
    "internal_error": PublicOutcome.FAILED,
    "invalid_request": PublicOutcome.FAILED,
    "provider_contract_violation": PublicOutcome.FAILED,
}

_missing_codes = ORCHESTRATOR_ERROR_CODES - set(PUBLIC_OUTCOME_BY_ERROR_CODE)
if _missing_codes:  # pragma: no cover - import-time totality guard
    raise RuntimeError(
        f"orchestrator error codes without a public-outcome bucket: {sorted(_missing_codes)}"
    )
_extra_codes = set(PUBLIC_OUTCOME_BY_ERROR_CODE) - ORCHESTRATOR_ERROR_CODES
if _extra_codes:  # pragma: no cover - import-time totality guard
    raise RuntimeError(
        f"public-outcome bucket table names codes orchestrator.py never emits: "
        f"{sorted(_extra_codes)}"
    )

#: Total over ``ToolID``: the closed-vocabulary ``SourceClass`` each tool's
#: coverage entries fold to. Deliberately lossy for the cross-cutting tools
#: (``query_metric.v1``/``list_metrics.v1``/``search_evidence.v1``/
#: ``get_evidence.v1``/``resolve_scope.v1`` do not name one source domain);
#: ``SOURCE_HEALTH`` is the honest "something about source
#: availability/quality" bucket for those, not a guess at which domain.
SOURCE_CLASS_BY_TOOL_ID: Mapping[_ToolID, SourceClass] = {
    _ToolID.RESOLVE_SCOPE: SourceClass.SOURCE_HEALTH,
    _ToolID.LIST_METRICS: SourceClass.SOURCE_HEALTH,
    _ToolID.QUERY_METRIC: SourceClass.STATUS_CHANGE,
    _ToolID.STATUS_SNAPSHOT: SourceClass.STATUS_CHANGE,
    _ToolID.CHANGE_SUMMARY: SourceClass.WORK_ITEM,
    _ToolID.WORK_GRAPH_NEIGHBORS: SourceClass.WORK_GRAPH,
    _ToolID.SEARCH_EVIDENCE: SourceClass.SOURCE_HEALTH,
    _ToolID.GET_EVIDENCE: SourceClass.SOURCE_HEALTH,
    _ToolID.DATA_HEALTH: SourceClass.SOURCE_HEALTH,
}

_missing_tools = frozenset(_ToolID) - set(SOURCE_CLASS_BY_TOOL_ID)
if _missing_tools:  # pragma: no cover - import-time totality guard
    raise RuntimeError(
        f"ToolID members without a SourceClass bucket: {sorted(_missing_tools)}"
    )

#: ``_budget_answer`` (``orchestrator.py``) reports a degraded partial answer's
#: unavailable source as the literal string ``"tool_results"`` -- not a
#: ``ToolID`` value at all. Folds to the same honest bucket as the
#: cross-cutting tools rather than raising on an unmapped token.
_LEGACY_COVERAGE_FALLBACK = SourceClass.SOURCE_HEALTH

#: A registered *compatibility* plan id (CHAOS-3297 Codex review MEDIUM #3),
#: not a governed ``DevInvestigationPlan``: no real investigation plan runs
#: the legacy model-tool-choice loop, and ``DevFrameVersions.plan_id`` is
#: enforced against ``contracts_v2.plan.PLAN_REGISTRY`` membership on *every*
#: content-bearing frame (``validators.validate_plan_registry_membership``),
#: not only the no-answer path's own totality checks. Registering this id is
#: the truthful choice: it says "the legacy loop produced this, not a
#: plan-executor investigation" rather than borrowing a real plan's identity
#: to pass an unrelated check. See ``contracts_v2.plan.PLAN_REGISTRY``'s own
#: comment on this entry, and
#: ``test_terminal_frames.py::test_legacy_plan_id_is_a_registered_compatibility_entry``.
LEGACY_ANSWER_PLAN_ID: PlatformVersionToken = "legacy.tool_choice.v1"
_LEGACY_PLAN_VERSION: PlatformVersionToken = "legacy_tool_choice.v1"
_LEGACY_INTERPRETER_VERSION: PlatformVersionToken = "legacy_tool_choice_interpreter.v1"

_CLAIM_KIND_TO_FACT_KIND: Mapping[ClaimKind, str] = {
    ClaimKind.OBSERVED: "observed",
    ClaimKind.INFERRED: "inferred",
    ClaimKind.RECOMMENDATION: "recommendation",
}


_SERVER_HANDLE_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def _canonical_run_id(run_id: str) -> str:
    """Canonicalize a run id, folding a non-UUID value deterministically.

    Mirrors ``preflight_outcomes._server_handle`` exactly (same fold-not-
    reject reasoning: a correlation handle is never a disclosure channel,
    hex digits and hyphens cannot spell a name). Production always supplies
    a real UUID (``router.py`` mints ``run_id`` from ``accepted.run.id``,
    a ``uuid.uuid4()``-defaulted column), but ``orchestrator.py``'s own unit
    tests widely use non-UUID ids like ``"run_01"`` -- rejecting those would
    make every one of that suite's runs silently produce no frame (finish()
    catches the construction failure and degrades gracefully), leaving this
    module's own behavior untested by the orchestrator's existing tier.
    """

    lowered = run_id.lower()
    if _SERVER_HANDLE_RE.match(lowered):
        return lowered
    return str(uuid.uuid5(_NAMESPACE, run_id))


def _disclosures_from_claim_flags(flags: DevClaimFlags) -> tuple[FactDisclosure, ...]:
    """Fold a v1 ``DevClaimFlags`` into the canonically-ordered disclosure tuple.

    Iterates ``FactDisclosure`` in its own enum declaration order (not the
    order the flags happen to be set), so the result already satisfies
    ``DevAnswerFact.validate_disclosures_canonical_order`` without a separate
    sort step -- the canonical form falls out of the iteration order.
    """

    return tuple(
        disclosure for disclosure in FactDisclosure if getattr(flags, disclosure.value)
    )


def _wrap_legacy_metric(metric: DevMetricRef) -> DevMetricRefV2:
    """One v1 ``DevMetricRef`` (from ``answer.metrics``) mirrored into
    ``DevMetricRefV2``, with F10's evidence_classification set
    UNCONDITIONALLY (team-lead ruling, 2026-08-02) -- never inferred from
    whether ``metric.evidence_ref_ids`` happens to be empty. Every v1-sourced
    metric originates from the legacy model-tool-choice loop, ultimately from
    ``production_runtime.py``'s ``query_metric.v1`` tool, which deliberately
    scrubs ``evidence_ref_ids`` to ``()`` on every call -- so this is always
    the correct classification for this path, by construction, not a guess.
    If that invariant is ever violated (a v1 metric genuinely carrying real
    evidence_ref_ids), ``DevMetricRefV2``'s own XOR validator rejects the
    contradiction loudly rather than silently picking one side.
    """

    return DevMetricRefV2.model_validate(
        {
            **metric.model_dump(),
            "evidence_classification": MetricEvidenceClassification.LEGACY_V1_UNMINTED,
        }
    )


def tolerant_parse_legacy_frame_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Patch a raw ``dev_answer_frame.v1`` payload read back from storage so
    it validates against today's ``DevAnswerFrame``, when the row predates a
    field this branch (CHAOS-3297 stack #3) added additively without a
    ``schema_version`` bump (version-skew read posture, team-lead ruling
    2026-08-02).

    Scope, deliberately narrow: this is a READ-time shim for a payload
    freshly loaded from storage, never a substitute for constructing a
    ``DevMetricRefV2`` correctly in application code. It must never run
    before a payload is WRITTEN (``persistence/service.py``'s
    ``record_frame`` stays strict -- a caller passing a metric with neither
    ``evidence_ref_ids`` nor ``evidence_classification`` is a caller bug,
    not data to quietly patch), and it is deliberately NOT implemented as a
    ``DevMetricRefV2`` validator: putting this logic in the type itself
    would silently default every future construction call that is missing
    the classification by a genuine bug to ``LEGACY_V1_UNMINTED`` instead of
    raising -- defeating F10's floor for every caller, not just replay.

    Today's one skew case: every row ``wrap_legacy_answer_as_frame``
    persisted before this branch's ``DevMetricRefV2.evidence_classification``
    field existed has metrics with ``evidence_ref_ids: []`` and no
    ``evidence_classification`` key at all (``production_runtime.py``'s
    ``query_metric.v1`` tool scrubs evidence on every call -- see
    ``_wrap_legacy_metric``'s docstring -- so this was already the
    universal legacy-answer metric shape, not a rare one). A metric dict is
    treated as pre-s3-shaped, and patched, only when BOTH hold: the
    ``evidence_classification`` key is entirely absent (never when it is
    present and ``null`` -- that is a different, already-invalid shape
    ``DevMetricRefV2``'s own XOR validator must keep rejecting) AND
    ``evidence_ref_ids`` is empty. A metric that already carries real
    evidence is passed through untouched.

    The health/deficiency findings fields added in the same branch need no
    equivalent shim: both are ``default_factory``-backed, so a payload that
    predates them simply omits the keys and pydantic supplies the (correct,
    empty) default -- see ``test_chaos_3297_s3_version_skew.py``.
    """

    patched: dict[str, Any] = dict(payload)
    metrics = patched.get("metrics")
    if isinstance(metrics, list) and metrics:
        patched_metrics: list[Any] = []
        for metric in metrics:
            if (
                isinstance(metric, Mapping)
                and "evidence_classification" not in metric
                and not metric.get("evidence_ref_ids")
            ):
                patched_metric = dict(metric)
                patched_metric["evidence_classification"] = (
                    MetricEvidenceClassification.LEGACY_V1_UNMINTED.value
                )
                patched_metrics.append(patched_metric)
            else:
                patched_metrics.append(metric)
        patched["metrics"] = patched_metrics
    return patched


def _source_class_for_legacy_token(token: str) -> SourceClass:
    try:
        tool_id = _ToolID(token)
    except ValueError:
        return _LEGACY_COVERAGE_FALLBACK
    return SOURCE_CLASS_BY_TOOL_ID[tool_id]


def _platform_token(
    value: str, *, fallback: PlatformVersionToken
) -> PlatformVersionToken:
    """Coerce a free-form v1 ``Version`` string into a ``PlatformVersionToken``.

    Mirrors ``preflight_outcomes._platform_token`` (kept as an independent
    copy rather than a cross-module import of a private helper): v1's
    ``DevContractVersions`` fields use hyphenated forms the stricter v2
    grammar does not admit.
    """

    folded = value.strip().lower().replace("-", "_")
    if len(folded) >= 3 and _looks_like_platform_token(folded):
        return folded
    return fallback


_PLATFORM_TOKEN_RE = re.compile(
    r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v\d+(?:\.\d+)*$"
)


def _looks_like_platform_token(value: str) -> bool:
    return bool(_PLATFORM_TOKEN_RE.match(value))


def build_error_frame(
    *,
    code: str,
    run_id: str,
    generated_at: datetime,
    versions: DevContractVersions | None = None,
) -> DevAnswerFrame:
    """The minimal frame one orchestrator-level ``error(code, ...)`` termination gets.

    ``code`` must be one of ``ORCHESTRATOR_ERROR_CODES`` -- every call site
    in ``orchestrator.run()``'s own ``error()`` closure, never a
    preflight-sourced code (those are recorded before ``finish()`` runs; see
    the module docstring). The frame's ``direct_answer``/``coverage`` are
    server-owned canonical copy, exactly like
    ``preflight_outcomes.build_preflight_answer`` -- this is intentionally
    NOT where the user-visible v1 message comes from (that stays the call
    site's own ``error(code, message, ...)`` object, passed to
    ``terminal()`` unchanged).
    """

    if code not in PUBLIC_OUTCOME_BY_ERROR_CODE:
        raise UnregisteredTerminalCode(
            f"unregistered orchestrator error code: {code!r}"
        )
    outcome = PUBLIC_OUTCOME_BY_ERROR_CODE[code]
    is_no_answer = outcome.value in NO_ANSWER_OUTCOMES
    frame_versions = None
    if not is_no_answer:
        source = versions or DevContractVersions(
            prompt_version=_LEGACY_PLAN_VERSION,
            tool_contract_version="ask_dev_tools.v1",
            metric_definition_version="ask_dev_metrics.v1",
            query_version="ask_dev_queries.v1",
        )
        frame_versions = DevFrameVersions(
            interpreter_version=_LEGACY_INTERPRETER_VERSION,
            plan_id=LEGACY_ANSWER_PLAN_ID,
            plan_version=_LEGACY_PLAN_VERSION,
            tool_contract_version=_platform_token(
                source.tool_contract_version, fallback="ask_dev_tools.v1"
            ),
            metric_definition_version=_platform_token(
                source.metric_definition_version, fallback="ask_dev_metrics.v1"
            ),
            query_version=_platform_token(
                source.query_version, fallback="ask_dev_queries.v1"
            ),
        )
    return DevAnswerFrame(
        schema_version="dev_answer_frame.v1",
        frame_id=str(uuid.uuid5(_NAMESPACE, f"error_frame:{run_id}:{code}")),
        run_id=_canonical_run_id(run_id),
        generated_at=generated_at,
        public_outcome=outcome,
        direct_answer=(
            CANONICAL_NO_ANSWER_COPY[outcome.value]
            if is_no_answer
            else _SCOPE_AMBIGUOUS_COPY
        ),
        coverage=DevCoverageV2(
            required_source_count=0,
            available_source_count=0,
            unavailable_required_sources=(),
            stale_required_sources=(),
            as_of=generated_at,
        ),
        versions=frame_versions,
    )


def _deficiency_category_statuses_from_contents(
    contents: list[DevSourceContent],
) -> tuple[DeficiencyCategoryStatus, ...]:
    """The first non-empty ``content.deficiency_category_statuses`` found
    across ``contents``, or ``()`` if none carry one.

    Today's ``deficiency.operational.v1`` plan (``wave_3_1_plans.py``) is
    SINGULAR-cardinality with exactly one mandatory step, so at most one
    observation in any real ``DevInvestigationResult`` ever populates this
    field -- unlike ``health_findings``/``deficiency_findings``, which can
    legitimately accumulate across several observations, a coverage block
    is one inventory's own fixed eight-category snapshot, and multiple
    genuinely-full ones cannot be meaningfully merged (a category evaluated
    in one but not another has no single correct combined status). If that
    single-observation assumption is ever violated, taking the first
    non-empty one is a conservative, non-fabricating choice -- never a
    fabricated merge -- documented here so a future second producer is a
    deliberate design decision, not a silent overwrite.
    """

    for content in contents:
        if content.deficiency_category_statuses:
            return content.deficiency_category_statuses
    return ()


def _findings_from_investigation_result(
    investigation_result: DevInvestigationResult | None,
) -> tuple[
    tuple[HealthRuleFinding, ...],
    bool,
    tuple[DeficiencyFinding, ...],
    bool,
    tuple[DeficiencyCategoryStatus, ...],
]:
    """Flatten every observation's ``content.health_findings``/
    ``deficiency_findings`` across ``investigation_result``, then re-sort
    and cap at the frame's own bound via the SAME
    ``capped_health_findings``/``capped_deficiency_findings`` functions
    ``investigation_plans.wave_3_1_plans``'s own step wiring uses -- one
    capping function, never a second copy that could disagree on which 50
    survive.

    A per-observation ``content.health_findings_truncated``/
    ``deficiency_findings_truncated`` is preserved by ORing it into the
    frame-level flag, never discarded: a source that already dropped
    findings before this function ever saw them must still disclose that,
    even if the flattened-and-recapped total this function computes
    happens to land back under 50 (Codex-anticipated finding: re-deriving
    truncation only from ``len(flattened) > 50`` would silently lose a
    truncation signal from any single observation whose OWN pre-cap set
    exceeded 50 but whose surviving 50 combine with few enough others to
    read as untruncated overall).

    ``deficiency_category_statuses`` (CHAOS-3297 s3 codex full-branch
    review round 1, FINDING 2, 2026-08-02) rides alongside via
    ``_deficiency_category_statuses_from_contents`` -- see that function's
    own docstring for why "first non-empty" is the right posture rather
    than a flatten-and-cap like the findings above.
    """

    if investigation_result is None:
        return (), False, (), False, ()
    contents = [
        obs.content
        for obs in investigation_result.observations
        if obs.content is not None
    ]
    all_health = tuple(
        finding for content in contents for finding in content.health_findings
    )
    all_deficiency = tuple(
        finding for content in contents for finding in content.deficiency_findings
    )
    health, health_truncated = capped_health_findings(all_health)
    deficiency, deficiency_truncated = capped_deficiency_findings(all_deficiency)
    health_truncated = health_truncated or any(
        content.health_findings_truncated for content in contents
    )
    deficiency_truncated = deficiency_truncated or any(
        content.deficiency_findings_truncated for content in contents
    )
    deficiency_category_statuses = _deficiency_category_statuses_from_contents(contents)
    return (
        health,
        health_truncated,
        deficiency,
        deficiency_truncated,
        deficiency_category_statuses,
    )


def wrap_legacy_answer_as_frame(
    answer: DevAnswer,
    *,
    run_id: str,
    investigation_result: DevInvestigationResult | None = None,
) -> DevAnswerFrame:
    """Mirror a fully-validated legacy v1 ``DevAnswer`` into a real frame.

    Always ``answered_with_gaps``, never plain ``answered``: the legacy
    model-tool-choice loop never computes a ``DevCompletionBlock``, and
    ``validate_outcome_consistency`` requires either disclosed limitations
    or a non-calculable completion block for ``answered_with_gaps`` --
    which every wrapped answer satisfies (see the always-present synthetic
    section below, which also stands in for a genuinely empty ``claims``
    list, e.g. ``orchestrator._budget_answer``'s metrics-only partial
    answer).

    Each claim's ``DevClaimFlags`` are folded into its fact's
    ``disclosures`` in canonical order (see ``_disclosures_from_claim_flags``
    and ``contracts_v2.base.FactDisclosure``), so a v1 claim carrying
    stale/uncertain/conflicting/untrusted_source is no longer silently
    dropped when embedded into a frame.

    CHAOS-3297 stack #3 (team-lead boundary ruling, 2026-08-02):
    ``investigation_result``, when the run's plan executor produced one
    ALONGSIDE the legacy model loop (both run unconditionally today --
    see ``orchestrator.run()``), is embedded into this SAME frame's
    ``health_findings``/``deficiency_findings`` -- never a second,
    separate frame. ``direct_answer``/``public_outcome``/``completion``
    stay driven purely by ``answer`` (the legacy loop is still what a
    caller sees); the plan's structured findings ride alongside as
    additional, independently-verifiable content. ``versions.plan_id``
    stays ``LEGACY_ANSWER_PLAN_ID`` regardless -- it discloses provenance
    of ``direct_answer``, which the plan never authored, not of every
    field on the frame. Collapsing this divergence (the frame becoming
    authoritative end to end) is explicitly stack #4/#5 territory.
    """

    (
        health_findings,
        health_findings_truncated,
        deficiency_findings,
        deficiency_findings_truncated,
        deficiency_category_statuses,
    ) = _findings_from_investigation_result(investigation_result)

    facts = tuple(
        DevAnswerFact(
            fact_id=claim.claim_id,
            text=claim.text,
            kind=_CLAIM_KIND_TO_FACT_KIND[claim.kind],
            evidence_ref_ids=tuple(claim.evidence_ref_ids),
            confidence=claim.confidence,
            disclosures=_disclosures_from_claim_flags(claim.flags),
        )
        for claim in answer.claims
    )
    # Always emit one section, even with zero fact_ids: `has_content` in
    # `validate_outcome_consistency` is `bool(sections) or bool(facts)`, and
    # a metrics-only legacy answer (empty `claims`) would otherwise fail
    # that check despite carrying real content.
    section = DevAnswerSection(
        section_id=str(
            uuid.uuid5(_NAMESPACE, f"legacy_answer_section:{run_id}:{answer.answer_id}")
        ),
        title="Answer",
        fact_ids=tuple(fact.fact_id for fact in facts),
    )
    return DevAnswerFrame(
        schema_version="dev_answer_frame.v1",
        frame_id=str(
            uuid.uuid5(_NAMESPACE, f"legacy_answer_frame:{run_id}:{answer.answer_id}")
        ),
        run_id=_canonical_run_id(run_id),
        generated_at=answer.generated_at,
        public_outcome=PublicOutcome.ANSWERED_WITH_GAPS,
        direct_answer=answer.direct_summary,
        # The legacy model-tool-choice loop never computes a completion
        # block at all -- `calculable=False` states that honestly (P8:
        # never an inferred 0%/100%) and is what makes `answered_with_gaps`
        # valid per `validate_outcome_consistency` even when `answer.warnings`
        # is empty (e.g. a fully "complete" legacy status answer that simply
        # never assessed completion the v2 way).
        completion=DevCompletionBlock(calculable=False),
        sections=(section,),
        facts=facts,
        health_findings=health_findings,
        health_findings_truncated=health_findings_truncated,
        deficiency_findings=deficiency_findings,
        deficiency_findings_truncated=deficiency_findings_truncated,
        deficiency_category_statuses=deficiency_category_statuses,
        metrics=tuple(_wrap_legacy_metric(metric) for metric in answer.metrics),
        evidence=tuple(
            DevEvidenceRefV2.model_validate(item.model_dump())
            for item in answer.evidence
        ),
        conflicts=tuple(
            DevFrameConflict(
                summary=conflict.summary,
                evidence_ref_ids=tuple(conflict.evidence_ref_ids),
            )
            for conflict in answer.conflicts
        ),
        limitations=tuple(answer.warnings)[:20],
        coverage=DevCoverageV2(
            required_source_count=answer.coverage.required_source_count,
            available_source_count=answer.coverage.available_source_count,
            unavailable_required_sources=tuple(
                _source_class_for_legacy_token(token)
                for token in answer.coverage.unavailable_required_sources
            )[:25],
            stale_required_sources=tuple(
                _source_class_for_legacy_token(token)
                for token in answer.coverage.stale_required_sources
            )[:25],
            as_of=answer.coverage.as_of,
        ),
        safe_follow_up_questions=tuple(answer.suggested_follow_up_questions)[:10],
        versions=DevFrameVersions(
            interpreter_version=_LEGACY_INTERPRETER_VERSION,
            plan_id=LEGACY_ANSWER_PLAN_ID,
            plan_version=_LEGACY_PLAN_VERSION,
            tool_contract_version=_platform_token(
                answer.versions.tool_contract_version, fallback="ask_dev_tools.v1"
            ),
            metric_definition_version=_platform_token(
                answer.versions.metric_definition_version, fallback="ask_dev_metrics.v1"
            ),
            query_version=_platform_token(
                answer.versions.query_version, fallback="ask_dev_queries.v1"
            ),
            prompt_version=_platform_token(
                answer.versions.prompt_version, fallback=_LEGACY_PLAN_VERSION
            ),
        ),
    )
