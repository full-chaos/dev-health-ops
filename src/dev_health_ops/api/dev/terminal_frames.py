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

Known contract gap (filed on CHAOS-3297, referenced from the PR): ``DevAnswerFact``
has no flags-bearing field, so a v1 claim's ``DevClaimFlags``
(stale/uncertain/conflicting/untrusted_source) cannot round-trip through
``wrap_legacy_answer_as_frame``. That is acceptable for this transitional
compatibility shim (the frame is not the source of truth here; the v1
answer passed to ``terminal()`` unchanged is), but it is a blocking
precondition for CHAOS-3297 stack #3's real builders, which have no such
fallback -- see the docstring cross-reference on the Linear issue.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from datetime import datetime

from .contracts import (
    ClaimKind,
    DevAnswer,
    DevContractVersions,
)
from .contracts import ToolID as _ToolID
from .contracts_v2.base import PlatformVersionToken, PublicOutcome, SourceClass
from .contracts_v2.embedded import DevCoverageV2, DevEvidenceRefV2, DevMetricRefV2
from .contracts_v2.frame import (
    DevAnswerFact,
    DevAnswerFrame,
    DevAnswerSection,
    DevCompletionBlock,
    DevFrameConflict,
    DevFrameVersions,
)
from .contracts_v2.no_answer_policy import CANONICAL_NO_ANSWER_COPY, NO_ANSWER_OUTCOMES

__all__ = [
    "LEGACY_ANSWER_PLAN_ID",
    "ORCHESTRATOR_ERROR_CODES",
    "PUBLIC_OUTCOME_BY_ERROR_CODE",
    "SOURCE_CLASS_BY_TOOL_ID",
    "UnregisteredTerminalCode",
    "build_error_frame",
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


def wrap_legacy_answer_as_frame(answer: DevAnswer, *, run_id: str) -> DevAnswerFrame:
    """Mirror a fully-validated legacy v1 ``DevAnswer`` into a real frame.

    Always ``answered_with_gaps``, never plain ``answered``: the legacy
    model-tool-choice loop never computes a ``DevCompletionBlock``, and
    ``validate_outcome_consistency`` requires either disclosed limitations
    or a non-calculable completion block for ``answered_with_gaps`` --
    which every wrapped answer satisfies (see the always-present synthetic
    section below, which also stands in for a genuinely empty ``claims``
    list, e.g. ``orchestrator._budget_answer``'s metrics-only partial
    answer).

    Known gap: v1 ``DevClaim.flags`` (stale/uncertain/conflicting/
    untrusted_source) has no home on ``DevAnswerFact`` and is dropped here.
    Filed on CHAOS-3297 as a blocking precondition for stack #3's real
    builders (see module docstring).
    """

    facts = tuple(
        DevAnswerFact(
            fact_id=claim.claim_id,
            text=claim.text,
            kind=_CLAIM_KIND_TO_FACT_KIND[claim.kind],
            evidence_ref_ids=tuple(claim.evidence_ref_ids),
            confidence=claim.confidence,
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
        metrics=tuple(
            DevMetricRefV2.model_validate(metric.model_dump())
            for metric in answer.metrics
        ),
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
