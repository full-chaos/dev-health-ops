"""Outcome mapping and canonical copy for the CHAOS-3292 subject preflight.

This module owns the translation from a per-mention ``ResolutionOutcome`` to
the public vocabularies, and builds the ``dev_answer_frame.v1`` /
``dev_answer.v2`` pair a preflight termination emits.

Two structural properties matter more than the mapping itself:

* **``FORBIDDEN_OR_NOT_FOUND`` is unreachable from here.** Not scrubbed, not
  renamed, not filtered — the preflight simply has no code path that
  constructs it. The token is baked into five published v1 JSON Schemas, so
  renaming it would be a wire-contract change; unreachability is the closure
  that does not touch the wire.
* **Public copy comes only from closed tables.** ``CANONICAL_NO_ANSWER_COPY``
  (landed with CHAOS-3294) for the four no-answer outcomes, and
  ``CLARIFICATION_COPY`` here for ``needs_clarification`` — which is *not* a
  no-answer outcome, so the contract's own allowlist projection does not
  govern its ``direct_answer`` and this module must supply server-owned copy
  itself.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

from .contracts import (
    DevContractVersions,
    DevError,
    DevTimeRange,
    dev_error_remediation,
)
from .contracts_v2 import (
    DevAnswerFrame,
    DevAnswerV2,
    DevCoverageV2,
    DevFrameVersions,
    DevResolutionCandidate,
    PublicOutcome,
    QuestionIntentID,
    ResolutionOutcome,
    project_answer_v2_to_v1,
)
from .contracts_v2.plan import PLAN_REGISTRY
from .contracts_v2.validators import CANONICAL_NO_ANSWER_COPY
from .orchestrator_states import RunState
from .question_interpreter import INTERPRETER_VERSION

__all__ = [
    "CLARIFICATION_COPY",
    "LEGACY_ONLY_QUESTION_INTENTS",
    "NOT_FOUND_CLOSE_MATCHES_KEY",
    "PLAN_ID_BY_INTENT",
    "PREFLIGHT_OUTCOME_BY_RESOLUTION",
    "TERMINAL_STATE_BY_OUTCOME",
    "build_preflight_answer",
    "project_preflight_error",
]

#: The plan each launch intent would have executed. The registry is 1:1 with
#: ``QuestionIntentID`` today; the mapping is written out rather than derived
#: so a future second plan for one intent is a visible edit here.
PLAN_ID_BY_INTENT: Mapping[QuestionIntentID, str] = {
    QuestionIntentID.ENTITY_STATUS: "status.entity.v2",
    QuestionIntentID.PORTFOLIO_STATUS: "status.portfolio.v1",
    QuestionIntentID.REMAINING_WORK: "work.remaining.v1",
    QuestionIntentID.OBSERVED_CHANGE: "change.observed.v1",
    QuestionIntentID.REGISTERED_STATISTICS: "statistics.registered.v1",
    QuestionIntentID.METRIC_COMPARISON: "metric.comparison.v1",
    QuestionIntentID.DATA_TRUST: "trust.data.v1",
    QuestionIntentID.PROJECT_HEALTH: "health.project.v1",
    QuestionIntentID.TEAM_HEALTH: "health.team.v1",
    QuestionIntentID.TEAM_WORKLOAD_BALANCE: "balance.team_workload.v1",
    QuestionIntentID.OPERATIONAL_DEFICIENCY_INVENTORY: "deficiency.operational.v1",
    QuestionIntentID.BOUNDED_INVESTIGATION: "investigation.bounded.v1",
}

# Import-time totality, in both directions: an intent added without a plan, or
# a plan id that is not in the landed registry, breaks the import rather than
# failing at runtime on the one request that happens to hit it.
_missing_intents = sorted(
    intent.value for intent in QuestionIntentID if intent not in PLAN_ID_BY_INTENT
)
if _missing_intents:
    raise RuntimeError(f"question intents without a plan id: {_missing_intents}")
_unregistered = sorted(set(PLAN_ID_BY_INTENT.values()) - PLAN_REGISTRY)
if _unregistered:
    raise RuntimeError(f"plan ids outside the landed plan registry: {_unregistered}")

#: Intents that are correctly, permanently NEVER plan-governed -- falling
#: through to the legacy model-tool-choice loop is their intended, designed
#: behavior, not a capability gap (CHAOS-3300 finding, 2026-08-02).
#: ``BOUNDED_INVESTIGATION`` is minted by ``question_interpreter.py`` itself
#: as the "a miss degrades to this" catch-all when no deterministic intent
#: recognizer fires; ``PLAN_ID_BY_INTENT``'s own entry for it
#: (``"investigation.bounded.v1"``) is a *compatibility* plan-id token --
#: registered in ``PLAN_REGISTRY``'s vocabulary so it can be disclosed on
#: the wire, exactly like ``terminal_frames.LEGACY_ANSWER_PLAN_ID`` -- never
#: a real, governed ``DevInvestigationPlan`` document, and none should ever
#: be authored for it.
#:
#: Every OTHER member of ``PLAN_ID_BY_INTENT`` names an intent that SHOULD
#: eventually resolve through a real plan. If ``orchestrator.py``'s own
#: ``self._plan_registry.get(intent.intent_id)`` returns ``None`` for one of
#: those at request time, that is a genuine, still-open capability gap (an
#: intent CHAOS-3295/3297/3303/3304/3305 name a plan for, but this runtime's
#: registry does not yet carry) -- ``orchestrator.run()`` emits
#: ``ASK_DEV_PLAN_REGISTRY_GAP_TOTAL``/a structured warning for exactly this
#: case, distinguishing "designed to be legacy" (this set, silent) from
#: "should be plan-governed but isn't yet" (everything else, loud). Removing
#: an intent from this set (because a real plan was finally authored for it)
#: is a deliberate, reviewed edit here, never implicit.
LEGACY_ONLY_QUESTION_INTENTS: frozenset[QuestionIntentID] = frozenset(
    {QuestionIntentID.BOUNDED_INVESTIGATION}
)

#: Per-mention resolution outcome to the public v2 outcome. ``EXACT_MATCH`` is
#: deliberately absent: it is not a termination, it is the path that proceeds.
PREFLIGHT_OUTCOME_BY_RESOLUTION: Mapping[ResolutionOutcome, PublicOutcome] = {
    ResolutionOutcome.NO_AUTHORIZED_MATCH: PublicOutcome.NOT_FOUND,
    ResolutionOutcome.AMBIGUOUS_CANDIDATES: PublicOutcome.NEEDS_CLARIFICATION,
    ResolutionOutcome.CATALOG_UNAVAILABLE: PublicOutcome.TEMPORARILY_UNAVAILABLE,
    ResolutionOutcome.UNSUPPORTED_KIND: PublicOutcome.UNSUPPORTED,
}

#: The v1 terminal run state each public outcome maps to on today's surface.
TERMINAL_STATE_BY_OUTCOME: Mapping[PublicOutcome, RunState] = {
    PublicOutcome.NOT_FOUND: RunState.INSUFFICIENT_EVIDENCE,
    PublicOutcome.NEEDS_CLARIFICATION: RunState.INSUFFICIENT_EVIDENCE,
    PublicOutcome.TEMPORARILY_UNAVAILABLE: RunState.FAILED,
    PublicOutcome.UNSUPPORTED: RunState.FAILED,
}

#: ``needs_clarification`` is not one of ``NO_ANSWER_OUTCOMES``, so the
#: contract's total field allowlist does not constrain its ``direct_answer``.
#: Server-owned copy is supplied here instead, and — like the canonical
#: no-answer table — it names nothing about the subject.
#: CHAOS-3366's key into ``CLARIFICATION_COPY``. Named rather than spelled as
#: a literal at both ends so the preflight and this table cannot drift apart.
NOT_FOUND_CLOSE_MATCHES_KEY = "not_found_close_matches"

CLARIFICATION_COPY: Mapping[str, str] = {
    "ambiguous": (
        "More than one authorized entity matches the name in this question. "
        "Please ask again naming exactly which one you mean."
    ),
    "uninterpretable": (
        "This question could not be interpreted confidently. Please rephrase "
        "it, naming the project, repository, or team you are asking about."
    ),
    # CHAOS-3366, provisional. The PRD sentence names the kind and the name
    # back ("I couldn't find an authorized <kind> named 'X'"); interpolating
    # request text into this table is a change to how public copy is governed,
    # which is CHAOS-3367's contract to make, not this one's. Static
    # server-owned text until then -- it says the same thing and, like every
    # other value here, names nothing the catalog did not confirm.
    NOT_FOUND_CLOSE_MATCHES_KEY: (
        "I could not find an authorized entity of that kind with that name. "
        "Here are the closest matches -- please ask again naming one of them."
    ),
}

#: The v1 ``DevError`` code each preflight outcome terminates with today. Taken
#: from ``compat._ERROR_OUTCOME_CODES`` for the four no-answer outcomes (via
#: the projector, so there is one mapping) and from the orchestrator's own
#: pre-existing ambiguity terminal for ``needs_clarification``.
_AMBIGUOUS_V1_ERROR = ("scope_ambiguous", "The requested scope is ambiguous.")

_PLATFORM_TOKEN = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v\d+(?:\.\d+)*$")
_SERVER_HANDLE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
#: Stable namespace for folding a non-UUID correlation id into a
#: ``ServerHandle``. Mirrors ``router._storage_uuid``, which already folds
#: arbitrary client-supplied ids to a UUID5 at the storage boundary.
_HANDLE_NAMESPACE = uuid.UUID("6f9f5b12-6b2f-5a2e-9f2a-2f0a8f4c1d33")


def _server_handle(value: str) -> str:
    """A canonical dashed lowercase UUID for one correlation id.

    ``ServerHandle`` admits only that grammar (CHAOS-3294 constraint #1), and
    production already mints UUID run/answer/conversation ids. A caller that
    supplies something else — a test harness, a legacy row — is folded
    deterministically rather than rejected, because a correlation handle is
    never a disclosure channel: hex digits and hyphens cannot spell a name.
    """

    lowered = value.lower()
    if _SERVER_HANDLE.match(lowered):
        return lowered
    return str(uuid.uuid5(_HANDLE_NAMESPACE, value))


def _platform_token(value: str, *, fallback: str) -> str:
    """Coerce one declared version string into a ``PlatformVersionToken``.

    ``DevContractVersions`` fields are free-form ``Version`` strings and
    production uses hyphenated forms (``ask-dev-metrics.v1``) that the stricter
    v2 provenance grammar does not admit. Hyphens fold to underscores; anything
    still outside the grammar falls back to a server-owned constant rather than
    failing the whole termination on a provenance field.
    """

    folded = value.strip().lower().replace("-", "_")
    return folded if _PLATFORM_TOKEN.match(folded) else fallback


def _frame_versions(
    *, intent_id: QuestionIntentID, versions: DevContractVersions
) -> DevFrameVersions:
    return DevFrameVersions(
        interpreter_version=INTERPRETER_VERSION,
        plan_id=PLAN_ID_BY_INTENT[intent_id],
        plan_version="ask_dev_plan.v1",
        tool_contract_version=_platform_token(
            versions.tool_contract_version, fallback="ask_dev_tools.v1"
        ),
        metric_definition_version=_platform_token(
            versions.metric_definition_version, fallback="ask_dev_metrics.v1"
        ),
        query_version=_platform_token(
            versions.query_version, fallback="ask_dev_queries.v1"
        ),
        prompt_version=_platform_token(
            versions.prompt_version, fallback="ask_dev_prompt.v1"
        ),
    )


#: Safe display label per public outcome. **Total over ``PublicOutcome``**, not
#: over the outcomes this module happens to emit today: a partial vocabulary
#: handler is a ``KeyError`` waiting for the first caller that reaches the
#: missing member, and ``DENIED`` — unreachable from the preflight right now —
#: was exactly that latent crash.
#:
#: Every value must equal the canonical label ``dev_answer.v2`` validates
#: against (``answer._OUTCOME_DISPLAY_LABELS``); the totality test asserts both
#: halves, so a member added without a label, or with a *wrong* label, fails at
#: build time rather than on the one request that produces it.
_DISPLAY_LABELS: Mapping[PublicOutcome, str] = {
    PublicOutcome.ANSWERED: "Answered",
    PublicOutcome.ANSWERED_WITH_GAPS: "Answered with some gaps",
    PublicOutcome.NEEDS_CLARIFICATION: "Needs clarification",
    PublicOutcome.NOT_FOUND: "Not found",
    PublicOutcome.TEMPORARILY_UNAVAILABLE: "Temporarily unavailable",
    PublicOutcome.UNSUPPORTED: "Not supported yet",
    PublicOutcome.DENIED: "Not permitted",
    PublicOutcome.FAILED: "Something went wrong",
}


def build_preflight_answer(
    *,
    outcome: PublicOutcome,
    intent_id: QuestionIntentID,
    versions: DevContractVersions,
    run_id: str,
    answer_id: str,
    conversation_id: str,
    generated_at: datetime,
    clarification_key: str = "ambiguous",
    clarification_candidates: tuple[DevResolutionCandidate, ...] = (),
) -> DevAnswerV2:
    """Build the v2 answer one preflight termination emits.

    Never a content-bearing frame — that is CHAOS-3297's. For the four
    no-answer outcomes the frame carries no provenance block at all and its
    ``direct_answer`` is the canonical server sentence; the contract's own
    ``validate_no_answer_projection`` re-derives that from the same table, so
    an error here is a validation failure rather than a silent disclosure.

    ``clarification_candidates`` (CHAOS-3325) is the resolution ledger's own
    ``DevResolutionCandidate`` tuple for one ``ambiguous_candidates`` mention
    — passed straight through from the ledger entry that terminated the run,
    never re-derived or invented here. It is meaningless for every outcome
    but ``needs_clarification`` (``validate_outcome_consistency`` /
    ``NO_ANSWER_FRAME_FIELD_POLICY`` both forbid it elsewhere), so callers on
    the four no-answer paths simply never pass it and the default empty tuple
    is always the correct value there.
    """

    is_no_answer = outcome is not PublicOutcome.NEEDS_CLARIFICATION
    frame = DevAnswerFrame(
        schema_version="dev_answer_frame.v1",
        frame_id=_server_handle(f"frame:{run_id}"),
        run_id=_server_handle(run_id),
        generated_at=generated_at,
        public_outcome=outcome,
        direct_answer=(
            CANONICAL_NO_ANSWER_COPY[outcome.value]
            if is_no_answer
            else CLARIFICATION_COPY[clarification_key]
        ),
        clarification_candidates=clarification_candidates,
        coverage=DevCoverageV2(
            required_source_count=0,
            available_source_count=0,
            unavailable_required_sources=(),
            stale_required_sources=(),
            as_of=generated_at,
        ),
        # A no-answer outcome carries no provenance block at all; a
        # needs_clarification frame is not a no-answer outcome and
        # validate_versions_presence requires one.
        versions=(
            None
            if is_no_answer
            else _frame_versions(intent_id=intent_id, versions=versions)
        ),
    )
    return DevAnswerV2(
        schema_version="dev_answer.v2",
        answer_id=_server_handle(answer_id),
        conversation_id=_server_handle(conversation_id),
        run_id=_server_handle(run_id),
        generated_at=generated_at,
        public_outcome=outcome,
        outcome_display_label=_DISPLAY_LABELS[outcome],
        frame=frame,
        narrative=None,
    )


#: CHAOS-3388. How many named candidates the v1 clarification message will
#: enumerate, and how long each one may be. Small on purpose: this is a
#: sentence a person reads, not a data dump, and the frame's own
#: ``clarification_candidates`` (bounded separately, at up to
#: ``MAX_CANDIDATES``/``NOT_FOUND_FALLBACK_LIMIT``) remains the authoritative
#: full list for any future structured surface.
_MAX_NAMED_CANDIDATES = 5
_MAX_CANDIDATE_LABEL_CHARS = 120


def _candidate_names(
    candidates: tuple[DevResolutionCandidate, ...],
) -> tuple[str, ...]:
    names: list[str] = []
    seen: set[str] = set()
    for candidate in candidates[:_MAX_NAMED_CANDIDATES]:
        label = candidate.entity_ref.display_label.strip()[:_MAX_CANDIDATE_LABEL_CHARS]
        if not label or label in seen:
            continue
        seen.add(label)
        names.append(label)
    return tuple(names)


def _name_candidates(
    base_message: str, candidates: tuple[DevResolutionCandidate, ...]
) -> str:
    """Append real candidate display names to a closed-vocab base sentence.

    CHAOS-3388. ``candidates`` are the resolution ledger's own authorized,
    catalog-confirmed entities (``DevResolutionCandidate.entity_ref.
    display_label``) — never model- or user-authored text — so naming them
    here is the same trust tier as ``base_message`` itself, not a new
    disclosure channel: the copy-contract rule (CHAOS-3367) is that
    user-visible copy comes from closed tables plus, at most, data the
    server itself already confirmed, and a catalog display label is exactly
    that. An empty ``candidates`` tuple (the four no-answer outcomes, or a
    genuine ambiguity/close-matches search that found nothing to enrich)
    returns ``base_message`` unchanged.
    """

    names = _candidate_names(candidates)
    if not names:
        return base_message
    return f"{base_message} Candidates: {', '.join(names)}."


def project_preflight_error(answer: DevAnswerV2, *, request_id: str) -> DevError:
    """Project one preflight v2 answer to the v1 ``DevError`` the router streams.

    For the four no-answer outcomes this delegates to the landed
    ``compat.project_answer_v2_to_v1``, whose ``_project_error`` reads *nothing*
    off the frame and builds code, message and remediation entirely from the
    canonical tables. That is what makes "the internal combined outcome never
    leaves the server" structural on the v1 surface too.

    ``needs_clarification`` is handled separately and deliberately, on both
    the live path (``orchestrator.run``) and the replay path (``router``'s
    terminal-payload reconstruction): both call this function, not
    ``compat.project_answer_v2_to_v1``, for a preflight ambiguity. It always
    returns a v1 ``DevError`` — never a v1 ``DevAnswer`` — with the fixed
    ``scope_ambiguous`` *code*: that part is unchanged by CHAOS-3388, and
    still deliberate for the reason CHAOS-3325 gave it (a preflight
    ambiguity is the more faithful terminal-state statement — the run
    genuinely produced no answer — and ``scope_ambiguous`` is the shape the
    router and web client already handle for every terminal in this
    family).

    The *message* is no longer the one fixed sentence, though (CHAOS-3388).
    Before this, the real candidate list computed onto ``answer.frame.
    clarification_candidates`` (CHAOS-3325) never reached a v1 client at
    all here: an unresolved named subject with real, catalog-confirmed
    close matches available produced the exact same generic "the requested
    scope is ambiguous" text as a genuine same-name ambiguity with none —
    the acceptance-oracle-visible half of the CHAOS-3388 defect, since the
    v1 wire has no separate structured field for a candidate list and
    inventing one is out of scope. ``_name_candidates`` starts from
    ``answer.frame.direct_answer`` — already the differentiated,
    closed-vocab ``CLARIFICATION_COPY`` sentence for whichever
    ``clarification_key`` produced this termination, "ambiguous" or
    CHAOS-3366's "here are the closest matches" — and appends the real
    candidates' own catalog display labels, never invented, never
    model-authored: the same ``DevResolutionCandidate`` entries
    ``contracts_v2.compat``'s ``_project_needs_clarification`` already
    projects for whichever *other* caller runs a ``needs_clarification``
    answer through the general ``project_answer_v2_to_v1`` projector. This
    function's own preflight-ambiguity path still does not use that
    projector (the ``DevAnswer`` shape it builds remains out of scope here
    — see above), but it no longer discards the same candidates that
    projector already trusted.
    """

    if answer.public_outcome is PublicOutcome.NEEDS_CLARIFICATION:
        code, _ = _AMBIGUOUS_V1_ERROR
        message = _name_candidates(
            answer.frame.direct_answer, answer.frame.clarification_candidates
        )
        return DevError(
            schema_version="dev_error.v1",
            request_id=request_id,
            code=code,
            safe_message=message,
            retryable=False,
            remediation=dev_error_remediation(code),
        )
    projected = project_answer_v2_to_v1(
        answer,
        organization_id="preflight",
        time_range=_ZERO_RANGE,
    )
    if not isinstance(projected, DevError):  # pragma: no cover - defensive
        raise RuntimeError(
            "a preflight no-answer outcome must project to a v1 DevError"
        )
    # Re-emitted against the request's own correlation id: every other terminal
    # error in the run loop carries request.request_id, and the projector uses
    # run_id because a v2 answer has no request id. Code, copy, retryability
    # and remediation are carried across verbatim from the canonical tables.
    return DevError(
        schema_version="dev_error.v1",
        request_id=request_id,
        code=projected.code,
        safe_message=projected.safe_message,
        retryable=projected.retryable,
        remediation=list(projected.remediation),
    )


def _zero_range() -> DevTimeRange:
    start = datetime(2000, 1, 1, tzinfo=UTC)
    return DevTimeRange(start=start, end=start + timedelta(days=1), timezone="UTC")


#: Required by the projector's signature but unreachable on the error path —
#: ``_project_error`` builds no scope at all. A fixed, content-free range keeps
#: the call total rather than passing the run's real window into a code path
#: that must disclose nothing.
_ZERO_RANGE = _zero_range()
