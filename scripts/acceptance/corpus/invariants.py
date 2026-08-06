"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: the pluggable invariant-check registry.

This is the seam the CHAOS-3389 fold-in requires (invariant-first case
assertions; matcher-specific expectations come from a versioned
``resolution-profile``): a case's ``invariants[].check`` names a checker
here; the checker's ``args`` may pull an exact expected value out of the
case's resolved resolution-profile block (``from_profile``) instead of a
literal, so the SAME case file works unmodified across profiles -- when a
future QUA profile flips a case's expected resolution behavior, only that
profile's JSON changes, never the case file or this registry.

Six checkers are implemented today (``resolution_path_in``,
``no_internal_error``, ``public_outcome_in``, ``scope_resolution_outcome_in``,
``no_unauthorized_candidate_surfaces``, ``terminal_persists_assistant_row``)
-- the first three prove the runner's own wiring end-to-end; the latter
three graduated from :data:`NOT_YET_IMPLEMENTED_CATEGORIES` on Lane 2b's
explicit request once its authored cases started declaring them (2026-08-06:
57/57 of 2b's cases load against this module's schema today). The full "for
every case assert" matrix (issue Group 1: intent/cardinality, plan
id/version, per-source applicability, fact counts, metric ids/values/units,
health dimensions, evidence/citation precision, persistence/replay, etc.) is
still NOT implemented here -- each remaining one is a real checker this
registry can grow, one at a time, as Lane 2b's case authoring needs it.
Listed explicitly in :data:`NOT_YET_IMPLEMENTED_CATEGORIES` so this is a
documented gap, not a silent one.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "NOT_YET_IMPLEMENTED_CATEGORIES",
    "CHECKS",
    "InvariantCheckError",
    "InvariantContext",
    "InvariantResult",
    "evaluate_invariant",
    "register_check",
]

#: The issue's Group 1 "For every case assert" bullets this registry does
#: NOT yet have a checker for -- see the module docstring. Not consulted by
#: any code path; exists purely as an honest, grep-able residual-coverage
#: note.
NOT_YET_IMPLEMENTED_CATEGORIES: tuple[str, ...] = (
    "intent-cardinality-version",
    "plan-id-version-bounds",
    "source-applicability-observation-state",
    "fact-sample-counts-coverage-attribution",
    "required-prohibited-facts",
    "completion-numerator-denominator",
    "metric-ids-values-units-versions",
    "health-dimensions-rule-versions",
    "workload-denominator-baseline",
    "deficiency-category-severity-evidence",
    "evidence-citation-precision-freshness-conflicts",
    "ordered-sections-narrative-mode-fallback",
    "provider-source-time-token-cost-limits",
    "persistence-replay-retention-deletion",
)


class InvariantCheckError(Exception):
    """A checker is misconfigured (unknown name, missing/bad args) -- this
    is a case/profile-authoring defect, distinct from an assertion that ran
    and failed."""


@dataclass(frozen=True, slots=True)
class InvariantContext:
    """Everything one invariant check can inspect about a completed case run."""

    resolution_path: str | None
    public_outcome: str | None
    #: Raw, already-validated SSE event dicts, in stream order.
    events: Sequence[Mapping[str, Any]]
    #: The case's resolved matcher-specific expectations block
    #: (``case_schema.resolve_case_expectations``'s return value) -- empty
    #: for a case with no ``resolution_profile_ref``.
    expectations: Mapping[str, Any]
    #: ``dev_messages.answer_payload.schema_version`` for every assistant
    #: row this run's conversation has, in creation order -- via the
    #: docker-exec verification plane
    #: (``db_verify.query_transcript_assistant_schema_versions_via_exec``).
    #: Empty by default: only populated by a caller that actually ran that
    #: query; a case with no ``terminal_persists_assistant_row`` invariant
    #: never needs it, and the wire alone cannot answer this question (see
    #: ``resolution_path``'s identical "public API cannot see this" story).
    assistant_schema_versions: Sequence[str | None] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class InvariantResult:
    passed: bool
    detail: str


CheckFn = Callable[[Mapping[str, Any], InvariantContext], InvariantResult]

CHECKS: dict[str, CheckFn] = {}


def register_check(name: str) -> Callable[[CheckFn], CheckFn]:
    """Decorator registering a checker under ``name``. Raises if ``name`` is
    already registered -- a silent overwrite would let two checkers with the
    same name shadow each other with no signal about which one actually
    runs."""

    def decorator(fn: CheckFn) -> CheckFn:
        if name in CHECKS:
            raise InvariantCheckError(f"a check named {name!r} is already registered")
        CHECKS[name] = fn
        return fn

    return decorator


def _resolve_allowed(
    args: Mapping[str, Any], context: InvariantContext, *, check_name: str
) -> list[Any]:
    allowed = list(args.get("allowed", []))
    profile_key = args.get("from_profile")
    if profile_key is not None:
        if profile_key not in context.expectations:
            raise InvariantCheckError(
                f"{check_name}: profile key {profile_key!r} not found in "
                f"resolved expectations {dict(context.expectations)!r}"
            )
        allowed.append(context.expectations[profile_key])
    if not allowed:
        raise InvariantCheckError(
            f"{check_name} requires a non-empty 'allowed' list and/or a "
            "'from_profile' key resolvable in the case's resolution profile"
        )
    return allowed


@register_check("resolution_path_in")
def _resolution_path_in(
    args: Mapping[str, Any], context: InvariantContext
) -> InvariantResult:
    allowed = _resolve_allowed(args, context, check_name="resolution_path_in")
    if context.resolution_path is None:
        # Codex round-1 finding (HIGH, confirmed): `None` means "not
        # observed" (see resolution_path.py's own honest-absence contract)
        # -- it must NEVER satisfy this check, even if a case/profile
        # authoring mistake put a literal `null` into `allowed`. A case
        # that declares this check is asserting it OBSERVED a specific
        # resolution path; an unobserved run has not met that bar by
        # definition.
        return InvariantResult(
            passed=False,
            detail=(
                "resolution_path was not observed (None) -- a case that "
                "declares resolution_path_in requires an observed path, "
                f"never an absence; allowed={allowed!r}"
            ),
        )
    passed = context.resolution_path in allowed
    return InvariantResult(
        passed=passed,
        detail=f"resolution_path={context.resolution_path!r}, allowed={allowed!r}",
    )


@register_check("no_internal_error")
def _no_internal_error(
    _args: Mapping[str, Any], context: InvariantContext
) -> InvariantResult:
    offending = [
        event
        for event in context.events
        if event.get("event") == "error"
        and isinstance(event.get("data"), Mapping)
        and isinstance(event["data"].get("error"), Mapping)
        and event["data"]["error"].get("code") == "internal_error"
    ]
    return InvariantResult(
        passed=not offending,
        detail=(
            "no internal_error event"
            if not offending
            else f"{len(offending)} internal_error event(s) present"
        ),
    )


@register_check("public_outcome_in")
def _public_outcome_in(
    args: Mapping[str, Any], context: InvariantContext
) -> InvariantResult:
    """Assert the run's terminal ``PublicOutcome`` -- added per Codex
    round-1 (HIGH, confirmed): before this checker existed, no implemented
    invariant asserted the terminal outcome at all, so a case whose only
    check was ``no_internal_error`` would pass identically whether it
    actually answered or silently terminated in ``scope_ambiguous``, a
    missing answer frame, or any other non-``internal_error`` shape. Same
    ``allowed``/``from_profile`` seam as ``resolution_path_in``.
    """

    allowed = _resolve_allowed(args, context, check_name="public_outcome_in")
    if context.public_outcome is None:
        return InvariantResult(
            passed=False,
            detail=(
                "public_outcome was not observed (None) -- no "
                "answer.completed event was found, or the run did not "
                f"reach a terminal answer; allowed={allowed!r}"
            ),
        )
    passed = context.public_outcome in allowed
    return InvariantResult(
        passed=passed,
        detail=f"public_outcome={context.public_outcome!r}, allowed={allowed!r}",
    )


def _all_scope_resolutions_from_events(
    events: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """Every ``scope_resolution`` block off every ``scope.resolved`` event
    in stream order -- the only event type that carries one
    (``contracts.py``'s per-event allowed-payload mapping).

    Codex round-3 finding (HIGH, confirmed): production's own
    ``validate_stream`` does not forbid more than one ``scope.resolved``
    event in a single run's stream (e.g. a re-resolution mid-investigation)
    -- an earlier version of this helper returned only the FIRST match, so
    an unauthorized candidate surfaced on a SECOND ``scope.resolved`` event
    was invisible to ``no_unauthorized_candidate_surfaces`` even though the
    overall stream passed ``validate_stream`` cleanly. Every caller that
    cares about "the current/final resolution" (e.g.
    ``scope_resolution_outcome_in``) uses the LAST element; every caller
    that cares about "was anything unauthorized EVER surfaced" (e.g.
    ``no_unauthorized_candidate_surfaces``) must scan ALL elements.
    """

    resolutions: list[Mapping[str, Any]] = []
    for event in events:
        if event.get("event") != "scope.resolved":
            continue
        data = event.get("data")
        if isinstance(data, Mapping):
            resolution = data.get("scope_resolution")
            if isinstance(resolution, Mapping):
                resolutions.append(resolution)
    return resolutions


@register_check("scope_resolution_outcome_in")
def _scope_resolution_outcome_in(
    args: Mapping[str, Any], context: InvariantContext
) -> InvariantResult:
    """Assert the wire ``scope.resolved`` event's own
    ``ScopeResolutionOutcome`` (exact/filtered/inherited/
    organization_fallback/ambiguous/unresolved/forbidden_or_not_found) --
    the SCOPE-level enum, distinct from ``resolution_path``'s per-mention
    ``ResolutionOutcome`` classification (see resolution_path.py's module
    docstring for why the two are different vocabularies). Same
    ``allowed``/``from_profile`` seam as the other ``*_in`` checks.

    Uses the LAST ``scope.resolved`` event when a stream carries more than
    one (e.g. a re-resolution mid-investigation) -- the current/final
    resolution is what "the outcome of this run" means.
    """

    allowed = _resolve_allowed(args, context, check_name="scope_resolution_outcome_in")
    resolutions = _all_scope_resolutions_from_events(context.events)
    outcome = resolutions[-1].get("outcome") if resolutions else None
    if outcome is None:
        return InvariantResult(
            passed=False,
            detail=(
                "no scope.resolved event with a scope_resolution.outcome "
                f"was found in the stream; allowed={allowed!r}"
            ),
        )
    passed = outcome in allowed
    return InvariantResult(
        passed=passed,
        detail=f"scope_resolution_outcome={outcome!r}, allowed={allowed!r}",
    )


def _candidate_entity_ids(resolution: Mapping[str, Any]) -> list[str]:
    ids: list[str] = []
    for candidate in resolution.get("candidates") or []:
        if not isinstance(candidate, Mapping):
            continue
        entity_ref = candidate.get("entity_ref")
        if isinstance(entity_ref, Mapping):
            entity_id = entity_ref.get("entity_id")
            if isinstance(entity_id, str):
                ids.append(entity_id)
    return ids


@register_check("no_unauthorized_candidate_surfaces")
def _no_unauthorized_candidate_surfaces(
    args: Mapping[str, Any], context: InvariantContext
) -> InvariantResult:
    """Assert every disambiguation candidate the run surfaced belongs to
    the case's own authorized world-entity set -- the launch threshold
    "zero cross-tenant leakage" / "zero unresolved-subject fabrication",
    applied per case. ``authorized_entity_ids`` (literal list, and/or
    ``from_profile`` pulling it out of the resolved resolution-profile
    block) is REQUIRED -- unlike the ``*_in`` checks' ``allowed``, there is
    no sensible default "everything is authorized" fallback a security
    invariant should ever silently assume.

    Scans EVERY ``scope.resolved`` event in the stream, not only the first
    (Codex round-3, HIGH, confirmed: a stream can legally carry more than
    one, and an earlier version of this check only ever inspected the
    first one -- see :func:`_all_scope_resolutions_from_events`).

    KNOWN TRUST BOUNDARY (Codex round-3, HIGH, evaluated and NOT changed
    here -- reported, not silently accepted): ``authorized_entity_ids`` is
    a value the CASE/PROFILE declares, exactly like every other ``*_in``
    checker's ``allowed`` list -- this function has no independent access
    to the pinned world manifest to verify that declaration is itself
    correct. A case that mis-declares an overly broad authorized set can
    make this check vacuous for that case, same as a case that mis-declares
    an overly broad ``resolution_path_in``/``public_outcome_in`` allowed
    set would. Closing this would mean threading the pinned world/subjects
    registry into every invariant evaluation as an independent oracle --
    a real, larger design change (cross-referencing case-declared
    expectations against the frozen registry), not a bug in this specific
    checker; correctness of the DECLARED authorized set is Lane 2b's case-
    authoring/review responsibility today, not something this generic
    registry enforces at runtime.
    """

    authorized = list(args.get("authorized_entity_ids", []))
    profile_key = args.get("from_profile")
    if profile_key is not None:
        if profile_key not in context.expectations:
            raise InvariantCheckError(
                "no_unauthorized_candidate_surfaces: profile key "
                f"{profile_key!r} not found in resolved expectations "
                f"{dict(context.expectations)!r}"
            )
        authorized.extend(context.expectations[profile_key])
    if not authorized:
        raise InvariantCheckError(
            "no_unauthorized_candidate_surfaces requires a non-empty "
            "'authorized_entity_ids' list and/or a 'from_profile' key "
            "resolvable in the case's resolution profile"
        )
    authorized_set = set(authorized)

    resolutions = _all_scope_resolutions_from_events(context.events)
    candidate_ids = [
        entity_id
        for resolution in resolutions
        for entity_id in _candidate_entity_ids(resolution)
    ]
    offending = [
        entity_id for entity_id in candidate_ids if entity_id not in authorized_set
    ]
    return InvariantResult(
        passed=not offending,
        detail=(
            "no unauthorized candidates surfaced"
            if not offending
            else f"unauthorized candidate id(s) surfaced: {offending!r} "
            f"(authorized: {sorted(authorized_set)!r})"
        ),
    )


#: The closed set of schema versions that mean "a real terminal transcript
#: row exists" -- mirrors ``persistence/service.py``'s own
#: ``_REAL_ANSWER_SCHEMA_VERSIONS`` plus ``dev_error.v1`` (CHAOS-3423's
#: no-answer-terminal row), since ``terminal_persists_assistant_row`` must
#: accept EITHER a genuine answer or a genuine no-answer-terminal error row
#: -- both are "the terminal persisted a transcript row", which is exactly
#: what CHAOS-3423 exists to guarantee.
_REAL_TERMINAL_SCHEMA_VERSIONS = frozenset(
    {"dev_answer.v1", "dev_answer.v2", "dev_error.v1"}
)


@register_check("terminal_persists_assistant_row")
def _terminal_persists_assistant_row(
    _args: Mapping[str, Any], context: InvariantContext
) -> InvariantResult:
    """Assert CHAOS-3423's own guarantee from the corpus side: every
    terminal run leaves EXACTLY ONE real ``dev_messages`` assistant row
    (``dev_answer.v1``/``dev_answer.v2``/``dev_error.v1``) for its
    conversation. Needs ``assistant_schema_versions`` populated via the
    docker-exec verification plane (``db_verify.
    query_transcript_assistant_schema_versions_via_exec``) -- the public
    wire alone cannot answer "did persistence actually happen".

    Codex round-3 finding (MEDIUM, confirmed): the original version only
    checked "at least one recognized row exists", which would also pass for
    a DUPLICATE or stale extra assistant row left over from a retry/replay
    bug -- exactly the kind of persistence defect CHAOS-3423/replay-safety
    exists to prevent. Tightened to require the conversation's assistant
    rows be EXACTLY one, and that one recognized.

    ASSUMPTION, explicit: the corpus runner creates a FRESH conversation
    per case (one turn each) -- see ``test_wave4_corpus_runner_live.py``'s
    ``test_corpus_case``. "Exactly one assistant row in the whole
    conversation" is therefore equivalent to "exactly one for this run"
    today. A future MULTI-TURN corpus case (e.g. the two-turn acronym-alias
    disambiguation flow ``resolution_path.py``'s module docstring
    describes) would need this checker to scope by the run/message
    boundary instead of the whole conversation -- not built here since no
    such case exists yet; noted so it isn't rediscovered as a surprise.
    """

    matching = [
        version
        for version in context.assistant_schema_versions
        if version in _REAL_TERMINAL_SCHEMA_VERSIONS
    ]
    total = len(context.assistant_schema_versions)
    passed = total == 1 and len(matching) == 1
    if passed:
        detail = f"exactly one real terminal transcript row found: {matching!r}"
    elif total == 0:
        detail = "no assistant row found at all for this conversation"
    elif total > 1:
        detail = (
            f"expected exactly one assistant row, found {total}: "
            f"{list(context.assistant_schema_versions)!r}"
        )
    else:
        detail = (
            "the conversation's one assistant row is not a recognized "
            f"dev_answer.v1/dev_answer.v2/dev_error.v1 schema_version: "
            f"{list(context.assistant_schema_versions)!r}"
        )
    return InvariantResult(passed=passed, detail=detail)


def evaluate_invariant(
    entry: Mapping[str, Any], context: InvariantContext
) -> InvariantResult:
    """Dispatch one case ``invariants[]`` entry to its registered checker."""

    check_name = entry.get("check")
    if not isinstance(check_name, str):
        raise InvariantCheckError(f"invariant entry has no string 'check': {entry!r}")
    fn = CHECKS.get(check_name)
    if fn is None:
        raise InvariantCheckError(
            f"unknown invariant check {check_name!r} -- known checks: "
            f"{sorted(CHECKS)!r}"
        )
    args = entry.get("args", {})
    if not isinstance(args, Mapping):
        raise InvariantCheckError(
            f"invariant entry 'args' must be an object: {entry!r}"
        )
    return fn(args, context)
