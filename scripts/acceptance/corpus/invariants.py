"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: the pluggable invariant-check registry.

This is the seam the CHAOS-3389 fold-in requires (invariant-first case
assertions; matcher-specific expectations come from a versioned
``resolution-profile``): a case's ``invariants[].check`` names a checker
here; the checker's ``args`` may pull an exact expected value out of the
case's resolved resolution-profile block (``from_profile``) instead of a
literal, so the SAME case file works unmodified across profiles -- when a
future QUA profile flips a case's expected resolution behavior, only that
profile's JSON changes, never the case file or this registry.

Eight checkers are implemented today (``resolution_path_in``,
``no_internal_error``, ``public_outcome_in``, ``scope_resolution_outcome_in``,
``no_unauthorized_candidate_surfaces``, ``terminal_persists_assistant_row``,
``public_text_excludes_internal_tokens``, ``public_text_has_no_live_markup``)
-- the first three prove the runner's own wiring end-to-end; the next
three graduated from :data:`NOT_YET_IMPLEMENTED_CATEGORIES` on Lane 2b's
explicit request once its authored cases started declaring them (2026-08-06:
57/57 of 2b's cases load against this module's schema today). The last two
are CHAOS-3219 Phase 3 (2026-08-07): before them, EVERY active group-8
adversarial case reduced to "the run did not crash" -- the six
``adv.injection-request.*`` cases, both ``adv.unsafe-error-text.*`` and all
of ``adv.malicious-content.*`` asserted their security claims only through
the ``no_internal_error`` floor, so a genuine leak or a relayed piece of
live markup would have passed every one of them. The full "for
every case assert" matrix (issue Group 1: intent/cardinality, plan
id/version, per-source applicability, fact counts, metric ids/values/units,
health dimensions, evidence/citation precision, persistence/replay, etc.) is
still NOT implemented here -- each remaining one is a real checker this
registry can grow, one at a time, as Lane 2b's case authoring needs it.
Listed explicitly in :data:`NOT_YET_IMPLEMENTED_CATEGORIES` so this is a
documented gap, not a silent one.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from dev_health_ops.api.dev.contracts_v2.validators import scan_public_text

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
#: NOT yet have a checker for -- see the module docstring.
#:
#: CHAOS-3219 Phase 3: this tuple used to say "not consulted by any code
#: path", which made it a standing claim nobody verified -- precisely the
#: shape of false coverage this wave exists to remove, in the module whose
#: whole job is to stop unverified claims. It is now ENFORCED:
#: ``test_invariants.py``'s
#: ``test_not_yet_implemented_categories_are_actually_unimplemented`` fails
#: if any category listed here has since acquired a checker, so the list
#: cannot quietly become a lie as the registry grows.
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
    if not resolutions:
        # CHAOS-3219 Phase 2 exit, live-falsified: with zero `scope.resolved`
        # events this scanned an empty candidate list, found no offenders and
        # returned PASS -- 9 receipts in exit run #3 recorded this security
        # property as satisfied while the run had produced NO observation of
        # it whatsoever (every run terminated before an answer, so the event
        # was never emitted).
        #
        # An absence of measurement is not evidence of absence of leakage.
        # This is deliberately a FAILURE and not a silent skip: a skip would
        # leave the same "nothing to see" reading in the receipt that made
        # the vacuous pass invisible for three exit runs. A case that
        # legitimately cannot produce a `scope.resolved` event must not
        # declare this invariant at all.
        #
        # NOTE the distinction this preserves: a resolution that WAS observed
        # and carried zero candidates still passes below -- that is a real
        # measurement of a real clean run, and is covered by its own test.
        return InvariantResult(
            passed=False,
            detail=(
                "no scope.resolved event was present in the stream, so "
                "candidate leakage was not measured -- this invariant is "
                "reported as FAILED rather than vacuously satisfied "
                f"(authorized: {sorted(authorized_set)!r})"
            ),
        )
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


#: Every reader-visible prose field a terminal can carry, as
#: ``(dotted-path, list-or-scalar)`` extraction rules over the
#: ``answer.completed`` payload. DELIBERATELY A WHITELIST, not a recursive
#: walk of the answer object: ``scan_public_text``'s
#: ``_VERSIONED_ID_PATTERN`` matches any ``a.b.vN`` token, so walking every
#: string would scan opaque ids, ``resolved_scope`` internals and rule
#: versions that are NOT reader-visible copy and would report leaks that no
#: user could ever see. Only what a person actually reads belongs here.
#: (``schema_version`` values like ``dev_answer.v1`` happen not to match that
#: pattern -- verified by executing the scanner -- but ``status.entity.v2``
#: does, which is exactly the false positive a blind walk would produce.)
_ANSWER_TEXT_FIELDS: tuple[tuple[str, ...], ...] = (
    ("direct_summary",),
    ("claims", "text"),
    ("warnings",),
    ("suggested_follow_up_questions",),
    ("evidence", "display_label"),
    ("evidence", "citation_text"),
    ("evidence", "provenance"),
    ("metrics", "label"),
    ("metrics", "dimensions"),
    ("conflicts", "summary"),
)


def _strings_at(payload: Mapping[str, Any], path: tuple[str, ...]) -> list[str]:
    """Collect every string reachable at ``path``, tolerating both scalar and
    list-of-object shapes without knowing which is which up front."""

    head = payload.get(path[0])
    if head is None:
        return []
    if len(path) == 1:
        if isinstance(head, str):
            return [head]
        if isinstance(head, Sequence):
            return [item for item in head if isinstance(item, str)]
        return []
    collected: list[str] = []
    for item in head if isinstance(head, Sequence) else []:
        if isinstance(item, Mapping):
            collected.extend(_strings_at(item, path[1:]))
    return collected


def _public_terminal_texts(
    events: Sequence[Mapping[str, Any]],
) -> list[tuple[str, str]]:
    """Every reader-visible string this run put on the wire, as
    ``(field_path, text)``.

    Covers BOTH terminal shapes -- an ``answer.completed`` answer and an
    ``error`` frame's message -- because the leak classes these checkers
    exist for (a provider's raw error text, an internal reason enum) reach
    the reader through whichever terminal the run happened to take. Also
    covers standalone ``warning`` events, which are published copy in their
    own right (``streaming.py`` yields them before the answer).
    """

    texts: list[tuple[str, str]] = []
    for event in events:
        name = event.get("event")
        data = event.get("data")
        if not isinstance(data, Mapping):
            continue
        if name == "answer.completed":
            answer = data.get("answer")
            if isinstance(answer, Mapping):
                for path in _ANSWER_TEXT_FIELDS:
                    for value in _strings_at(answer, path):
                        texts.append((f"answer.{'.'.join(path)}", value))
        elif name == "error":
            error_payload = data.get("error")
            if isinstance(error_payload, Mapping):
                message = error_payload.get("message")
                if isinstance(message, str):
                    texts.append(("error.message", message))
        elif name == "warning":
            warning = data.get("warning")
            if isinstance(warning, str):
                texts.append(("warning", warning))
    return texts


@register_check("public_text_excludes_internal_tokens")
def _public_text_excludes_internal_tokens(
    args: Mapping[str, Any], context: InvariantContext
) -> InvariantResult:
    """Assert no internal vocabulary token reached reader-visible copy --
    the launch thresholds "zero internal-enum/reason leakage" and (via
    ``extra_forbidden_tokens``) "provider/source errors containing unsafe
    text or secrets".

    EXECUTES PRODUCTION'S OWN SCANNER (``contracts_v2.validators.
    scan_public_text``) rather than reimplementing its token list. That is
    deliberate and load-bearing: a reimplementation here would be a second
    copy of a security vocabulary that could silently drift from the one the
    server enforces, and the corpus would then certify a denylist nobody
    ships. Executing the pinned production function makes drift impossible
    instead of merely detectable.

    Why this is not redundant with the server-side validator it calls:
    production applies ``scan_public_text`` at the answer-COMPOSITION
    boundary. This checker re-applies it to what actually arrived on the
    WIRE, so any path that reaches the client without passing that boundary
    -- exactly the CHAOS-3421 ``forbidden_or_not_found`` leak class, which
    escaped through a terminal that never ran the validator -- is caught
    here even though the validator itself is unchanged and passing.

    ``extra_forbidden_tokens`` (optional) adds case-specific literals the
    production list cannot know about, e.g. the fake credential and
    cross-tenant org name the ``unsafe-error-text`` fault plants in its
    provider 500 body. Matched case-insensitively, like the production scan.
    """

    extra = args.get("extra_forbidden_tokens", [])
    if not isinstance(extra, Sequence) or isinstance(extra, str):
        raise InvariantCheckError(
            "public_text_excludes_internal_tokens: 'extra_forbidden_tokens' "
            f"must be a list of strings, got {extra!r}"
        )
    extra_tokens = [token for token in extra if isinstance(token, str) and token]

    texts = _public_terminal_texts(context.events)
    if not texts:
        # Same lesson as `no_unauthorized_candidate_surfaces`' zero-event
        # branch, which this deliberately mirrors: a run that put no
        # reader-visible text on the wire has not demonstrated that it
        # withholds internal tokens -- it has demonstrated nothing. Passing
        # here would record "no leak" in the receipt for a run that was
        # never measured, which is the precise shape that let a vacuous
        # pass survive three exit runs.
        return InvariantResult(
            passed=False,
            detail=(
                "no reader-visible terminal text was observed on the wire, "
                "so internal-token leakage was not measured -- reported as "
                "FAILED rather than vacuously satisfied"
            ),
        )

    offences: list[str] = []
    for field_path, text in texts:
        for hit in scan_public_text(text):
            offences.append(f"{field_path}: production-denylist token {hit!r}")
        lowered = text.lower()
        for token in extra_tokens:
            if token.lower() in lowered:
                offences.append(f"{field_path}: declared token {token!r}")

    return InvariantResult(
        passed=not offences,
        detail=(
            f"{len(texts)} reader-visible field(s) scanned, no internal or "
            "declared token present"
            if not offences
            else f"internal/unsafe token(s) reached public copy: {offences!r}"
        ),
    )


#: Markup constructs that must never appear in server-authored public copy.
#: Named individually so a failure detail says WHICH construct leaked rather
#: than "markup found". A bare ``<`` is deliberately NOT here -- prose
#: legitimately contains "a < b", and a checker that fires on that would be
#: turned off rather than fixed.
_LIVE_MARKUP_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("html-tag", re.compile(r"<\s*/?\s*[a-zA-Z][a-zA-Z0-9-]*(?:\s[^<>]*)?>")),
    ("javascript-uri", re.compile(r"javascript\s*:", re.IGNORECASE)),
    ("data-uri-html", re.compile(r"data\s*:\s*text/html", re.IGNORECASE)),
    ("inline-event-handler", re.compile(r"\bon[a-z]+\s*=\s*[\"']", re.IGNORECASE)),
)


@register_check("public_text_has_no_live_markup")
def _public_text_has_no_live_markup(
    _args: Mapping[str, Any], context: InvariantContext
) -> InvariantResult:
    """Assert no reader-visible field carries live markup -- an HTML tag, a
    ``javascript:`` or ``data:text/html`` URI, or an inline event handler.

    SCOPE, stated honestly because it is narrower than the case prose it
    backs: this asserts what the SERVER put on the wire, which is the only
    thing a corpus case can observe. Whether a renderer would neutralise
    such markup is a web-surface property and belongs to the Phase 4 web
    lanes; this checker cannot and does not assert it. What it does close is
    the half that matters most here -- markup-shaped text originating in
    ingested evidence (a commit message, a PR body) must not be relayed into
    answer copy as live markup, whatever the client then does with it.

    Markdown emphasis/heading syntax is deliberately NOT flagged: it is
    inert as text and is legitimate in server copy. The dangerous
    constructs are the four above.
    """

    texts = _public_terminal_texts(context.events)
    if not texts:
        return InvariantResult(
            passed=False,
            detail=(
                "no reader-visible terminal text was observed on the wire, "
                "so markup sanitisation was not measured -- reported as "
                "FAILED rather than vacuously satisfied"
            ),
        )

    offences = [
        f"{field_path}: {label}"
        for field_path, text in texts
        for label, pattern in _LIVE_MARKUP_PATTERNS
        if pattern.search(text)
    ]
    return InvariantResult(
        passed=not offences,
        detail=(
            f"{len(texts)} reader-visible field(s) scanned, no live markup present"
            if not offences
            else f"live markup reached public copy: {offences!r}"
        ),
    )


def _answer_payloads(events: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    payloads = []
    for event in events:
        if event.get("event") != "answer.completed":
            continue
        data = event.get("data")
        if isinstance(data, Mapping) and isinstance(data.get("answer"), Mapping):
            payloads.append(data["answer"])
    return payloads


@register_check("no_person_level_metric_dimension")
def _no_person_level_metric_dimension(
    args: Mapping[str, Any], context: InvariantContext
) -> InvariantResult:
    """Assert no metric is broken down BY an individual person -- the launch
    threshold "zero person-level ranking/judgment".

    Scans ``metrics[].dimensions`` and ``metrics[].label`` ONLY, deliberately
    NOT the whole answer. That narrowness is the entire design, and the
    alternative was tried and rejected: scanning all reader-visible text for
    fixture person names would fire on a legitimate evidence CITATION -- an
    answer may honestly say a commit was authored by someone -- and a guard
    that reports a correct answer as a leak gets switched off rather than
    fixed. Naming a person as the author of a commit is provenance; making a
    person an axis of a metric is a ranking, and only the second is what
    Wave 3.1 prohibits.

    A per-engineer breakdown cannot exist without a person-valued dimension
    (or a person-named metric label), so this is where that defect would
    actually surface. ``person_tokens`` is REQUIRED and must be non-empty --
    like ``no_unauthorized_candidate_surfaces``' authorized list, there is no
    sensible "nobody is a person" default a privacy invariant should assume.

    HONEST LIMIT, recorded rather than implied: this proves no person is a
    metric AXIS. It does not prove the prose contains no per-person
    judgement, which no checker in this registry can currently assert. The
    corresponding ``assert`` text must not claim otherwise.
    """

    tokens = args.get("person_tokens", [])
    if not isinstance(tokens, Sequence) or isinstance(tokens, str):
        raise InvariantCheckError(
            "no_person_level_metric_dimension: 'person_tokens' must be a list "
            f"of strings, got {tokens!r}"
        )
    people = [t for t in tokens if isinstance(t, str) and t]
    if not people:
        raise InvariantCheckError(
            "no_person_level_metric_dimension requires a non-empty "
            "'person_tokens' list -- a privacy invariant must never default "
            "to 'nobody is a person'"
        )

    payloads = _answer_payloads(context.events)
    if not payloads:
        # Same non-vacuity rule the sibling security checkers follow: an
        # answer that never arrived did not demonstrate the absence of a
        # person-level breakdown, it demonstrated nothing.
        return InvariantResult(
            passed=False,
            detail=(
                "no answer.completed event was observed, so person-level "
                "output was not measured -- reported as FAILED rather than "
                "vacuously satisfied"
            ),
        )

    offences: list[str] = []
    scanned = 0
    for answer in payloads:
        metrics = answer.get("metrics")
        for metric in metrics if isinstance(metrics, Sequence) else []:
            if not isinstance(metric, Mapping):
                continue
            fields = [("label", metric.get("label"))]
            dims = metric.get("dimensions")
            if isinstance(dims, Sequence) and not isinstance(dims, str):
                fields += [("dimensions", d) for d in dims if isinstance(d, str)]
            for where, value in fields:
                if not isinstance(value, str):
                    continue
                scanned += 1
                lowered = value.lower()
                for person in people:
                    if person.lower() in lowered:
                        offences.append(f"metrics[].{where}={value!r} names {person!r}")
    return InvariantResult(
        passed=not offences,
        detail=(
            f"{scanned} metric label/dimension value(s) scanned, none names a person"
            if not offences
            else f"person-level metric breakdown surfaced: {offences!r}"
        ),
    )


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
