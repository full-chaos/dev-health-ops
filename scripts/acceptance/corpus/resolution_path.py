"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: derive a ``wave4_case_result.v1``
receipt's ``resolution_path`` field from persisted CHAOS-3423/3424 state.

Enum (team-lead decree, binding, also fixed with the CHAOS-3389 QUA shadow
fold-in): exactly ``deterministic-exact | deterministic-alias |
miss-clarification | qua-shadow | qua-committed``, kebab-case. Only the
first three are ever produced by this module today -- ``qua-shadow`` and
``qua-committed`` are reserved for the future Question Understanding Agent
shadow-replay mode (CHAOS-3389) and are never emitted here; a caller wiring
that mode in later assigns them directly, this module has no opinion on it.

WHY THIS MUST BE RE-DERIVED, NOT READ OFF A COLUMN (recon finding,
2026-08-05): ``dev_run_resolutions.outcome`` is the closed
``ResolutionOutcome`` vocabulary (``exact_match`` / ``ambiguous_candidates``
/ ``no_authorized_match`` / ``catalog_unavailable`` / ``unsupported_kind``)
-- it does not distinguish "matched the mention text directly" from "matched
via a CHAOS-3388 derived alias/acronym form". Worse, ``alias_matching.py``'s
own contract (see its module docstring) is that an alias/acronym hit is
*never* auto-commit eligible -- it must always be offered as a candidate
first (``ambiguous_candidates``), never silently picked. So a single ledger
entry's ``exact_match`` outcome, on its own, actually already tells you the
match was NOT an alias/acronym hit (those can't produce ``exact_match``
directly) -- UNLESS a *later* entry for the same mention commits an entity
that an *earlier* entry for that mention had offered as a candidate, which
is exactly what a real acronym-alias corpus case's two-turn disambiguation
flow (ask -> get candidates -> user selects one -> commits) looks like on
the wire. That "did this mention's own history include a candidate offer
before it committed" check is therefore the PRIMARY signal; a label-based
fallback (recomputing ``alias_matching.alias_forms`` against the case's own
known mention text) exists only to positively confirm a single-entry
``exact_match`` really is a direct match, not silently assume it.

Only ``ambiguous_candidates`` is ever "a candidate was genuinely offered"
evidence (Codex round-1 finding, HIGH, confirmed against
``DevResolutionEntry.validate_outcome_payload``: ``no_authorized_match`` /
``catalog_unavailable`` / ``unsupported_kind`` are structurally forbidden
from carrying ``candidates`` at all). Treating any of those three as
candidate-evidence would mislabel "transient failure, then a genuine direct
match on retry" as alias-assisted convergence -- fixed here to check
``ambiguous_candidates`` specifically, never the whole unresolved set.

``classify_match_kind`` (Codex round-1 finding, HIGH, confirmed by executing
it against this repo's real ``subjects.json`` fixture data): the original
implementation accepted ANY bidirectional substring as "exact" -- both a
false positive ("meridian/web-app extra" against "meridian/web-app") and,
separately, too narrow for a real canonical-ID-keyed exact match (production
``ClickHouseAuthorizedEntityCatalog.exact`` matches ``ref.value.casefold() in
{canonical_id.casefold(), label.casefold()}`` -- literal equality against
EITHER the id or the raw label, never a substring). Rewritten to mirror that
exact contract: equality against the committed canonical id (when supplied),
the raw committed label, or its parenthetical-stripped primary form: never a
substring test. A mention that only substring-matches was never a
production ``exact_match`` in the first place (it would have gone through
``search``/candidates instead) -- so a caller handing this function such a
pair has a data problem this function is right to reject loudly, not paper
over.

RESIDUAL, DOCUMENTED HONESTLY: whether a follow-up disambiguation turn's
``dev_run_resolutions`` entry reuses the ORIGINAL mention's ``mention_id`` or
mints a fresh one (a new mention extracted from the follow-up text) has not
yet been observed against a real multi-turn acronym-alias corpus case (Lane
2b has not authored one yet). Both branches are handled here: the
mention-id-history check covers a reused id; the label-based fallback covers
a fresh id whose committed label is still only alias-reachable from the
case's declared mention text. If a live run surfaces a THIRD shape neither
branch covers, that is a defect in this module to fix once observed --
not something this docstring should overclaim proof of today.

CALLER CONTRACT for ``mention_text`` (tightened after Codex round-1):
callers MUST supply the exact, already-normalized span that reached the
resolver -- e.g. ``DevSubjectMention.normalized_lookup_text`` or a case's own
explicitly pinned lookup string -- NEVER a full natural-language utterance
lifted verbatim from ``subjects.json``'s human-readable ``mentions`` array
(several of those, e.g. "the web-app repo", are descriptive phrases, not
resolver input; only the acronym-alias subject class's ``mentions`` entries
are documented as literal spans). Passing the wrong string here does not
silently misclassify -- it raises, per the equality-only contract above --
but choosing the right string is the caller's responsibility, not
something this module can infer.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from dev_health_ops.api.dev.alias_matching import alias_forms, strip_parentheticals

__all__ = [
    "ResolutionPath",
    "ResolutionPathError",
    "ResolutionLedgerEntry",
    "attach_mention_texts",
    "classify_match_kind",
    "derive_resolution_path",
]

ResolutionPath = Literal[
    "deterministic-exact",
    "deterministic-alias",
    "miss-clarification",
    "qua-shadow",
    "qua-committed",
]

#: Mirrors ``dev_health_ops.api.dev.contracts_v2.subject.UNRESOLVED_OUTCOMES``
#: as plain strings -- this module deliberately never imports the ORM/session
#: layer or the contracts_v2 pydantic models, only the pure alias-scoring
#: helper, so it has no opinion on how a caller obtained these values (a real
#: ``DevRunResolution`` row's ``.outcome`` column, or a fabricated string in a
#: unit test).
_UNRESOLVED_OUTCOMES = frozenset(
    {
        "ambiguous_candidates",
        "no_authorized_match",
        "catalog_unavailable",
        "unsupported_kind",
    }
)
_KNOWN_OUTCOMES = _UNRESOLVED_OUTCOMES | {"exact_match"}

#: The ONLY outcome that can ever carry candidates
#: (``DevResolutionEntry.validate_outcome_payload``'s own contract) -- the
#: sole legitimate evidence that "a candidate was offered before this
#: mention committed". ``no_authorized_match``/``catalog_unavailable``/
#: ``unsupported_kind`` are structurally forbidden from carrying candidates
#: and must never be treated as alias-assisted-convergence evidence.
_CANDIDATE_BEARING_OUTCOME = "ambiguous_candidates"


class ResolutionPathError(Exception):
    """The ledger cannot be classified as any known resolution path.

    Raised rather than defaulted to a guess: a receipt that silently
    reported ``deterministic-exact`` for a row this module could not
    actually explain would be exactly the false-confidence this lane exists
    to prevent (an outcome value outside the CHECK-constrained vocabulary,
    or an ``exact_match`` entry whose committed label matches neither
    directly nor via any alias form of the case's own declared mention
    text).
    """


@dataclass(frozen=True, slots=True)
class ResolutionLedgerEntry:
    """The minimal, backend-agnostic shape this module needs off one
    resolution-ledger entry.

    Callers build this from either a real ``DevRunResolution`` ORM row (an
    integration/live test) or a fabricated one (a fast unit test) -- this
    module never imports SQLAlchemy so it cannot itself be the reason a live
    run needs a database.

    ``mention_text`` is NOT read off the persisted row -- ``DevResolutionEntry``
    (contracts_v2/subject.py) carries only an opaque ``mention_id``, never the
    original text span (that lives on a separate, not-independently-persisted
    ``DevSubjectMention``). Callers supply it out-of-band -- see the module
    docstring's "CALLER CONTRACT" section for exactly what string that must
    be. Required for ``exact_match`` entries with no prior candidate offer
    (needed to classify exact-vs-alias); may be ``None`` for an unresolved
    entry, which never needs that classification.

    ``committed_canonical_id`` is the committed entity's own id (e.g.
    ``DevEntityRefV2.entity_id``) -- optional, but when supplied lets a
    mention that names the id directly (rather than any display label)
    classify as ``exact`` too, matching production's own
    ``ClickHouseAuthorizedEntityCatalog.exact`` contract (id-or-label
    equality).
    """

    outcome: str
    mention_id: str
    committed_label: str | None = None
    committed_canonical_id: str | None = None
    mention_text: str | None = None


def classify_match_kind(
    mention_text: str,
    committed_label: str,
    *,
    committed_canonical_id: str | None = None,
) -> Literal["exact", "alias"]:
    """Whether ``committed_label``/``committed_canonical_id`` matches
    ``mention_text`` directly or only through a CHAOS-3388 derived
    alias/acronym form.

    "Directly" mirrors production's own
    ``ClickHouseAuthorizedEntityCatalog.exact`` contract exactly: casefolded
    EQUALITY (never a substring test) against the committed canonical id (if
    supplied), the raw committed label, or the label's parenthetical-stripped
    primary form. Anything that only lines up through
    ``alias_matching.alias_forms`` (a parenthetical literal alias or a
    derived acronym of the label) is "alias".

    Raises :class:`ResolutionPathError` if none of those match -- this can
    happen if the caller passed the wrong mention text (see the module
    docstring's "CALLER CONTRACT"), or the catalog label changed between
    resolution time and this later recomputation. A mention that only
    substring-matches the label was never a production ``exact_match`` in
    the first place (production's own ``exact()`` never accepts a
    substring) -- so silently accepting one here would misclassify by
    definition, not merely by bad luck.
    """

    needle = mention_text.strip().casefold()
    if not needle or not committed_label.strip():
        raise ResolutionPathError(
            "cannot classify an empty mention_text or committed_label "
            f"(mention_text={mention_text!r}, committed_label={committed_label!r})"
        )
    if (
        committed_canonical_id is not None
        and needle == committed_canonical_id.strip().casefold()
    ):
        return "exact"
    raw_label = committed_label.strip().casefold()
    if needle == raw_label:
        return "exact"
    primary, _aliases = strip_parentheticals(committed_label)
    if primary.strip() and needle == primary.strip().casefold():
        return "exact"
    forms = alias_forms(committed_label)
    if needle in forms.literal_aliases or needle in forms.acronyms:
        return "alias"
    raise ResolutionPathError(
        f"committed label {committed_label!r} (canonical id "
        f"{committed_canonical_id!r}) matches neither directly (equality) "
        f"nor via any alias/acronym form of mention text {mention_text!r} -- "
        "cannot classify exact vs alias. If this mention text is a natural-"
        "language utterance rather than the normalized resolver-input span, "
        "that is the likely cause -- see this module's CALLER CONTRACT."
    )


def attach_mention_texts(
    entries: Sequence[ResolutionLedgerEntry], mention_texts: Sequence[str]
) -> list[ResolutionLedgerEntry]:
    """Attach each mention's declared lookup text to its ledger entries.

    CHAOS-3462 B6 closes the gap this module's docstring named as its own
    residual: ``DevResolutionEntry`` never persists the mention span, so the
    docker-exec ledger read returns entries with ``mention_text=None`` and
    every single-shot ``exact_match`` was unclassifiable --
    ``deterministic-exact`` was dead vocabulary for the whole corpus. The
    case now DECLARES the spans (``expected_mention_texts``), derived from
    production's own ``QuestionInterpreter`` rather than hand-authored, and
    this function threads them onto the entries the exec plane returned.

    ORDERING CONTRACT, and why positional mapping is sound here rather than
    a guess: ``subject_preflight._build_ledger`` builds entries by
    ``zip(mentions, resolutions, strict=True)`` and stamps ``entry_ordinal``
    from that index, and ``_inner_ledger_query`` orders by
    ``entry_ordinal``. So distinct ``mention_id`` values, in first-seen
    order, correspond one-to-one with the question's mentions in the order
    the interpreter produced them -- which is the order
    ``expected_mention_texts`` is declared in, asserted against the real
    interpreter by the corpus guard test.

    THE TWO COUNT MISMATCHES ARE NOT SYMMETRIC, and treating them as if they
    were is a defect this function had until it was attacked directly:

    * MORE observed mentions than declared -> RAISE. The declaration is
      genuinely short, so at least one real mention would get no span, and
      positional mapping past the end is meaningless. That is a corpus
      defect to fix, not to absorb.
    * FEWER observed than declared -> attach NOTHING, and let
      ``derive_resolution_path`` proceed on the raw entries. This is a
      LEGITIMATE shape, not drift. A PROCEED ledgers every mention
      (``_build_ledger`` zips with ``strict=True``) and ``append_resolution``
      only flushes, so a partial PROCEED write is not possible -- a failure
      there rolls back to ZERO rows, which lands in the empty-ledger branch
      of :func:`derive_resolution_path`, not here. A NON-EMPTY short ledger
      therefore comes from the TERMINATE path, which persists ONLY a
      ``terminating_resolution_entry``, and ``_terminate`` sets that solely
      for ``ambiguous_candidates``. Such an entry never needs a span: its
      mention's final outcome is not ``exact_match``, so
      ``derive_resolution_path`` short-circuits to ``miss-clarification``
      without ever consulting ``mention_text``. Raising here would turn a
      correct, classifiable run RED with a message blaming the case author
      for drift that did not happen -- reproduced with a two-mention case
      that terminates ambiguous on one of them.

      Residual, stated rather than assumed away: if a short ledger ever DID
      carry an ``exact_match``, this returns it unattached and
      :func:`derive_resolution_path` raises its "no mention_text was
      supplied" error -- a true failure, but one whose message points at the
      wrong cause. No known code path produces that shape today.

    Attaching positionally in that short case would be worse than attaching
    nothing: the surviving entry is not necessarily the FIRST mention, so
    the mapping could silently pair a real mention with another mention's
    span.
    """

    order: list[str] = []
    for entry in entries:
        if entry.mention_id not in order:
            order.append(entry.mention_id)
    if len(order) > len(mention_texts):
        raise ResolutionPathError(
            f"the ledger carries {len(order)} distinct mention(s) "
            f"({order!r}) but the case declares only {len(mention_texts)} "
            f"expected_mention_texts ({list(mention_texts)!r}) -- at least "
            "one real mention would get no span. The case's declaration has "
            "drifted from what the interpreter produces for its question; "
            "regenerate it from the interpreter."
        )
    if len(order) < len(mention_texts):
        # Partial (terminating) ledger -- see the docstring. Nothing to
        # attach, and nothing that needs attaching.
        return list(entries)
    text_by_mention = dict(zip(order, mention_texts, strict=True))
    return [
        ResolutionLedgerEntry(
            outcome=entry.outcome,
            mention_id=entry.mention_id,
            committed_label=entry.committed_label,
            committed_canonical_id=entry.committed_canonical_id,
            mention_text=(
                entry.mention_text
                if entry.mention_text is not None
                else text_by_mention[entry.mention_id]
            ),
        )
        for entry in entries
    ]


def derive_resolution_path(
    entries: Sequence[ResolutionLedgerEntry],
) -> ResolutionPath | None:
    """The ``wave4_case_result.v1`` ``resolution_path`` for one case's full
    resolution history.

    ``entries`` must already be reduced to the caller's intended scope, in
    chronological order (ascending ``entry_ordinal`` within a run; runs in
    the order they occurred within the same conversation for a multi-turn
    case) -- this module does not itself know how many runs a case's
    conversation spans, only classifies whatever sequence it is handed.

    Returns ``None`` for an empty sequence -- a case whose subject class is
    ``n/a`` (no subject mention at all: ``portfolio.*``, ``investment.*``,
    etc.) never appends to ``dev_run_resolutions``, and reporting a
    resolution path for a run that resolved no subject would be a
    fabricated field, not an honest absence. Callers must not coerce
    ``None`` to a default path value -- and note ``invariants.py``'s
    ``resolution_path_in`` checker independently refuses to ever match
    ``None`` against ANY allowed list, precisely to close that path at the
    assertion layer too.

    Classification, per distinct ``mention_id`` (in first-seen order, since
    that is the order the case's own subject mentions were made in):

    * If any entry for that mention has an unresolved outcome, AND NO later
      entry for the SAME mention_id ever commits it (``exact_match``), that
      mention counts as unresolved.
    * Otherwise the mention resolves, via its LAST entry (which must be
      ``exact_match`` given the above) -- classified ``alias`` if any
      EARLIER entry for that same mention_id was specifically
      ``ambiguous_candidates`` (the only outcome that can carry a candidate
      offer -- the two-turn disambiguation shape), else via
      :func:`classify_match_kind` against the mention's own declared text
      and the committed label/id (the single-shot fallback).

    The whole case is ``miss-clarification`` if ANY mention never resolves.
    Otherwise it is ``deterministic-alias`` if ANY resolved mention
    classified as ``alias``, else ``deterministic-exact``.
    """

    if not entries:
        return None

    unknown = {entry.outcome for entry in entries} - _KNOWN_OUTCOMES
    if unknown:
        raise ResolutionPathError(
            f"resolution ledger entry outcome(s) outside the known "
            f"ResolutionOutcome vocabulary: {sorted(unknown)!r}"
        )

    by_mention: dict[str, list[ResolutionLedgerEntry]] = {}
    order: list[str] = []
    for entry in entries:
        if entry.mention_id not in by_mention:
            by_mention[entry.mention_id] = []
            order.append(entry.mention_id)
        by_mention[entry.mention_id].append(entry)

    saw_alias = False
    for mention_id in order:
        history = by_mention[mention_id]
        final = history[-1]
        if final.outcome != "exact_match":
            # The mention's own last word is still unresolved -- the whole
            # case never converged on a committed subject for it.
            return "miss-clarification"
        candidate_previously_offered = any(
            entry.outcome == _CANDIDATE_BEARING_OUTCOME for entry in history[:-1]
        )
        if candidate_previously_offered:
            saw_alias = True
            continue
        if final.mention_text is None or final.committed_label is None:
            raise ResolutionPathError(
                f"mention {mention_id!r} resolved to exact_match on its first "
                "entry with no prior candidate offer, but no mention_text/"
                "committed_label was supplied -- cannot confirm this is a "
                "direct match rather than an (impossible-by-contract, but "
                "unverifiable without the text) single-shot alias commit"
            )
        if (
            classify_match_kind(
                final.mention_text,
                final.committed_label,
                committed_canonical_id=final.committed_canonical_id,
            )
            == "alias"
        ):
            saw_alias = True

    return "deterministic-alias" if saw_alias else "deterministic-exact"


#: ``resolution_path`` is absent because the run really was queried and its
#: ``dev_run_resolutions`` ledger really was empty. HONEST: a zero-mention
#: question (``portfolio.*``, ``investment.*``, and any question whose text
#: carries no extractable subject span) appends nothing, and reporting a
#: path for it would be fabrication. Not a defect in the measurement.
ABSENCE_EMPTY_LEDGER = "empty-resolution-ledger"

#: ``resolution_path`` is absent because the stream never yielded a run_id,
#: so the ledger was NEVER QUERIED. This is a broken measurement, not an
#: observation of absence, and callers must surface it as a failure.
ABSENCE_RUN_ID_NOT_OBSERVED = "run-id-not-observed"

#: ``resolution_path`` is absent because the ledger WAS queried and was NOT
#: empty, but :func:`derive_resolution_path` could not classify it (a
#: ``ResolutionPathError``: declared mention spans drifted from what the run
#: actually resolved, or an entry shape this module does not handle).
#:
#: Codex adversarial review (MEDIUM, confirmed): this used to collapse into
#: ``ABSENCE_EMPTY_LEDGER``, because the runner sets ``path=None`` on the
#: error path while ``run_id`` is still present. "Could not be read" is not
#: "was empty" -- it is a broken measurement, and labelling it an honest
#: absence is the very conflation this vocabulary exists to prevent.
ABSENCE_UNCLASSIFIABLE_LEDGER = "unclassifiable-resolution-ledger"


def resolution_path_absence_reason(
    *,
    run_id: str | None,
    path: ResolutionPath | None,
    classification_failed: bool = False,
) -> str | None:
    """Why this case has no ``resolution_path`` -- or ``None`` if it has one.

    CHAOS-3219 Phase 2 exit, live-falsified. :func:`derive_resolution_path`
    returns ``None`` for situations a receipt reader must be able to tell
    apart, and exit run #3 wrote ``resolution_path: null`` into all 143
    receipts without recording which one applied:

    * the ledger was read and was genuinely empty (honest absence),
    * the runner never obtained a run_id, so nothing was ever read, or
    * the ledger was read, was non-empty, and could not be classified
      (``classification_failed``) -- the Codex MEDIUM finding.

    Collapsing those is what let an entire corpus that measured nothing read
    as a corpus that measured cleanly. This does not change
    ``derive_resolution_path``'s own contract (its ``None`` for an empty
    sequence remains correct and is still the right answer for a zero-mention
    question) -- it records the CONTEXT that ``None`` alone cannot carry.

    ``classification_failed`` is checked BEFORE the empty-ledger fallback:
    only the caller knows the derivation raised, and that fact must not be
    reconstructible from ``path is None`` alone, which is exactly how the two
    got conflated in the first place.
    """

    if path is not None:
        return None
    if run_id is None:
        return ABSENCE_RUN_ID_NOT_OBSERVED
    if classification_failed:
        return ABSENCE_UNCLASSIFIABLE_LEDGER
    return ABSENCE_EMPTY_LEDGER


def absence_is_a_broken_measurement(reason: str | None) -> bool:
    """Whether an absence reason means "this was not honestly measured".

    Deliberately a positive test against the known-broken reasons rather than
    ``reason != ABSENCE_EMPTY_LEDGER``: a future reason then defaults to "not
    broken" only by explicit choice, never by falling through an inequality
    nobody revisited. (That inequality would, by luck, have handled the Codex
    MEDIUM finding correctly -- and would equally have mislabelled the next
    honest reason someone adds. The explicit set is still right.)
    """

    return reason in {ABSENCE_RUN_ID_NOT_OBSERVED, ABSENCE_UNCLASSIFIABLE_LEDGER}
