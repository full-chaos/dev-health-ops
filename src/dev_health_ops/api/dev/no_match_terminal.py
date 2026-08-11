"""The server-owned no-match terminal and the user-visible copy contract.

CHAOS-3367. Two separate things live here, and they answer two different
halves of the Wave 3.1 PRD §12 prohibitions:

* **The no-match terminal** (``named_subject_not_found_answer``). When a run
  resolved a *named* subject to ``forbidden_or_not_found``, the model must not
  be the author of what the user reads. The PRD fixes that sentence verbatim,
  and it is built here from server-owned text plus — at most — a span the user
  themselves typed. A no-match is not a refusal, so this answer is never
  ``AnswerStatus.REFUSED``; it carries the run's own not-found
  ``DevScopeResolution`` (so the resolved-scope row cannot say ``exact`` while
  the summary says a subject was not found) and a zero coverage block (so no
  "1 of 1 sources" line can claim a source plan ran).

* **The internal-token denylist** (``internal_token_leak``). A closed set,
  *derived from the live enum classes themselves* rather than hand-listed, of
  the internal vocabulary tokens that must never appear in a user-visible
  string. ``orchestrator.finish()`` runs it over every terminal it writes.

Why the denylist is derived, and why it keeps only underscore-bearing members:

* Derived, so a member added to ``ScopeResolutionOutcome``/``AnswerStatus``/
  ``RunState``/``DevError.code``/``PublicOutcome``/``ResolutionOutcome`` is
  covered the day it lands. A hand-maintained list is a list that drifts, and
  the drift is invisible until a leak reaches a customer.
* Underscore-bearing only, because that is what makes the check
  false-positive-free by construction: ``exact``, ``failed`` and ``denied``
  are ordinary English that safe prose may legitimately contain, while
  ``forbidden_or_not_found`` and ``scope_forbidden`` cannot occur in written
  English at all. The tokens the PRD names by hand are both in the
  underscore-bearing set, and ``assert_denylist_covers_prd_tokens`` pins that
  at import time so a future enum edit cannot quietly drop either one.

The subject label rule is the other structural property worth stating up
front: **only text the user typed is ever echoed back**. The resolve-scope
query is model-authored, so echoing it verbatim would reopen exactly the
producer-authored-copy channel that ``contracts_v2.no_answer_policy``'s
canonical tables closed. ``user_supplied_subject_label`` therefore returns the
*question's own* span, and only when the model's query occurs in the question
verbatim; otherwise the copy simply omits the name.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import get_args

from .answer_validator import answer_has_material_grounding
from .contracts import (
    AnswerStatus,
    DevActualCompletion,
    DevAnswer,
    DevContractVersions,
    DevCoverage,
    DevError,
    DevModelMetadata,
    DevScopeResolution,
    ScopeResolutionOutcome,
)
from .contracts_v2 import PublicOutcome, ResolutionOutcome
from .contracts_v2.base import QuestionIntentID
from .graph_investigation_query import CohortDiscoveryFamily, GraphQueryOutcome
from .investigation_contract import ComparisonShape, PacketLimitationKind
from .orchestrator_states import RunState
from .status_change_service import STATUS_REASON_CODES

__all__ = [
    "INTERNAL_TOKEN_DENYLIST",
    "NEVER_ATTESTABLE_TOKENS",
    "REFUSED_WITH_GROUNDING_SUMMARY",
    "SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE",
    "SUBJECT_MATCHED_BY_UNDERSTANDING_TEMPLATE",
    "WITHHELD_COPY",
    "attested_strings",
    "disclose_scope_widening",
    "disclose_subject_match",
    "internal_token_leak",
    "internal_token_leak_field",
    "named_subject_not_found_answer",
    "no_match_summary",
    "redact_persisted_answer",
    "redact_persisted_error",
    "scrub_auxiliary_leaks",
    "subject_matched_disclosure",
    "user_supplied_subject_kind",
    "user_supplied_subject_label",
    "user_visible_strings",
    "user_visible_strings_by_field",
]


def _underscore_members(values: Iterable[str]) -> frozenset[str]:
    return frozenset(value for value in values if "_" in value)


#: CHAOS-3377 defect 2: the CHAOS-3367 denylist covered only scope-resolution
#: and run/outcome vocabulary -- a live run showed the §10 completion
#: assessment's own internal tokens (``not_ready``, ``open_blocker``,
#: ``required_child_incomplete``, ...) and evidence-handle ids (``ev1_...``)
#: leaking into user-visible prose instead. Widened here rather than only in
#: ``status_answer_render.py``'s translation tables, because those tables
#: only cover the module's OWN server-rendered copy; this denylist is the
#: fail-closed backstop ``orchestrator.finish()`` runs over EVERY terminal
#: (including any model-authored prose that still reaches a field this
#: module scans), so a leak survives here even if a future producer forgets
#: to route through the deterministic renderer at all.
#:
#: ``STATUS_REASON_CODES`` is ``status_change_service``'s own derived,
#: pinned-total set (see that module), so this cannot silently fall behind a
#: reason code added there. ``DevActualCompletion.state`` is a 3-member
#: ``Literal`` on the wire, so its members are pulled the same way
#: ``DevError.code`` already is below.
_EXTRA_INTERNAL_TOKENS: frozenset[str] = frozenset({"actual_completion", "ev1_"})

#: CHAOS-3698. The graph-assisted routing seam (CHAOS-3502/3650) makes a new
#: family of internal vocabulary reachable in user-visible prose for the
#: first time: once ``orchestrator._graph_grounded_answer`` starts building
#: real answers from graph packets, ``discovered_cohort``,
#: ``deadline_exceeded``, ``provider_failure``, ``team_pressure``,
#: ``project_capacity`` and every ``PacketLimitationKind`` member become
#: values a producer could echo, exactly the CHAOS-3367/3377 leak class,
#: reopened against this vocabulary. #1681 (ops main) registered
#: ``GraphQueryOutcome``/``CohortDiscoveryFamily`` here already but silently
#: dropped ``PacketLimitationKind`` and the ``discovered_cohort`` intent/
#: comparison-shape token -- this closes both gaps rather than leaving a
#: partial union that looks complete.
#:
#: Only ``QuestionIntentID.DISCOVERED_COHORT``/``ComparisonShape.
#: DISCOVERED_COHORT`` are pulled from their enums, not the enums wholesale:
#: those two enums carry members well beyond the graph route (every Wave 3.1
#: launch intent, every comparison shape), and widening the union to all of
#: them is a broader change than this leak class needs -- CHAOS-3693 (filed,
#: unstarted) is the deliberate follow-up for "every StrEnum is covered-or-
#: excluded" as a structural guard, not this fix.
#:
#: Deliberately NOT unioned, with the reason stated here rather than left
#: silent (the exact omission that caused this issue):
#: * ``graph_arm.query_service.GraphMechanism`` -- "Trial metadata only --
#:   never reaches the wire" per its own docstring; it has no path to any
#:   producer-authored or model-authored string this scan runs over.
#: * ``evidence_service.EvidenceAvailability`` -- a real, pre-existing
#:   leak-shaped vocabulary (``no_matches``, ``unauthorized``,
#:   ``unconfigured``, ...), but not part of CHAOS-3698's reported instance
#:   and not newly made reachable by this leg; tracked as a candidate for
#:   the CHAOS-3693 structural guard rather than folded into this fix.
_GRAPH_ASSISTED_INTERNAL_TOKENS: frozenset[str] = (
    _underscore_members(member.value for member in GraphQueryOutcome)
    | _underscore_members(member.value for member in CohortDiscoveryFamily)
    | _underscore_members(member.value for member in PacketLimitationKind)
    | _underscore_members(
        (
            QuestionIntentID.DISCOVERED_COHORT.value,
            ComparisonShape.DISCOVERED_COHORT.value,
        )
    )
)

#: Every internal vocabulary token that must never reach a user-visible
#: string, derived from the live enums. See the module docstring for why only
#: underscore-bearing members are kept.
INTERNAL_TOKEN_DENYLIST: frozenset[str] = (
    _underscore_members(member.value for member in ScopeResolutionOutcome)
    | _underscore_members(member.value for member in AnswerStatus)
    | _underscore_members(member.value for member in RunState)
    | _underscore_members(member.value for member in PublicOutcome)
    | _underscore_members(member.value for member in ResolutionOutcome)
    | _underscore_members(get_args(DevError.model_fields["code"].annotation))
    | _underscore_members(STATUS_REASON_CODES)
    | _underscore_members(
        get_args(DevActualCompletion.model_fields["state"].annotation)
    )
    # CHAOS-3660 §8(h). Both freshly reserved on `main` by the graph-routing
    # wave's contract package -- see their own docstrings in `contracts.py`.
    # Registered here at the same commit that introduces them (not left for
    # a later PR to remember), same discipline this comment already
    # describes for every other source above. `GraphAssistedAvailability`
    # is deliberately NOT unioned in: none of its 6 members contain an
    # underscore (`_underscore_members` would drop them all), and its
    # values (`enabled`, `stale`, ...) are ordinary English words legitimate
    # prose may use -- the same reasoning the module docstring gives for
    # keeping this denylist underscore-only in the first place.
    | _underscore_members(member.value for member in CohortDiscoveryFamily)
    | _underscore_members(member.value for member in PacketLimitationKind)
    | _EXTRA_INTERNAL_TOKENS
    | _GRAPH_ASSISTED_INTERNAL_TOKENS
)

# NOT included, deliberately: ``ToolID``. A round-2 review argued that a
# model-authored "resolve_scope.v1 returned no matches" is the same class of
# leak, and adding it was tried. It is not: tool ids are a *disclosed*
# vocabulary, published in the tool contract and already named in
# server-authored copy on purpose -- ``scripted_openai_service`` emits the
# warning "Provider health was measured through data_health.v1", which is
# provenance a reader benefits from, and the acceptance oracle asserts it.
# Denying tool ids failed that healthy run. §12's prohibition is about Ask
# Dev's own internal STATE (which outcome the resolver reached, which error
# code was chosen), not about which tool ran.

#: Tokens no provenance may ever exempt.
#:
#: The ``attested`` mechanism exists so an authorized entity genuinely called
#: ``not_found`` does not fail its own answer. Left unbounded, it also became a
#: hole: an evidence label named ``scope_forbidden`` would exempt a genuinely
#: leaked ``scope_forbidden`` anywhere else in the same answer (codex
#: adversarial review, round 2 MEDIUM).
#:
#: These are the tokens that describe Ask Dev's own scope-resolution decision.
#: An entity cannot plausibly be named after one, and they are exactly what §12
#: prohibits by name -- so the escape hatch does not apply to them at all, and
#: no reviewer has to reason about whether some label earned it.
NEVER_ATTESTABLE_TOKENS: frozenset[str] = _underscore_members(
    member.value for member in ScopeResolutionOutcome
) | frozenset({"scope_forbidden", "scope_not_found", "scope_ambiguous"})

#: The two tokens Wave 3.1 §12 prohibits by name. Pinned at import time
#: against the derived set: if an enum is renamed or re-homed such that
#: either stops being derivable, this raises here rather than silently
#: shipping a denylist that no longer catches the reported defect.
_PRD_NAMED_TOKENS = frozenset({"forbidden_or_not_found", "scope_forbidden"})


def assert_denylist_covers_prd_tokens() -> None:
    missing = sorted(_PRD_NAMED_TOKENS - INTERNAL_TOKEN_DENYLIST)
    if missing:  # pragma: no cover - import-time totality guard
        raise RuntimeError(
            f"internal-token denylist no longer derives the PRD-named tokens: {missing}"
        )


assert_denylist_covers_prd_tokens()


# KNOWN DIVERGENCE, stated rather than papered over (codex adversarial review,
# round 1 MEDIUM). TRD §7.1 maps this internal outcome to the `not_found`
# public class, and the web renderer labels the row "No authorized match
# found". The v2 FRAME this terminal produces does NOT carry `not_found`:
# `terminal_frames.wrap_legacy_answer_as_frame` stamps every legacy answer
# `answered_with_gaps`, and switching it here is not a one-line change --
# `not_found` is one of `NO_ANSWER_OUTCOMES`, so
# `validate_no_answer_projection` would require the frame's `direct_answer` to
# equal `CANONICAL_NO_ANSWER_COPY["not_found"]` exactly and carry no content,
# which is precisely NOT the PRD's subject-naming sentence.
#
# Reconciling the two needs the canonical no-answer copy policy to admit a
# subject-naming variant, which is a v2 wire decision beyond this fix. No
# client reads a frame today (streaming sends `result.answer`; router replay
# prefers the stored v1 answer), so the divergence is invisible to users --
# but it IS recorded in `dev_runs.public_outcome`, so an operator reading that
# column sees `answered_with_gaps` for a no-match. Filed as follow-up.
#
# An earlier revision of this module exported NO_MATCH_PUBLIC_OUTCOME /
# NO_MATCH_PUBLIC_OUTCOME_LABEL constants here. They were removed: nothing in
# production read them, so the only thing asserting the mapping was a test
# asserting the constants against themselves -- a claim of coverage where
# there was none. The label lives where it is actually rendered (web's
# SCOPE_OUTCOME_LABELS).


# The PRD fixes this wording verbatim. Split into its three sentences so the
# subject-naming half can vary without the two invariant halves ("I did not
# substitute organization-wide data", the closest-matches offer) ever being
# reworded by accident.
#: Straight ASCII quotes around the label, deliberately: the PRD writes the
#: sentence that way, and an acceptance check that greps for the literal
#: string must not be defeated by a typographic substitution.
_NAMED_SUBJECT_SENTENCE = (
    "I couldn't find an authorized {kind} named '{label}' in the selected organization."
)
_UNNAMED_SUBJECT_SENTENCE = (
    "I couldn't find an authorized match for the subject named in this "
    "question in the selected organization."
)
_NO_SUBSTITUTION_SENTENCE = "I did not substitute organization-wide data."
_CLOSEST_MATCHES_SENTENCE = "Here are the closest matches, if any."

#: CHAOS-3497 part 2: the one sentence that discloses a widening in prose.
#:
#: When a bare name in the question cannot be resolved, the run does not stop
#: -- it widens to organization scope and answers anyway (see
#: ``subject_preflight``'s ``unresolved_untyped`` branch and its comment for
#: why). That widening was recorded machine-readably
#: (``dev_scope_resolution.v1.fallbacks == ["organization"]``, outcome
#: ``organization_fallback``, no ``subject_ref`` on the frame) but said
#: nowhere a person reading the rendered answer would see it: measured live
#: 2026-08-06, ``resolved_scope.warnings`` was empty and the frame's
#: ``limitations`` carried only unrelated provenance entries. A reader saw
#: "answered with gaps" and no indication that the thing they named had been
#: missed.
#:
#: Deliberately NOT keyed on ``fallbacks == ["organization"]``, which the
#: ticket suggested. That marker has a second producer:
#: ``scope_service.resolve`` sets it for a request that named no subject at
#: all and was ALLOWED to default to the organization -- not a widening away
#: from anything, and a reader must not be told their subject was missed when
#: they never named one. Measured, that second producer is unreachable from
#: Ask Dev today (``production_runtime._scope_request`` passes
#: ``allow_organization_fallback=False``), so the two predicates currently
#: agree -- ``test_chaos_3497_scope_observability`` pins exactly that, so this
#: is a checked claim rather than a story, and the day the flag flips the copy
#: does not start lying.
#:
#: The trigger is instead the preflight's own ``legacy_guard_required``: true
#: on exactly the branch where a NAMED bare subject went unresolved and the
#: organization was committed in its place.
#:
#: Names nothing the user typed: the unresolved span is available
#: (``SubjectPreflightResult.unresolved_name_spans``) but echoing it here
#: would reopen the producer-authored-copy channel this module's docstring
#: closes, and the sentence is true and actionable without it.
SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE = (
    "I could not match the subject named in this question, so this answer "
    "covers the whole organization instead."
)


#: ``DevAnswer.warnings``' own contract bound, read off the model so this
#: cannot drift from it.
_MAX_ANSWER_WARNINGS: int = next(
    metadata.max_length
    for metadata in DevAnswer.model_fields["warnings"].metadata
    if getattr(metadata, "max_length", None) is not None
)


def _disclosed(answer: DevAnswer, sentence: str) -> DevAnswer:
    """Put one server-owned ``sentence`` into ``answer.warnings``, once.

    The single implementation of "a disclosure survives the bound", shared by
    every disclosure below so the rule is decided once rather than per
    caller. CHAOS-3531: the two callers used to disagree here -- one yielded
    at the bound and one took the slot -- which is a trap for the next reader
    and made "never silent" true of one disclosure and false of its twin.

    ``warnings`` is the right channel rather than a bespoke field: it is what
    ``streaming`` publishes as ``warning`` frames and what
    ``terminal_frames.wrap_legacy_answer_as_frame`` copies into the frame's
    ``limitations``, so one entry reaches the wire and the rendered answer
    together. ``answered_with_gaps`` (what every wrapped legacy answer is)
    requires disclosed limitations, so adding one can never invalidate the
    frame.

    At the contract's twenty-warning bound the disclosure DISPLACES the last
    producer warning rather than yielding to it. That trade is deliberate and
    it is the whole point of this function: an undisclosed scope decision is
    a claim the reader cannot check, and a dropped twentieth warning is not.
    An answer carrying twenty warnings is already degenerate; the disclosure
    is the one entry whose absence changes what the answer MEANS.

    That judgement rests on a checked property of today's producers, stated
    so a future change can notice it: every server-authored writer of
    ``warnings`` emits at MOST one entry (the server-grounded notice, the
    budget-exhaustion notice, these disclosures), so the only way to reach
    the bound is model-authored free text -- displacement evicts model prose,
    never a server safety signal. If a deterministic producer ever starts
    emitting many machine warnings here, revisit this trade rather than
    assuming it still holds.

    The sentence goes FIRST so a truncating renderer keeps it, and the
    function is idempotent -- disclosing twice costs one slot, not two.
    """

    if sentence in answer.warnings:
        return answer
    return answer.model_copy(
        update={"warnings": [sentence, *answer.warnings][:_MAX_ANSWER_WARNINGS]}
    )


def disclose_scope_widening(answer: DevAnswer) -> DevAnswer:
    """Disclose that the run widened to organization scope, once.

    CHAOS-3531 corrects what CHAOS-3497 claimed: this used to return the
    answer unchanged once the twenty-warning bound was spent, so a widened
    run could answer organization-wide with no prose disclosure at all --
    while CHAOS-3497's own write-up said the widening "is said out loud"
    without qualification. The claim is now true rather than qualified: see
    ``_disclosed`` for the bound rule and why displacement is the right
    trade.
    """

    return _disclosed(answer, SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE)


#: CHAOS-3525: the sentence that makes a model-chosen subject visible.
#:
#: Promotion is the first time an LLM decides WHAT a question is about, and a
#: subject chosen this way must never be indistinguishable from one the
#: catalog matched literally. The named span is the user's own text and the
#: label is a catalog-confirmed authorized entity -- the two things the
#: reader needs to check the match themselves, and nothing else.
#:
#: Named "matched" rather than "found": the run did not find a literal match,
#: it interpreted. Overstating that would be the same class of error as the
#: silent commit this sentence exists to prevent.
SUBJECT_MATCHED_BY_UNDERSTANDING_TEMPLATE = (
    "I matched '{span}' to {label}. If that is not what you meant, ask again "
    "using the full name."
)


def subject_matched_disclosure(*, span: str, label: str) -> str:
    """The disclosure sentence for one QUA-committed subject."""

    return SUBJECT_MATCHED_BY_UNDERSTANDING_TEMPLATE.format(span=span, label=label)


def disclose_subject_match(answer: DevAnswer, *, span: str, label: str) -> DevAnswer:
    """Disclose which subject a QUA proposal committed, once.

    The label is authorized catalog content, so a legitimate entity whose
    name happens to contain a denylisted token must not fail its own answer:
    the caller attests it through ``finish()``'s ``extra_attested`` seam, the
    same trust tier already granted to clarification-candidate labels.

    Bound behaviour is ``_disclosed``'s, shared with the widening disclosure
    -- a model-chosen subject reaching a reader unannounced is the failure
    the whole promotion path is gated to prevent, and it must not become
    reachable just because an answer already carried twenty warnings.
    """

    return _disclosed(answer, subject_matched_disclosure(span=span, label=label))


#: The noun used when the question gave no entity noun of its own. Never
#: "project": guessing a kind the search never confirmed would state
#: something the server does not know (the model-facing search spans every
#: kind in ``scope_service.MODEL_SEARCHABLE_ENTITY_KINDS`` at once, so a
#: no-match result carries no kind at all).
_DEFAULT_SUBJECT_KIND = "subject"

#: The entity nouns the question may supply, normalized to the word the copy
#: renders. Deliberately the same vocabulary ``orchestrator._ENTITY_NOUN_
#: PATTERN`` recognizes, so the two cannot disagree about what counts as a
#: named-entity noun.
_SUBJECT_KIND_BY_NOUN: Mapping[str, str] = {
    "project": "project",
    "repository": "repository",
    "repo": "repository",
    "team": "team",
    "issue": "issue",
    "pull request": "pull request",
    "deployment": "deployment",
    "incident": "incident",
    "work unit": "work unit",
}

#: How long a user-typed span may be before the copy drops it. A subject
#: label is a name, not a sentence; a long span is a sign the match was
#: incidental rather than a real name, and the answer contract bounds
#: ``direct_summary`` regardless.
_MAX_SUBJECT_LABEL_CHARS = 120


def internal_token_leak_field(
    values: Iterable[tuple[str, str | None]], *, attested: Iterable[str | None] = ()
) -> tuple[str, str] | None:
    """Like ``internal_token_leak``, but over ``(field, value)`` pairs and
    returning ``(field, token)`` -- the field-labeled counterpart used at the
    write-time boundary (``orchestrator.finish()``) so a leak's log line can
    name WHERE the token was found, not only what it was.

    CHAOS-3377 leak-hardening: a prior incident's log line carried only the
    literal string ``"ask_dev.orchestrator.internal_token_leak"`` with no
    detail of which field leaked -- this dev stack runs with ``LOG_JSON=0``,
    under which a bare-message ``logging.StreamHandler`` silently drops
    anything passed via ``extra=``, so the token/run_id/field were captured
    by the log call but never actually reached the log line. Returning the
    field here lets the caller embed it directly in the message string,
    which survives regardless of formatter configuration.

    See ``internal_token_leak`` for the matching/attestation semantics this
    shares in full; this is the single implementation, with
    ``internal_token_leak`` now a thin field-blind wrapper around it.
    """

    attested_text = " ".join(text.casefold() for text in attested if text)
    for field, value in values:
        if not value:
            continue
        lowered = value.casefold()
        for token in sorted(INTERNAL_TOKEN_DENYLIST):
            if token not in lowered:
                continue
            if token in NEVER_ATTESTABLE_TOKENS or token not in attested_text:
                return field, token
    return None


def internal_token_leak(
    values: Iterable[str | None], *, attested: Iterable[str | None] = ()
) -> str | None:
    """The first denylisted internal token found in ``values``, or ``None``.

    Case-insensitive and substring-based: the reported defect rendered
    ``forbidden_or_not_found`` inside a longer sentence, so an equality check
    against the whole field would not have seen it.

    ``attested`` is the provenance escape hatch, and the reason this is not a
    naive substring scan. Some denylisted tokens are also plausible real names
    -- an authorized repository can genuinely be called ``not_found``, and a
    claim can genuinely mention ``app/not_found.tsx``. Without provenance the
    fail-closed check in ``orchestrator.finish()`` would destroy that healthy
    answer (codex adversarial review, round 1 MEDIUM, with a working repro).

    So a token is a leak only when NOTHING in ``attested`` -- the user's own
    question and the authorized entity labels this very answer already carries
    -- contains it. That is the difference between "the model narrated an
    internal enum" and "the run is about a thing whose name looks like one":
    the second has a server-authorized source for the string, the first does
    not. It is a per-token exemption rather than a per-string one, so a
    sentence that mixes an attested name with a genuinely leaked token still
    fails on the leaked one.
    """

    found = internal_token_leak_field(
        ((str(index), value) for index, value in enumerate(values)),
        attested=attested,
    )
    return found[1] if found is not None else None


def attested_strings(
    answer: DevAnswer | None, question: str | None = None
) -> tuple[str, ...]:
    """Every string with a server-authorized or user-typed provenance.

    The authorized entity labels are read off the answer's OWN scope and
    evidence, never off a catalog lookup: an entity that is not already part
    of this answer has no business exempting a token in it.
    """

    texts: list[str] = [question or ""]
    if answer is not None:
        for scope in (
            answer.resolved_scope.requested_scope,
            answer.resolved_scope.resolved_scope,
        ):
            if scope is None:
                continue
            texts.extend(ref.display_label for ref in scope.entity_refs)
            texts.extend(scope.repositories)
        for candidate in answer.resolved_scope.candidates:
            texts.append(candidate.entity_ref.display_label)
        texts.extend(item.display_label for item in answer.evidence)
        texts.extend(metric.label for metric in answer.metrics)
    return tuple(text for text in texts if text)


def user_visible_strings_by_field(
    *, answer: DevAnswer | None = None, error: DevError | None = None
) -> tuple[tuple[str, str], ...]:
    """``user_visible_strings``, paired with the field each string came from.

    CHAOS-3377 leak-hardening (operability): a leak log line naming only the
    bare token forces an operator to grep the whole answer shape by hand to
    find which field carried it; naming the field turns that into a one-line
    diagnosis. List-valued fields are indexed (``warnings[0]``) so a specific
    offending entry, not just its field name, is identifiable. This is the
    single source of the string content ``user_visible_strings`` returns --
    see that function for why each field is included.
    """

    pairs: list[tuple[str, str]] = []
    if answer is not None:
        pairs.append(("direct_summary", answer.direct_summary))
        pairs.extend(
            (f"warnings[{i}]", value) for i, value in enumerate(answer.warnings)
        )
        pairs.extend(
            (f"claims[{i}].text", claim.text) for i, claim in enumerate(answer.claims)
        )
        pairs.extend(
            (f"conflicts[{i}].summary", conflict.summary)
            for i, conflict in enumerate(answer.conflicts)
        )
        pairs.extend(
            (f"suggested_follow_up_questions[{i}]", value)
            for i, value in enumerate(answer.suggested_follow_up_questions)
        )
        pairs.extend(
            (f"resolved_scope.warnings[{i}]", value)
            for i, value in enumerate(answer.resolved_scope.warnings)
        )
    if error is not None:
        pairs.append(("safe_message", error.safe_message))
        pairs.extend(
            (f"remediation[{i}]", value) for i, value in enumerate(error.remediation)
        )
    return tuple(pairs)


def user_visible_strings(
    *, answer: DevAnswer | None = None, error: DevError | None = None
) -> tuple[str, ...]:
    """Every prose string a client renders as copy, from one terminal payload.

    Deliberately prose only. Evidence display labels and citation text are
    data-derived (a file path, a branch name, a commit subject) and can
    legitimately contain underscores, so including them would make the check
    fire on real content rather than on leaked vocabulary. Those fields are
    already governed by the evidence contract's own safe-excerpt handling.
    """

    return tuple(
        text for _, text in user_visible_strings_by_field(answer=answer, error=error)
    )


def user_supplied_subject_label(question: str, query: str | None) -> str | None:
    """The question's own span for ``query``, or ``None``.

    ``query`` is model-authored (the model composes the ``resolve_scope.v1``
    query), so it is used only as a *lookup key* into the user's own text,
    never echoed itself. The returned span is sliced out of ``question``, so
    every character in it was typed by the person asking.
    """

    if not query:
        return None
    needle = " ".join(query.split()).strip()
    if not needle or len(needle) > _MAX_SUBJECT_LABEL_CHARS:
        return None
    # Word-boundary and UNIQUE, not a bare substring search. A bare search let
    # the model claim a name the user never wrote: for the question "What is
    # the status of project Falconary?" a model-authored query of "Falcon"
    # matched inside "Falconary", and the server then stated 'I couldn't find
    # an authorized project named "Falcon"' (codex adversarial review, round 1
    # MEDIUM). Requiring a whole-word match kills that; requiring the match to
    # be unique keeps the noun lookup below unambiguous, since with two
    # occurrences there is no single neighbourhood to read a kind from.
    pattern = re.compile(rf"(?<!\w){re.escape(needle)}(?!\w)", re.IGNORECASE)
    found = pattern.findall(question)
    if len(found) != 1:
        return None
    return found[0]


def user_supplied_subject_kind(question: str, label: str | None) -> str:
    """The entity noun the user wrote next to ``label``, or ``"subject"``.

    Both orderings the question may use ("the Falcon project", "project
    Falcon"), matched against the same noun vocabulary the orchestrator's
    named-entity backstop recognizes. Anything else falls back to the neutral
    noun rather than guessing a kind the search never confirmed.
    """

    if not label:
        return _DEFAULT_SUBJECT_KIND
    nouns = "|".join(
        re.escape(noun) for noun in sorted(_SUBJECT_KIND_BY_NOUN, key=len, reverse=True)
    )
    # Word-boundary on both sides of the label for the same reason
    # `user_supplied_subject_label` needs it: "Falcon project" must not be
    # matched by a label of "Falcon" sitting inside "Falconary project".
    escaped = rf"(?<!\w){re.escape(label)}(?!\w)"
    trailing = re.compile(rf"{escaped}(?:'s)?\s+({nouns})\b", re.IGNORECASE)
    leading = re.compile(rf"\b({nouns})\s+{escaped}", re.IGNORECASE)
    matched = [
        found.group(1)
        for pattern in (leading, trailing)
        if (found := pattern.search(question))
    ]
    # Two different nouns around the same name ("the Falcon repository in the
    # Falcon project") is the caller telling us two things at once. Asserting
    # either would state a kind the server never confirmed, so state neither.
    kinds = {
        _SUBJECT_KIND_BY_NOUN[" ".join(noun.split()).casefold()] for noun in matched
    }
    return kinds.pop() if len(kinds) == 1 else _DEFAULT_SUBJECT_KIND


def no_match_summary(question: str, query: str | None) -> str:
    """The PRD's verbatim no-match sentence for one named subject."""

    label = user_supplied_subject_label(question, query)
    if label is None:
        opening = _UNNAMED_SUBJECT_SENTENCE
    else:
        opening = _NAMED_SUBJECT_SENTENCE.format(
            kind=user_supplied_subject_kind(question, label), label=label
        )
    return " ".join((opening, _NO_SUBSTITUTION_SENTENCE, _CLOSEST_MATCHES_SENTENCE))


def named_subject_not_found_answer(
    *,
    answer_id: str,
    conversation_id: str,
    question: str,
    query: str | None,
    resolution: DevScopeResolution,
    versions: DevContractVersions,
    model: DevModelMetadata,
    now: datetime,
) -> DevAnswer:
    """The server-owned v1 answer a named-subject no-match terminates with.

    ``resolution`` must be the run's own ``forbidden_or_not_found``
    resolution, not the previously committed scope: rendering the committed
    organization scope here is precisely the "Scope outcome: exact while a
    named subject could not be found" juxtaposition §12 prohibits. Its
    ``candidates`` list is passed through untouched — empty today, filled by
    CHAOS-3366 — so that work is additive rather than a second contract
    change.

    ``warnings`` is empty by construction. §12 prohibits an
    authorization-shaped warning unless access was actually denied, and a
    no-match cannot distinguish the two (the backend collapses forbidden and
    not-found into one outcome so scope resolution cannot enumerate what
    exists). The summary states what happened; nothing else needs to.
    """

    if resolution.outcome is not ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND:
        raise ValueError(
            "the no-match terminal requires the run's own forbidden_or_not_found "
            f"resolution, got {resolution.outcome.value!r}"
        )
    return DevAnswer(
        schema_version="dev_answer.v1",
        answer_id=answer_id,
        conversation_id=conversation_id,
        generated_at=now,
        resolved_scope=resolution,
        as_of=now,
        # Never REFUSED: §12 prohibits labelling a no-match as a refusal, and
        # it would be untrue -- Ask Dev did not decline to answer, it looked
        # and found nothing it is allowed to report on.
        status=AnswerStatus.INSUFFICIENT_EVIDENCE,
        direct_summary=no_match_summary(question, query),
        claims=[],
        metrics=[],
        evidence=[],
        conflicts=[],
        # Zero required and zero available: no source plan ran for a subject
        # that was never resolved, and "1 of 1 sources" for that run is the
        # third §12 prohibition. The web renderer hides the coverage line
        # entirely when nothing was required.
        coverage=DevCoverage(
            required_source_count=0,
            available_source_count=0,
            unavailable_required_sources=[],
            stale_required_sources=[],
            degraded_required_sources=[],
            as_of=now,
        ),
        warnings=[],
        suggested_follow_up_questions=[],
        versions=versions,
        model=model,
    )


#: What one leaked prose field is replaced with on a READ of persisted data.
#: A neutral statement, not a redaction of the token alone: a sentence built
#: around a leaked enum does not become true when the enum is deleted from it
#: ("Scope resolution returned ." is worse than saying nothing), and it must
#: not read as a claim the server made.
WITHHELD_COPY = "This part of the answer could not be shown."

#: CHAOS-3377 HIGH (codex adversarial web review, round 2): the read-time
#: counterpart of ``answer_validator``'s "a refused answer cannot carry
#: material grounding" write-time check. A row persisted before that check
#: existed can still have ``status=refused`` alongside real claim/metric/
#: evidence content. The web client's own defense-in-depth backstop
#: (``AskDevAnswer.refusedDespiteMaterialGrounding``) was found relabeling
#: that contradiction "Answered" while STILL rendering the model's original
#: (rejected-shaped) prose underneath -- exactly the leak this server-side
#: normalization now closes upstream, so the client never receives it in
#: the first place. A neutral statement, not a redaction of the model's
#: words: the original claim/direct_summary text is discarded entirely,
#: never partially edited into something that could still read as a claim
#: the server verified.
REFUSED_WITH_GROUNDING_SUMMARY = (
    "This answer's recorded status could not be reconciled with its own "
    "content. The original narrative has been withheld; the structured "
    "metrics and evidence below are unaffected."
)


def _normalize_refused_with_grounding(answer: DevAnswer) -> DevAnswer:
    """The read-time mirror of ``answer_validator``'s "refused answer cannot
    carry material grounding" check (CHAOS-3377 HIGH, web adversarial
    review round 2).

    A NEW run can no longer persist this contradiction (the write-time
    check rejects it, repairable, and demotes through
    ``Orchestrator._server_grounded_answer`` if the model still won't
    self-correct -- see ``orchestrator.py``). A row written before that
    check existed can still have it. This is the single read-time seam
    both ``router.py`` call sites that hand a persisted answer back to a
    client already go through (``redact_persisted_answer``, below) --
    exactly one place, mirroring the write path's single seam
    (``Orchestrator._deterministic_status_render``) rather than requiring
    every future replay call site to remember to check.

    The model's own ``claims``/``direct_summary`` are discarded outright --
    the same "narrative loses when it disagrees with the frame" rule the
    write-time demotion already applies -- while ``metrics``/``evidence``
    (server-issued, unaffected by the contradiction) are kept.
    ``status`` is recomputed from the answer's own persisted ``coverage``
    (the same fully-covered test ``deterministic_answer_status`` uses),
    never left as the self-contradicted ``REFUSED``.
    """

    if answer.status is not AnswerStatus.REFUSED or not answer_has_material_grounding(
        answer
    ):
        return answer
    coverage = answer.coverage
    fully_covered = (
        coverage.available_source_count == coverage.required_source_count
        and not coverage.unavailable_required_sources
        and not coverage.stale_required_sources
        and not coverage.degraded_required_sources
    )
    return answer.model_copy(
        update={
            "status": AnswerStatus.COMPLETE if fully_covered else AnswerStatus.PARTIAL,
            "direct_summary": REFUSED_WITH_GROUNDING_SUMMARY,
            "claims": [],
        }
    )


def _clean_or_withhold(value: str, *, attested: Iterable[str | None]) -> str:
    """``value``, or ``WITHHELD_COPY`` if it carries a leaked token.

    The single per-string cleaning rule both ``redact_persisted_answer``
    (read-time, every prose field) and ``scrub_auxiliary_leaks`` (write-time,
    model-authored auxiliary fields only) apply, so the two seams cannot
    silently diverge on what "cleaned" means for one string.
    """

    return (
        value
        if internal_token_leak([value], attested=attested) is None
        else WITHHELD_COPY
    )


def scrub_auxiliary_leaks(
    answer: DevAnswer, *, attested: Iterable[str | None] = ()
) -> tuple[DevAnswer, tuple[str, ...]]:
    """Remove a leaked token found ONLY in a model-authored auxiliary field,
    never in ``direct_summary``/``claims``.

    CHAOS-3377 leak-hardening: ``Orchestrator._deterministic_status_render``
    overwrites ``status``/``direct_summary``/``claims`` with server-rendered
    content once a bound ``status_snapshot.v1`` result exists, but
    ``warnings``, ``conflicts[].summary``, ``suggested_follow_up_questions``,
    and ``resolved_scope.warnings`` stay fully model-authored -- the model has
    seen the raw tool-result JSON (``actual_completion.reason_codes`` and
    friends) and can echo one of those tokens into any of the four, none of
    which the deterministic override ever touches. ``orchestrator.finish()``'s
    fail-closed scan then destroys the ENTIRE terminal -- including the safe
    deterministic core -- over one leaked auxiliary sentence (a live incident:
    run ``22f97bee-0b8b-44a5-979f-78d7d7a80a82``).

    This runs at the SAME write-time seam, immediately before that scan, and
    is deliberately narrower than it: only these four fields are eligible for
    scrubbing. ``direct_summary``/``claims`` are the answer's load-bearing
    deterministic content (or, absent a bound status_snapshot, the model's
    only substantive content) -- a leak reaching either of those is a
    genuine defect the whole-terminal fail-closed check in
    ``orchestrator.finish()`` must still catch, exactly as before this
    function existed. Scrubbing them here would silently rescue that case
    instead of surfacing it.

    Mirrors ``redact_persisted_answer``'s per-field treatment exactly
    (``warnings``/``conflicts``/``resolved_scope.warnings`` replaced with
    ``WITHHELD_COPY``, individual ``suggested_follow_up_questions`` entries
    dropped outright) via the shared ``_clean_or_withhold`` helper, so the
    write-time and read-time seams cannot describe "cleaned" two different
    ways. Returns the (possibly updated) answer and the field labels that
    were touched, empty if none were, so the caller can log what happened
    without re-deriving it.
    """

    attested_tuple = tuple(attested)
    touched: list[str] = []

    def field_leaked(field: str, value: str) -> bool:
        if internal_token_leak([value], attested=attested_tuple) is None:
            return False
        touched.append(field)
        return True

    warnings = [
        _clean_or_withhold(value, attested=attested_tuple)
        if field_leaked(f"warnings[{i}]", value)
        else value
        for i, value in enumerate(answer.warnings)
    ]
    conflicts = [
        conflict.model_copy(
            update={
                "summary": _clean_or_withhold(conflict.summary, attested=attested_tuple)
            }
        )
        if field_leaked(f"conflicts[{i}].summary", conflict.summary)
        else conflict
        for i, conflict in enumerate(answer.conflicts)
    ]
    follow_ups = [
        value
        for i, value in enumerate(answer.suggested_follow_up_questions)
        if not field_leaked(f"suggested_follow_up_questions[{i}]", value)
    ]
    scope_warnings = [
        _clean_or_withhold(value, attested=attested_tuple)
        if field_leaked(f"resolved_scope.warnings[{i}]", value)
        else value
        for i, value in enumerate(answer.resolved_scope.warnings)
    ]
    if not touched:
        return answer, ()
    updated = answer.model_copy(
        update={
            "warnings": warnings,
            "conflicts": conflicts,
            "suggested_follow_up_questions": follow_ups,
            "resolved_scope": answer.resolved_scope.model_copy(
                update={"warnings": scope_warnings}
            ),
        }
    )
    return updated, tuple(touched)


def redact_persisted_answer(
    answer: DevAnswer, *, question: str | None = None
) -> DevAnswer:
    """One stored answer with any leaked prose field replaced.

    ``orchestrator.finish()`` is a WRITE-time boundary: it cannot reach rows
    written before it existed, and the reported live defect is already one of
    those rows. Idempotent replay and transcript reads hand a persisted
    ``answer_payload`` straight back to the client, so without this the exact
    payload from the screenshot keeps rendering on every reload (codex
    adversarial review, round 1 HIGH).

    Read-time behaviour deliberately differs from write-time. ``finish()``
    fails the whole terminal closed because the run can still be honestly
    reported as failed; a read cannot -- the run is over, and discarding a
    stored answer would also discard real metrics and evidence that are not
    the problem. So the offending FIELDS are replaced and everything checkable
    is preserved.

    This redacts; it does not repair the stored row. Backfilling or
    quarantining already-persisted violating rows is a separate operational
    task, deliberately not done implicitly on a read path.

    CHAOS-3377 HIGH (round 2): the refused-with-grounding normalization
    (``_normalize_refused_with_grounding``) runs FIRST, unconditionally --
    it is not an internal-token leak, so it is not gated behind the
    ``internal_token_leak`` check below, which would otherwise skip it
    entirely for a contradictory row whose prose happens not to contain a
    denylisted token.
    """

    answer = _normalize_refused_with_grounding(answer)
    attested = attested_strings(answer, question)
    if (
        internal_token_leak(user_visible_strings(answer=answer), attested=attested)
        is None
    ):
        return answer

    def clean(value: str) -> str:
        return _clean_or_withhold(value, attested=attested)

    return answer.model_copy(
        update={
            "direct_summary": clean(answer.direct_summary),
            "warnings": [clean(warning) for warning in answer.warnings],
            "suggested_follow_up_questions": [
                question_text
                for question_text in answer.suggested_follow_up_questions
                if internal_token_leak([question_text], attested=attested) is None
            ],
            "claims": [
                claim.model_copy(update={"text": clean(claim.text)})
                for claim in answer.claims
            ],
            "conflicts": [
                conflict.model_copy(update={"summary": clean(conflict.summary)})
                for conflict in answer.conflicts
            ],
            "resolved_scope": answer.resolved_scope.model_copy(
                update={
                    "warnings": [
                        clean(warning) for warning in answer.resolved_scope.warnings
                    ]
                }
            ),
        }
    )


def redact_persisted_error(error: DevError) -> DevError:
    """``redact_persisted_answer``'s sibling for a stored terminal error.

    ``DevError.code`` is deliberately untouched: it is a machine field on the
    wire contract, not copy, and clients switch on it. Only the two prose
    fields a human reads are checked. No ``attested`` set: an error carries no
    entity of its own to attest anything, so any denylisted token in its prose
    is a leak.
    """

    if internal_token_leak(user_visible_strings(error=error)) is None:
        return error
    return error.model_copy(
        update={
            "safe_message": (
                error.safe_message
                if internal_token_leak([error.safe_message]) is None
                else WITHHELD_COPY
            ),
            "remediation": [
                item
                for item in error.remediation
                if internal_token_leak([item]) is None
            ],
        }
    )
