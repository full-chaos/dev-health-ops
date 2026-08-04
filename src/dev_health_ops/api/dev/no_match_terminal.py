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

from .contracts import (
    AnswerStatus,
    DevAnswer,
    DevContractVersions,
    DevCoverage,
    DevError,
    DevModelMetadata,
    DevScopeResolution,
    ScopeResolutionOutcome,
)
from .contracts_v2 import PublicOutcome, ResolutionOutcome
from .orchestrator_states import RunState

__all__ = [
    "INTERNAL_TOKEN_DENYLIST",
    "NEVER_ATTESTABLE_TOKENS",
    "WITHHELD_COPY",
    "attested_strings",
    "internal_token_leak",
    "named_subject_not_found_answer",
    "no_match_summary",
    "redact_persisted_answer",
    "redact_persisted_error",
    "user_supplied_subject_kind",
    "user_supplied_subject_label",
    "user_visible_strings",
]


def _underscore_members(values: Iterable[str]) -> frozenset[str]:
    return frozenset(value for value in values if "_" in value)


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

    attested_text = " ".join(text.casefold() for text in attested if text)
    for value in values:
        if not value:
            continue
        lowered = value.casefold()
        for token in sorted(INTERNAL_TOKEN_DENYLIST):
            if token not in lowered:
                continue
            if token in NEVER_ATTESTABLE_TOKENS or token not in attested_text:
                return token
    return None


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

    strings: list[str] = []
    if answer is not None:
        strings.append(answer.direct_summary)
        strings.extend(answer.warnings)
        strings.extend(claim.text for claim in answer.claims)
        strings.extend(conflict.summary for conflict in answer.conflicts)
        strings.extend(answer.suggested_follow_up_questions)
        strings.extend(answer.resolved_scope.warnings)
    if error is not None:
        strings.append(error.safe_message)
        strings.extend(error.remediation)
    return tuple(strings)


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
    """

    attested = attested_strings(answer, question)
    if (
        internal_token_leak(user_visible_strings(answer=answer), attested=attested)
        is None
    ):
        return answer

    def clean(value: str) -> str:
        return (
            value
            if internal_token_leak([value], attested=attested) is None
            else WITHHELD_COPY
        )

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
