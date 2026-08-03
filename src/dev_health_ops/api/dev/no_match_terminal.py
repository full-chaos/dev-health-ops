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
    "NO_MATCH_PUBLIC_OUTCOME",
    "NO_MATCH_PUBLIC_OUTCOME_LABEL",
    "internal_token_leak",
    "named_subject_not_found_answer",
    "no_match_summary",
    "user_supplied_subject_kind",
    "user_supplied_subject_label",
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


#: The TRD §7.1 public class a named-subject no-match maps to, and the label
#: that class renders as. The internal ``forbidden_or_not_found`` outcome
#: stays on the wire (it is baked into five published v1 JSON Schemas); this
#: is the public side of that boundary mapping, and the web renderer reads the
#: same label so there is one string, not two that can drift.
NO_MATCH_PUBLIC_OUTCOME = PublicOutcome.NOT_FOUND
NO_MATCH_PUBLIC_OUTCOME_LABEL = "No authorized match found"


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


def internal_token_leak(values: Iterable[str | None]) -> str | None:
    """The first denylisted internal token found in ``values``, or ``None``.

    Case-insensitive, substring-based: the reported defect rendered
    ``forbidden_or_not_found`` inside a longer sentence, so an equality check
    would not have seen it.
    """

    for value in values:
        if not value:
            continue
        lowered = value.casefold()
        for token in sorted(INTERNAL_TOKEN_DENYLIST):
            if token in lowered:
                return token
    return None


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
    haystack = question.casefold()
    start = haystack.find(needle.casefold())
    if start < 0:
        return None
    return question[start : start + len(needle)]


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
    escaped = re.escape(label)
    trailing = re.compile(rf"{escaped}\s+({nouns})\b", re.IGNORECASE)
    leading = re.compile(rf"\b({nouns})\s+{escaped}", re.IGNORECASE)
    for pattern in (leading, trailing):
        found = pattern.search(question)
        if found:
            return _SUBJECT_KIND_BY_NOUN[" ".join(found.group(1).split()).casefold()]
    return _DEFAULT_SUBJECT_KIND


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
