"""Server-owned question interpretation for Ask Dev (CHAOS-3292).

Amendment TRD v2 §4.1: the interpreter is server-owned. This module turns one
``dev_message_request`` into an authoritative ``dev_question_intent.v1`` plus
the ordered ``dev_subject_mention.v1`` list the subject preflight resolves. It
never executes a tool, never reads a catalog, and never widens a scope.

On "no new regex heuristics"
---------------------------

The recognizers below do lexical matching over a normalized question, and this
docstring says so rather than pretending otherwise. The load-bearing difference
from the CHAOS-3289 backstop this work replaces is the **failure mode**, not the
technique:

* The 3289 pattern extracted a name from prose, compared it against
  *model-authored answer text*, and **deleted an otherwise valid answer**
  (``orchestrator._unresolved_named_entity_error``). A miss left a fabricated
  answer standing; a false positive destroyed a good one.
* A recognizer here only picks a member of the closed ``QuestionIntentID``
  enum. A miss degrades to ``BOUNDED_INVESTIGATION`` — today's model-driven
  path. **A recognizer can never delete an answer and can never widen scope.**
  Whether a subject-bearing tool may execute is decided by *catalog resolution*
  of the extracted mentions (``subject_preflight``), never by matching prose
  against prose.

Mention extraction is likewise lexical, and has a stated residual gap: a
*typed* mention requires a name adjacent to one of the closed kind nouns
below. A noun-less name ("how is Nightfall doing?") and a lowercase definite
description of a body of work ("what about the auth work?") produce an
**untyped** mention instead — resolved across every kind, and degrading to
today's organization-wide behaviour with the legacy backstop still armed when
nothing resolves. That gap is narrower than fabricating a subject, and is the
direction CHAOS-3301 and CHAOS-3648 widen.

What remains outside extraction entirely is a question that contains no name
at all — a pronoun ("what's holding it up?") or a bare definite description
with a relative clause ("the project that kept cycling in review"). Those need
conversational-reference resolution, not a wider grammar, and no recognizer
here should pretend otherwise by guessing.

``interpretation_reasons`` carries **recognizer IDs only, never question
text**, per the "no raw question or entity text in logs, traces, or metric
labels" guardrail.
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from .contracts import DevEntityRef, DevMessageRequest
from .contracts_v2 import (
    Cardinality,
    DevQuestionIntent,
    DevSubjectMention,
    EntityKind,
    QuestionIntentID,
)
from .metrics.definitions import METRIC_REGISTRY

__all__ = [
    "CLARIFICATION_REASONS",
    "CLIENT_HINT_DEPRECATION_WARNING",
    "FALLBACK_CONFIDENCE_FLOOR",
    "INTERPRETER_VERSION",
    "MAX_MENTIONS",
    "ClassifierProposal",
    "IntentClassifier",
    "InterpretedQuestion",
    "QuestionInterpreter",
    "count_mention_candidates",
    "extract_mentions",
]

#: Satisfies ``base.PlatformVersionToken``, which
#: ``DevFrameVersions.interpreter_version`` requires.
INTERPRETER_VERSION = "intent_interpreter.v1"

#: Below this the deterministic recognizers did not commit to an intent and the
#: constrained model fallback (if one is wired) may propose one.
FALLBACK_CONFIDENCE_FLOOR = 0.6

#: ``DevSubjectMention.mention_ordinal`` is bounded at 24 by the contract.
MAX_MENTIONS = 25

_DETERMINISTIC_CONFIDENCE = 0.9
_UNRECOGNIZED_CONFIDENCE = 0.4
#: A model-assisted intent is never trusted as much as a recognizer hit.
_MAX_FALLBACK_CONFIDENCE = 0.85

#: Server-owned copy. The client's ``question_class`` is a non-authoritative
#: hint the interpreter records and ignores for planning (TRD v2 §4.1).
CLIENT_HINT_DEPRECATION_WARNING = (
    "The client-supplied question_class is deprecated and is ignored for "
    "planning; the server interprets the question itself."
)

#: Canonical, content-free clarification reasons. A reason is picked from this
#: closed table — it is never assembled from question or entity text.
CLARIFICATION_REASONS: Mapping[str, str] = {
    "interpreter_unavailable": (
        "The question could not be interpreted confidently. Please rephrase it, "
        "naming the project, repository, or team you mean."
    ),
    "interpreter_rejected_extraction": (
        "The question could not be interpreted confidently. Please name the "
        "project, repository, or team you are asking about."
    ),
}


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

_WHITESPACE = re.compile(r"\s+")


def _normalize(text: str) -> str:
    """NFKC, casefolded, whitespace-collapsed — the recognizer input form."""

    return _WHITESPACE.sub(" ", unicodedata.normalize("NFKC", text).casefold()).strip()


# ---------------------------------------------------------------------------
# Mention extraction
# ---------------------------------------------------------------------------

#: The closed noun vocabulary a name must be adjacent to. Ordered longest-first
#: so "pull request" wins over "pr" and "work unit" over "unit".
_KIND_NOUNS: tuple[tuple[str, EntityKind], ...] = (
    ("pull requests", EntityKind.PULL_REQUEST),
    ("merge requests", EntityKind.PULL_REQUEST),
    ("pull request", EntityKind.PULL_REQUEST),
    ("merge request", EntityKind.PULL_REQUEST),
    ("work units", EntityKind.WORK_UNIT),
    ("work unit", EntityKind.WORK_UNIT),
    ("repositories", EntityKind.REPOSITORY),
    ("repository", EntityKind.REPOSITORY),
    ("repos", EntityKind.REPOSITORY),
    ("repo", EntityKind.REPOSITORY),
    ("projects", EntityKind.PROJECT),
    ("project", EntityKind.PROJECT),
    ("teams", EntityKind.TEAM),
    ("team", EntityKind.TEAM),
    ("tickets", EntityKind.ISSUE),
    ("ticket", EntityKind.ISSUE),
    ("issues", EntityKind.ISSUE),
    ("issue", EntityKind.ISSUE),
    ("prs", EntityKind.PULL_REQUEST),
    ("pr", EntityKind.PULL_REQUEST),
)

_KIND_BY_NOUN: Mapping[str, EntityKind] = dict(_KIND_NOUNS)

_NOUN_ALTERNATION = "|".join(re.escape(noun) for noun, _kind in _KIND_NOUNS)

#: A capitalized display name of one to four words.
_NAME = r"[A-Z][A-Za-z0-9&/'\-]*(?:[ \t]+[A-Z][A-Za-z0-9&/'\-]*){0,3}"
#: A quoted span; quoting is an explicit naming act, so no capitalization is
#: required inside it.
_QUOTED = r"[\"'‘’“”`](?P<quoted>[^\"'‘’“”`]{1,120})[\"'‘’“”`]"

# Noun leading: "project Ask Dev", 'repo "dev-health-ops"'. CHAOS-3388: only
# the noun alternation is matched case-insensitively -- via the scoped inline
# flag `(?i:...)`, not a top-level `re.IGNORECASE`, which would also fold
# `_NAME`'s `[A-Z]` and silently turn any lowercase word into a "capitalized"
# name span. "the ACR Project" (a capitalized noun immediately after an
# all-caps acronym) is exactly as much a named mention as "the ACR project"
# is, and `_candidate_from` already `.casefold()`s the matched noun before its
# `_KIND_BY_NOUN` lookup, which only makes sense if the match itself can be
# any case. Without case-insensitivity here that casefold was dead code: the
# noun literal never matched a capitalized occurrence in the first place, so
# the mention was silently dropped to the untyped bare-name path instead.
_NOUN_LEADING = re.compile(
    rf"\b(?P<noun>(?i:{_NOUN_ALTERNATION}))\s+(?:{_QUOTED}|(?P<plain>{_NAME}))",
)
# Noun trailing: "the Ask Dev project", '"dev-health-ops" repo'.
_NOUN_TRAILING = re.compile(
    rf"(?:{_QUOTED}|(?P<plain>{_NAME}))\s+(?P<noun>(?i:{_NOUN_ALTERNATION}))\b",
)

#: Capitalized words that are never a subject name on their own. A candidate
#: consisting only of these is discarded, so "Our team is overburdened" does
#: not mint a mention for "Our".
_NAME_STOP_WORDS = frozenset(
    {
        "a",
        "all",
        "an",
        "and",
        "any",
        "are",
        "back",
        "can",
        "check",
        "compare",
        "could",
        "did",
        "do",
        "does",
        "each",
        "every",
        "find",
        "for",
        "get",
        "give",
        "has",
        "have",
        "help",
        "how",
        "in",
        "is",
        "it",
        "its",
        "list",
        "look",
        "many",
        "me",
        "my",
        "need",
        "new",
        "no",
        "of",
        "old",
        "on",
        "one",
        "or",
        "other",
        "our",
        "please",
        "review",
        "same",
        "see",
        "should",
        "show",
        "some",
        "status",
        "tell",
        "that",
        "the",
        "their",
        "these",
        "this",
        "those",
        "update",
        "us",
        "want",
        "was",
        "we",
        "were",
        "what",
        "whats",
        "when",
        "where",
        "which",
        "who",
        "why",
        "would",
        "your",
    }
)


@dataclass(frozen=True, slots=True)
class _MentionCandidate:
    start: int
    span: str
    kind: EntityKind
    #: Set only for a typed context ref, whose canonical id is the lookup key.
    lookup_text: str | None = None


def _candidate_from(match: re.Match[str]) -> _MentionCandidate | None:
    quoted = match.group("quoted")
    plain = match.group("plain")
    raw = quoted if quoted is not None else plain
    if raw is None:
        return None
    span = raw.strip()
    if len(span) < 2 or len(span) > 256:
        return None
    words = [word for word in _normalize(span).split(" ") if word]
    if not words or all(word.strip("'’-") in _NAME_STOP_WORDS for word in words):
        return None
    kind = _KIND_BY_NOUN[match.group("noun").casefold()]
    start = match.start("quoted") if quoted is not None else match.start("plain")
    return _MentionCandidate(start=start, span=span, kind=kind)


#: A bare capitalized or quoted span, with no kind noun anywhere beside it.
_BARE_NAME = re.compile(rf"(?:{_QUOTED}|(?P<plain>{_NAME}))")

#: Head nouns that denote a **discrete undertaking** — a body of engineering
#: work — without naming any member of ``EntityKind`` (CHAOS-3648).
#:
#: The class is eventive/deverbal nouns of *undertaking*: each one answers
#: "what is being done", and each takes the restrictive modifiers a proper
#: name occupies ("the payroll migration", "the search rewrite"). Nouns that
#: denote a **delta or an event** rather than an undertaking are deliberately
#: excluded — "change", "update", "fix", "release" head noun phrases that are
#: overwhelmingly descriptions of a difference ("the biggest change", "the
#: latest update"), so admitting them would mint mentions out of ordinary
#: prose. Nothing here overlaps ``_KIND_NOUNS``: a phrase whose head *does*
#: name a kind is the typed grammar's to claim.
_WORK_HEAD_NOUNS: frozenset[str] = frozenset(
    {
        "adoption",
        "buildout",
        "cleanup",
        "consolidation",
        "effort",
        "efforts",
        "hardening",
        "initiative",
        "initiatives",
        "integration",
        "integrations",
        "migration",
        "migrations",
        "modernisation",
        "modernization",
        "overhaul",
        "overhauls",
        "program",
        "programme",
        "programs",
        "redesign",
        "redesigns",
        "refactor",
        "refactoring",
        "refactors",
        "replatform",
        "revamp",
        "revamps",
        "rewrite",
        "rewrites",
        "rollout",
        "rollouts",
        "upgrade",
        "upgrades",
        "work",
        "workstream",
        "workstreams",
    }
)

#: Determiners whose noun phrase is a **definite description**: it presupposes
#: a referent the hearer can already identify uniquely. That presupposition is
#: what makes "the payroll migration" a naming act in the way "a migration"
#: is not, and it is why capitalization is not required after one — a chat
#: user lowercases a name they have already established as shared.
_DEFINITE_DETERMINERS: tuple[str, ...] = ("the", "this", "that", "our")

#: Modifiers that quantify, order, or locate in time rather than individuate.
#: "the current work" restricts by recency, not by identity, so the tokens are
#: stripped from the left of a definite description before it is treated as a
#: name. They are held apart from ``_NAME_STOP_WORDS`` because that list
#: governs capitalized spans, where a leading "Current" is far rarer.
_NON_NAMING_MODIFIERS: frozenset[str] = frozenset(
    {
        "actual",
        "biggest",
        "current",
        "entire",
        "final",
        "first",
        "general",
        "hardest",
        "initial",
        "last",
        "latest",
        "main",
        "most",
        "much",
        "next",
        "ongoing",
        "only",
        "outstanding",
        "overall",
        "previous",
        "prior",
        "real",
        "recent",
        "remaining",
        "rest",
        "total",
        "upcoming",
        "whole",
    }
)

_WORK_NOUN_ALTERNATION = "|".join(
    re.escape(noun) for noun in sorted(_WORK_HEAD_NOUNS, key=len, reverse=True)
)
_DETERMINER_ALTERNATION = "|".join(
    re.escape(word) for word in sorted(_DEFINITE_DETERMINERS, key=len, reverse=True)
)

#: ``the <one to three modifiers> <work head noun>``. Matched case-insensitively
#: on purpose: unlike ``_NAME``, this pattern does not use capitalization as
#: its evidence that a name is present — the definite determiner is.
_DEFINITE_DESCRIPTION = re.compile(
    rf"\b(?:{_DETERMINER_ALTERNATION})[ \t]+"
    rf"(?P<phrase>(?:[\w][\w&/'’\-]*[ \t]+){{1,3}}"
    rf"(?:{_WORK_NOUN_ALTERNATION}))\b",
    re.IGNORECASE,
)

_WORD_PUNCTUATION = re.compile(r"[^\w]+", re.UNICODE)


def _strip_word_punctuation(word: str) -> str:
    """``what's`` -> ``whats``, so contractions match the stop-word list."""

    return _WORD_PUNCTUATION.sub("", word)


def _opens_a_sentence(question: str, start: int) -> bool:
    prefix = question[:start].rstrip()
    return not prefix or prefix[-1] in ".!?;:"


def _is_naming_token(token: str) -> bool:
    """Whether ``token`` can individuate a referent, rather than quantify one."""

    word = _strip_word_punctuation(_normalize(token))
    return bool(word) and word not in _NAME_STOP_WORDS | _NON_NAMING_MODIFIERS


def _definite_description_spans(question: str) -> list[tuple[int, str]]:
    """``(start, span)`` for each definite description naming a body of work.

    CHAOS-3648. "What about the auth work?" and "what happened to the payments
    rewrite?" name a subject exactly as plainly as "the Nightfall project"
    does. Two independent properties of the kind-noun grammar make them
    invisible to it, and both are properties of English rather than of any
    corpus: the head noun ("work", "rewrite") is not a member of
    ``EntityKind``, and the modifier that carries the name is lowercased, so
    ``_NAME``'s capitalization evidence is absent. The definite determiner
    supplies the missing evidence — a definite description presupposes a
    uniquely identifiable referent, which is precisely the claim "this is a
    name" makes.

    Two readings are emitted per phrase, in specific-first order, because
    English does not settle which one holds without a catalog:

    * The **whole phrase**, for the reading where the head noun is part of the
      proper name ("the Payments Rewrite").
    * The **modifiers alone**, for the reading where the head noun is a common
      classifier of a differently-named entity ("the AuthCore Adoption work"),
      emitted only when the head noun is lowercased — a capitalized head is
      the writer's own evidence that it belongs to the name.

    This mirrors ``_NOUN_LEADING``/``_NOUN_TRAILING`` emitting a candidate per
    reading and letting resolution rank them; a broader reading that matches
    nothing costs nothing, and one that matches ranks below an exact
    display-name or alias hit.
    """

    spans: list[tuple[int, str]] = []
    for match in _DEFINITE_DESCRIPTION.finditer(question):
        phrase_start = match.start("phrase")
        tokens = [
            (phrase_start + token.start(), token.group())
            for token in re.finditer(r"\S+", match.group("phrase"))
        ]
        if len(tokens) < 2:
            continue
        head_start, head = tokens[-1]
        modifiers = tokens[:-1]
        # A leading "current"/"the"/"other" restricts by recency or contrast,
        # never by identity, so it is not part of the name it precedes.
        while modifiers and not _is_naming_token(modifiers[0][1]):
            modifiers = modifiers[1:]
        if not modifiers:
            continue
        start = modifiers[0][0]
        spans.append((start, question[start : head_start + len(head)]))
        if head[:1].islower():
            last_start, last = modifiers[-1]
            spans.append((start, question[start : last_start + len(last)]))
    return spans


def untyped_name_candidates(
    question: str, typed: Sequence[str] = ()
) -> tuple[str, ...]:
    """Naming spans the kind-noun grammar did not claim.

    Two shapes qualify, and neither states a kind:

    * A **capitalized or quoted** span. "How is Nightfall doing?" names a
      subject as plainly as "the Nightfall project" does; only the noun that
      would let us *type* it is missing.
    * A **definite description** of a body of work — "the auth work", "the
      payments rewrite" (CHAOS-3648, see ``_definite_description_spans``).

    These spans are resolved across every searchable kind, and — crucially —
    an unresolved one does **not** terminate the run, because we are not
    confident the span was a subject at all ("What is our DORA score?" would
    otherwise break). It re-arms the legacy backstop instead, which judges the
    model's own answer text and is exactly today's behaviour for this shape.
    That non-terminating failure mode is why the definite-description reading
    belongs here rather than in the typed grammar: a wrong *typed* mention
    refuses a question that works today, while a wrong untyped one simply
    matches nothing.

    Deliberately **uncapped** (CHAOS-3301 review fix): this used to truncate
    to ``MAX_MENTIONS`` internally, so a caller reading ``len(...)`` as an
    "uncapped" total for the oversized-rejection guard was in fact reading an
    already-capped number — a 26th all-untyped subject was silently invisible
    to that guard, not merely dropped from the merged mention list. Capping
    for the merge bound is the caller's job (``_add_untyped_mentions``
    already does it); this function only finds candidates.
    """

    claimed = {value.casefold() for value in typed}
    found: list[str] = []
    seen: set[str] = set()
    raw_spans: list[tuple[int, str]] = []
    for match in _BARE_NAME.finditer(question):
        raw = match.group("quoted") or match.group("plain")
        if raw is None:
            continue
        start = match.start("quoted")
        if start < 0:
            start = match.start("plain")
        raw_spans.append((start, raw))
    # Definite descriptions are merged by position rather than appended, so
    # mention ordinals stay in the order a reader sees the phrases in. The
    # sort is stable, so a bare name keeps its precedence over a definite
    # description that starts at the same offset.
    raw_spans.extend(_definite_description_spans(question))
    raw_spans.sort(key=lambda item: item[0])
    for start, raw in raw_spans:
        span = raw.strip()
        if len(span) < 2 or len(span) > 256:
            continue
        normalized = _normalize(span)
        words = [_strip_word_punctuation(word) for word in normalized.split(" ")]
        words = [word for word in words if word]
        if not words or all(word in _NAME_STOP_WORDS for word in words):
            continue
        # A single capitalized word that *opens* a sentence is capitalized by
        # grammar, not by naming: "Anything about X?" must not mint a subject
        # called "Anything". A multi-word span, or one anywhere else in the
        # sentence, is still a candidate.
        if len(words) == 1 and _opens_a_sentence(question, start):
            continue
        if normalized in seen or any(normalized in name for name in claimed):
            continue
        if any(name in normalized for name in claimed):
            continue
        seen.add(normalized)
        found.append(span)
    return tuple(found)


def _candidate_mentions(
    question: str, *, context_refs: Sequence[DevEntityRef] = ()
) -> list[_MentionCandidate]:
    """The full ordered, deduplicated mention candidate list, uncapped.

    Shared by ``extract_mentions`` (which additionally caps the result to
    ``MAX_MENTIONS`` for the ``DevSubjectMention``/``DevResolutionLedger``
    contract bound) and ``count_mention_candidates`` (which reports the raw,
    uncapped count — CHAOS-3301, see its docstring for why that matters).

    Two input classes, in precedence order:

    1. **Explicit user text** — a capitalized or quoted span adjacent to one of
       the closed kind nouns. Always ordinal-first.
    2. **Typed context refs** — the request's page/surface entity refs. These
       are admitted **only when the question names nothing itself**. When the
       user did name something, a context ref is a tiebreaker for ambiguity
       (applied by ``subject_preflight``), never an override: silently
       substituting page context for an unrecognized name is the fabrication
       path in a different costume.
    """

    candidates: list[_MentionCandidate] = []
    # A noun that is *followed* by a name binds to that name. Suppressing the
    # trailing reading of the same noun occurrence is what keeps "Compare
    # project Ask Dev" from minting a "Compare" project: one noun occurrence
    # names one entity, and the leading form is the unambiguous one.
    leading_noun_starts: set[int] = set()
    for match in _NOUN_LEADING.finditer(question):
        candidate = _candidate_from(match)
        if candidate is not None:
            leading_noun_starts.add(match.start("noun"))
            candidates.append(candidate)
    for match in _NOUN_TRAILING.finditer(question):
        if match.start("noun") in leading_noun_starts:
            continue
        candidate = _candidate_from(match)
        if candidate is not None:
            candidates.append(candidate)

    ordered: list[_MentionCandidate] = []
    seen: set[tuple[str, EntityKind]] = set()
    for candidate in sorted(candidates, key=lambda item: (item.start, item.span)):
        key = (_normalize(candidate.span), candidate.kind)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(candidate)

    # A context ref stands in for the subject only when the question named
    # *nothing at all* — including a name the kind-noun grammar could not type.
    # Substituting the page's project for a name we merely failed to parse is
    # the fabricated-premise defect wearing a different costume: the run would
    # answer about the page's entity under the name the user typed.
    if not ordered and not untyped_name_candidates(question):
        for ref in context_refs:
            try:
                kind = EntityKind(ref.entity_type.value)
            except ValueError:
                continue
            key = (_normalize(ref.entity_id), kind)
            if key in seen:
                continue
            seen.add(key)
            # A typed context ref already carries the catalog's own canonical
            # id, so the id *is* the normalized lookup key — casefolding it
            # would break an exact id match. The human-readable label stays in
            # ``original_text_span``.
            ordered.append(
                _MentionCandidate(
                    start=len(question) + len(ordered),
                    span=ref.display_label,
                    kind=kind,
                    lookup_text=ref.entity_id,
                )
            )
    return ordered


def count_mention_candidates(
    question: str, *, context_refs: Sequence[DevEntityRef] = ()
) -> int:
    """The number of named subjects in ``question``, before ``MAX_MENTIONS`` capping.

    CHAOS-3301. ``len(extract_mentions(question))`` alone cannot distinguish
    "a complete 25-subject cohort" from "26 subjects named, the 26th silently
    dropped" — both report exactly 25, since ``DevSubjectMention.mention_ordinal``
    and ``DevResolutionLedger.mention_ids`` are hard-capped at the same bound.
    Bounds must be rejections, never truncations: this is what lets the subject
    preflight tell the two apart and reject the oversized case honestly instead
    of narrating a false "complete" cohort.
    """

    return len(_candidate_mentions(question, context_refs=context_refs))


def extract_mentions(
    question: str,
    *,
    context_refs: Sequence[DevEntityRef] = (),
    mint_id: Callable[[], str] = lambda: str(uuid.uuid4()),
) -> tuple[DevSubjectMention, ...]:
    """Ordered, deduplicated subject mentions for one question, capped at
    ``MAX_MENTIONS`` — see ``count_mention_candidates`` for the uncapped count.
    """

    ordered = _candidate_mentions(question, context_refs=context_refs)
    mentions: list[DevSubjectMention] = []
    for ordinal, candidate in enumerate(ordered[:MAX_MENTIONS]):
        mentions.append(
            DevSubjectMention(
                schema_version="dev_subject_mention.v1",
                mention_id=mint_id(),
                mention_ordinal=ordinal,
                original_text_span=candidate.span[:2048],
                requested_entity_kind=candidate.kind,
                normalized_lookup_text=(
                    candidate.lookup_text or _normalize(candidate.span)
                )[:2048],
            )
        )
    return tuple(mentions)


# ---------------------------------------------------------------------------
# Deterministic recognizers
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Signals:
    normalized: str
    mention_count: int
    has_metric_alias: bool
    has_requested_metrics: bool


def _any_of(*phrases: str) -> Callable[[str], bool]:
    closed = tuple(phrases)

    def predicate(text: str) -> bool:
        return any(phrase in text for phrase in closed)

    return predicate


_STATUS_ANCHORS = _any_of(
    "status",
    "current state",
    "where are we on",
    "where do we stand",
    "how far along",
    "done",
    "complete",
    "ready",
    "release ready",
    "release-ready",
    "shippable",
)
_PORTFOLIO_ANCHORS = _any_of(
    "portfolio",
    "all projects",
    "every project",
    "across projects",
    "our projects",
    "each project",
)
_REMAINING_ANCHORS = _any_of(
    "what's left",
    "whats left",
    "what is left",
    "remaining",
    "outstanding",
    "still open",
    "blocked",
    "blocker",
    "what should we do first",
    "what do we do next",
    "next up",
)
_CHANGE_ANCHORS = _any_of(
    "what changed",
    "what has changed",
    "improved",
    "got worse",
    "regressed",
    "regression",
    "since last",
    "since the",
    "versus",
    " vs ",
    "compared to",
    "compared with",
    "week over week",
    "trend",
)
_REGISTERED_STATS_ANCHORS = _any_of(
    "what metrics",
    "which metrics",
    "which stats",
    "what stats",
    "what statistics",
    "what can you measure",
    "what can you report",
    "available metrics",
    "list metrics",
    "list the metrics",
    "metrics do you",
    "metrics can you",
)
_COMPARISON_ANCHORS = _any_of(
    "compare",
    "comparison",
    "versus",
    " vs ",
    "compared to",
    "compared with",
    "higher than",
    "lower than",
    "better than",
    "worse than",
    "against",
    "benchmark",
)
_TRUST_ANCHORS = _any_of(
    "stale",
    "fresh",
    "freshness",
    "missing data",
    "unconfigured",
    "not configured",
    "coverage",
    "can i trust",
    "can we trust",
    "how reliable",
    "data quality",
    "up to date",
    "up-to-date",
)
_HEALTH_ANCHORS = _any_of(
    "health",
    "healthy",
    "at risk",
    "at-risk",
    "needs attention",
    "how are things going",
    "how is it going",
    "trouble",
)
_TEAM_SUBJECT_ANCHORS = _any_of("team", "squad", "group")
_PROJECT_SUBJECT_ANCHORS = _any_of(
    "project", "repository", "repo", "service", "product"
)
_WORKLOAD_ANCHORS = _any_of(
    "overburdened",
    "overloaded",
    "workload",
    "work load",
    "pressure",
    "capacity",
    "investment mix",
    "allocation",
    "balance",
    "spread thin",
    "bandwidth",
)
_DEFICIENCY_ANCHORS = _any_of(
    "operational deficiencies",
    "operational deficiency",
    "operational gaps",
    "what's broken operationally",
    "whats broken operationally",
    "operational risks",
    "missing controls",
    "process gaps",
)
_RANKING_ANCHORS = _any_of(
    "top ",
    "most ",
    "least ",
    "worst",
    "best",
    "rank",
    "biggest",
    "highest",
    "lowest",
    "which team",
    "which project",
)
_PERIOD_COMPARISON_ANCHORS = _any_of(
    "since last",
    "last week",
    "last month",
    "last quarter",
    "week over week",
    "month over month",
    "previous period",
    "compared to last",
    "trend",
)
_COHORT_COMPARISON_ANCHORS = _any_of(
    "compared to other",
    "versus other",
    " vs other",
    "against other",
    "relative to other",
    "benchmark",
)


def _metric_aliases() -> frozenset[str]:
    """The closed set of metric names a question may name, from the registry."""

    aliases: set[str] = set()
    for metric_id, definition in METRIC_REGISTRY.items():
        aliases.add(metric_id.value)
        aliases.add(metric_id.value.replace("_", " "))
        aliases.add(_normalize(definition.label))
    return frozenset(alias for alias in aliases if len(alias) >= 4)


_METRIC_ALIASES = _metric_aliases()


@dataclass(frozen=True, slots=True)
class _Recognizer:
    recognizer_id: str
    intent: QuestionIntentID
    matches: Callable[[_Signals], bool]


#: Ordered, first-match-wins. Ordering is the tiebreak; it is deliberate and
#: pinned by test, so a question that satisfies two recognizers always resolves
#: to the same intent.
_RECOGNIZERS: tuple[_Recognizer, ...] = (
    _Recognizer(
        "stats.registered",
        QuestionIntentID.REGISTERED_STATISTICS,
        lambda s: _REGISTERED_STATS_ANCHORS(s.normalized),
    ),
    _Recognizer(
        "metric.comparison",
        QuestionIntentID.METRIC_COMPARISON,
        lambda s: (
            (s.has_metric_alias or s.has_requested_metrics)
            and _COMPARISON_ANCHORS(s.normalized)
        ),
    ),
    _Recognizer(
        "trust.data",
        QuestionIntentID.DATA_TRUST,
        lambda s: _TRUST_ANCHORS(s.normalized),
    ),
    _Recognizer(
        "deficiency.operational",
        QuestionIntentID.OPERATIONAL_DEFICIENCY_INVENTORY,
        lambda s: _DEFICIENCY_ANCHORS(s.normalized),
    ),
    _Recognizer(
        "balance.team_workload",
        QuestionIntentID.TEAM_WORKLOAD_BALANCE,
        lambda s: (
            _WORKLOAD_ANCHORS(s.normalized) and _TEAM_SUBJECT_ANCHORS(s.normalized)
        ),
    ),
    _Recognizer(
        "health.team",
        QuestionIntentID.TEAM_HEALTH,
        lambda s: _HEALTH_ANCHORS(s.normalized) and _TEAM_SUBJECT_ANCHORS(s.normalized),
    ),
    _Recognizer(
        "health.project",
        QuestionIntentID.PROJECT_HEALTH,
        lambda s: (
            _HEALTH_ANCHORS(s.normalized) and _PROJECT_SUBJECT_ANCHORS(s.normalized)
        ),
    ),
    _Recognizer(
        "change.observed",
        QuestionIntentID.OBSERVED_CHANGE,
        lambda s: _CHANGE_ANCHORS(s.normalized),
    ),
    _Recognizer(
        "work.remaining",
        QuestionIntentID.REMAINING_WORK,
        lambda s: _REMAINING_ANCHORS(s.normalized),
    ),
    _Recognizer(
        "status.portfolio",
        QuestionIntentID.PORTFOLIO_STATUS,
        lambda s: (
            _STATUS_ANCHORS(s.normalized)
            and (s.mention_count >= 2 or _PORTFOLIO_ANCHORS(s.normalized))
        ),
    ),
    _Recognizer(
        "status.singular",
        QuestionIntentID.ENTITY_STATUS,
        lambda s: _STATUS_ANCHORS(s.normalized) and s.mention_count >= 1,
    ),
)


def _cardinality_for(mention_count: int) -> Cardinality:
    """Derived, never asserted.

    ``DevQuestionIntent`` enforces ``PLURAL_COHORT => >=2 mentions`` and
    ``ORGANIZATION_WIDE => 0 mentions``, so derivation is the only construction
    that validates.
    """

    if mention_count == 0:
        return Cardinality.ORGANIZATION_WIDE
    if mention_count == 1:
        return Cardinality.SINGULAR
    return Cardinality.PLURAL_COHORT


def _comparison_mode(normalized: str, intent: QuestionIntentID) -> str:
    if _COHORT_COMPARISON_ANCHORS(normalized):
        return "cohort_relative"
    if _PERIOD_COMPARISON_ANCHORS(normalized):
        return "period_over_period"
    if intent is QuestionIntentID.OBSERVED_CHANGE:
        return "own_history"
    return "none"


# ---------------------------------------------------------------------------
# Constrained model fallback
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ClassifierProposal:
    """What a constrained classifier is allowed to return — and nothing else.

    It cannot mint an ID, cannot choose a scope, cannot request a tool, and
    cannot clear ``requires_clarification``: those fields simply do not exist
    on this type. Post-validation additionally rejects any ``text_span`` that
    is not a literal substring of the question, which is what makes "cannot
    author entity names" true rather than merely requested.
    """

    intent_id: str
    cardinality: str
    candidates: tuple[tuple[str, str], ...] = ()
    confidence: float = 0.0


class IntentClassifier(Protocol):
    """A no-tool, no-data classifier over the raw question text alone."""

    async def classify(self, *, question: str) -> ClassifierProposal | None: ...


@dataclass(frozen=True, slots=True)
class InterpretedQuestion:
    intent: DevQuestionIntent
    mentions: tuple[DevSubjectMention, ...]
    #: Mention IDs minted from a bare name the kind-noun grammar could not
    #: type. They are resolved across every searchable kind, and an unresolved
    #: one re-arms the legacy backstop rather than terminating the run — see
    #: ``untyped_name_candidates``.
    untyped_mention_ids: frozenset[str] = frozenset()
    #: The number of *typed* named subjects the kind-noun grammar found,
    #: before ``MAX_MENTIONS`` capping (CHAOS-3301). ``len(mentions)`` alone
    #: cannot distinguish "a complete 25-subject cohort" from "26 subjects
    #: named, the 26th silently dropped" — both would report 25. Bounds are
    #: rejections, never truncations: the subject preflight uses this to
    #: reject the oversized case honestly instead of narrating a false
    #: "complete" cohort.
    total_named_mention_count: int = 0

    @property
    def mention_by_id(self) -> dict[str, DevSubjectMention]:
        return {mention.mention_id: mention for mention in self.mentions}


class QuestionInterpreter:
    """Deterministic recognizers plus an optional constrained model fallback."""

    def __init__(
        self,
        *,
        classifier: IntentClassifier | None = None,
        mint_id: Callable[[], str] = lambda: str(uuid.uuid4()),
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._classifier = classifier
        self._mint_id = mint_id
        self._now = now

    async def interpret(self, request: DevMessageRequest) -> InterpretedQuestion:
        context_refs = list(request.scope.entity_refs)
        if request.scope.surface_context is not None:
            context_refs.extend(request.scope.surface_context.entity_refs)
        mentions = extract_mentions(
            request.question, context_refs=context_refs, mint_id=self._mint_id
        )
        # Computed once, from the raw text, before MAX_MENTIONS capping and
        # before any untyped-name/fallback additions — see
        # InterpretedQuestion.total_named_mention_count.
        total_typed_mention_count = count_mention_candidates(
            request.question, context_refs=context_refs
        )
        mentions, untyped_ids, total_untyped_candidate_count = (
            self._add_untyped_mentions(request.question, mentions)
        )
        # CHAOS-3301 fix: the uncapped total must include untyped bare-name
        # candidates too, counted before `_add_untyped_mentions` silently
        # stops merging at MAX_MENTIONS. Counting only the typed grammar
        # candidates let 25 typed + 1 resolvable bare name report a
        # "complete" 25-subject total instead of the true 26 -- the oversized
        # rejection below never saw it.
        total_named_mention_count = (
            total_typed_mention_count + total_untyped_candidate_count
        )
        normalized = _normalize(request.question)
        signals = _Signals(
            normalized=normalized,
            mention_count=len(mentions),
            has_metric_alias=any(alias in normalized for alias in _METRIC_ALIASES),
            has_requested_metrics=bool(request.requested_metric_ids),
        )

        intent_id = QuestionIntentID.BOUNDED_INVESTIGATION
        reasons: list[str] = ["recognizer.none"]
        confidence = _UNRECOGNIZED_CONFIDENCE
        for recognizer in _RECOGNIZERS:
            if recognizer.matches(signals):
                intent_id = recognizer.intent
                reasons = [recognizer.recognizer_id]
                confidence = _DETERMINISTIC_CONFIDENCE
                break

        requires_clarification = False
        clarification_reason: str | None = None
        if confidence < FALLBACK_CONFIDENCE_FLOOR and self._classifier is not None:
            (
                intent_id,
                mentions,
                confidence,
                requires_clarification,
                clarification_reason,
                fallback_reason,
            ) = await self._apply_fallback(
                request=request,
                intent_id=intent_id,
                mentions=mentions,
                confidence=confidence,
            )
            reasons.append(fallback_reason)

        subject_kinds = tuple(
            sorted(
                {mention.requested_entity_kind for mention in mentions},
                key=lambda kind: kind.value,
            )
        )[:5]
        intent = DevQuestionIntent(
            schema_version="dev_question_intent.v1",
            intent_id=intent_id,
            interpreter_version=INTERPRETER_VERSION,
            cardinality=_cardinality_for(len(mentions)),
            subject_kinds=subject_kinds,
            mention_ordinals=tuple(range(len(mentions))),
            requested_dimensions=(),
            requested_metric_ids=tuple(request.requested_metric_ids)[:8],
            comparison_mode=_comparison_mode(normalized, intent_id),
            ranking_requested=_RANKING_ANCHORS(normalized),
            confidence=confidence,
            interpretation_reasons=tuple(reasons[:10]),
            requires_clarification=requires_clarification,
            clarification_reason=clarification_reason,
            # Read, recorded, and never used for planning (TRD v2 §4.1).
            client_question_class_hint=request.question_class,
            client_hint_deprecation_warning=CLIENT_HINT_DEPRECATION_WARNING,
            generated_at=self._now(),
        )
        return InterpretedQuestion(
            intent=intent,
            mentions=mentions,
            untyped_mention_ids=frozenset(untyped_ids)
            & {mention.mention_id for mention in mentions},
            total_named_mention_count=total_named_mention_count,
        )

    def _add_untyped_mentions(
        self, question: str, mentions: tuple[DevSubjectMention, ...]
    ) -> tuple[tuple[DevSubjectMention, ...], set[str], int]:
        """Mint a mention for each bare name the kind-noun grammar missed.

        ``requested_entity_kind`` is a **declared default** for these, because
        the user did not state a kind — the preflight searches every kind, and
        the ledger's committed reference carries whichever kind actually
        matched. Recording a default is the honest option: the alternative is
        no mention at all, which is how an unresolved name reaches an answer.

        Returns the merged mentions, the minted untyped mention ids, and the
        *uncapped* number of untyped candidates found (CHAOS-3301) — counted
        before the ``MAX_MENTIONS`` cap below silently stops merging, so a
        caller can still see a candidate that got dropped for being past the
        bound instead of it vanishing into a false "complete" count.
        """

        typed = [mention.original_text_span for mention in mentions]
        candidates = untyped_name_candidates(question, typed)
        if not candidates:
            return mentions, set(), 0
        merged = list(mentions)
        untyped_ids: set[str] = set()
        for span in candidates:
            if len(merged) >= MAX_MENTIONS:
                break
            mention_id = self._mint_id()
            untyped_ids.add(mention_id)
            merged.append(
                DevSubjectMention(
                    schema_version="dev_subject_mention.v1",
                    mention_id=mention_id,
                    mention_ordinal=len(merged),
                    original_text_span=span[:2048],
                    requested_entity_kind=EntityKind.PROJECT,
                    normalized_lookup_text=_normalize(span)[:2048],
                )
            )
        return tuple(merged), untyped_ids, len(candidates)

    async def _apply_fallback(
        self,
        *,
        request: DevMessageRequest,
        intent_id: QuestionIntentID,
        mentions: tuple[DevSubjectMention, ...],
        confidence: float,
    ) -> tuple[
        QuestionIntentID,
        tuple[DevSubjectMention, ...],
        float,
        bool,
        str | None,
        str,
    ]:
        """One bounded classifier call, then total post-validation.

        On **any** provider error, timeout, malformed output, or post-validation
        rejection the run never falls back to organization scope. When the
        question named something, the failure becomes an explicit clarification
        request; when it named nothing there is no named subject to get wrong,
        so the run keeps today's bounded-investigation behaviour rather than
        refusing a question that works in production now.
        """

        assert self._classifier is not None
        try:
            proposal = await self._classifier.classify(question=request.question)
        except Exception:
            proposal = None
        rejected_reason = "fallback.unavailable"
        if proposal is not None:
            validated = self._validate_proposal(proposal, question=request.question)
            if validated is None:
                rejected_reason = "fallback.rejected"
            else:
                proposed_intent, spans, proposed_confidence = validated
                merged = self._merge_spans(mentions, spans)
                return (
                    proposed_intent,
                    merged,
                    min(proposed_confidence, _MAX_FALLBACK_CONFIDENCE),
                    False,
                    None,
                    "fallback.model_assisted",
                )
        if mentions:
            reason_key = (
                "interpreter_unavailable"
                if rejected_reason == "fallback.unavailable"
                else "interpreter_rejected_extraction"
            )
            return (
                intent_id,
                mentions,
                confidence,
                True,
                CLARIFICATION_REASONS[reason_key],
                rejected_reason,
            )
        return (intent_id, mentions, confidence, False, None, rejected_reason)

    @staticmethod
    def _validate_proposal(
        proposal: ClassifierProposal, *, question: str
    ) -> tuple[QuestionIntentID, tuple[tuple[str, EntityKind], ...], float] | None:
        try:
            intent = QuestionIntentID(proposal.intent_id)
        except ValueError:
            return None
        try:
            Cardinality(proposal.cardinality)
        except ValueError:
            return None
        if not 0.0 <= proposal.confidence <= 1.0:
            return None
        if len(proposal.candidates) > MAX_MENTIONS:
            return None
        spans: list[tuple[str, EntityKind]] = []
        for raw_span, raw_kind in proposal.candidates:
            # The literal-substring rule is what makes "the classifier cannot
            # author an entity name" structural: a name it invented is not in
            # the user's question, so it cannot survive this check.
            if not raw_span or raw_span not in question:
                return None
            # Substring alone is too weak: a single character, or a fragment
            # cutting across word boundaries, is present in almost any question
            # and would let the classifier point at an arbitrary catalog entry
            # while technically quoting the user.
            if len(raw_span.strip()) < 2 or raw_span != raw_span.strip():
                return None
            if not re.search(rf"(?<!\w){re.escape(raw_span)}(?!\w)", question):
                return None
            if all(
                _strip_word_punctuation(word) in _NAME_STOP_WORDS
                for word in _normalize(raw_span).split(" ")
                if word
            ):
                return None
            try:
                kind = EntityKind(raw_kind)
            except ValueError:
                return None
            spans.append((raw_span, kind))
        return intent, tuple(spans), proposal.confidence

    def _merge_spans(
        self,
        mentions: tuple[DevSubjectMention, ...],
        spans: tuple[tuple[str, EntityKind], ...],
    ) -> tuple[DevSubjectMention, ...]:
        merged = list(mentions)
        seen = {
            (mention.normalized_lookup_text, mention.requested_entity_kind)
            for mention in merged
        }
        for span, kind in spans:
            key = (_normalize(span), kind)
            if key in seen or len(merged) >= MAX_MENTIONS:
                continue
            seen.add(key)
            merged.append(
                DevSubjectMention(
                    schema_version="dev_subject_mention.v1",
                    mention_id=self._mint_id(),
                    mention_ordinal=len(merged),
                    original_text_span=span[:2048],
                    requested_entity_kind=kind,
                    normalized_lookup_text=_normalize(span)[:2048],
                )
            )
        return tuple(merged)
