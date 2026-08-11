"""CHAOS-3617 PR2: authorized candidate-subject search.

Resolves a human reference — a canonical id, a display name, an alias, an
acronym, a previous name, a provider identifier — to ranked canonical
subjects inside the caller's authorized scope.

**This capability is exact matching, and saying so is the point.** Every
signal the corpus plants (`alias`, `acronym`, `previous_name`,
`provider_identifier`) is *stored text the arm can look up*. None of it needs
a vector, and dressing it up as retrieval would be a claim the arm cannot
support while :class:`~.backend.DeterministicEmbedder` is active — which is
exactly why every finding carries a :class:`~.backend.MatchMechanism` and why
``packet_builder`` refuses an embedding-derived match under a non-semantic
embedder. The guard is not decoration here: it is the thing standing between
"we resolved 'the auth work' by lookup" and "we resolved it by retrieval",
which score identically in a packet and differ completely in what they prove.

**Authorization is applied before ranking, not after.** A restricted entity
that matches must never occupy a rank, be counted toward truncation, or
appear in a clarification candidate list — all three are ways an entity the
caller may not see becomes visible without ever being "returned".

**Ordering is total and content-derived.** Signal strength first, then
canonical id. No timestamps, no insertion order, no score float: the same
world and the same query produce the same ranking on every machine, which is
what makes a recorded trial run comparable to a re-run.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import SubjectMatchSignal

from .backend import MatchMechanism
from .projection import ALIAS_SIGNAL, GraphNode
from .vocabulary import GraphEntityKind

__all__ = [
    "SIGNAL_RANK",
    "CandidateMatch",
    "search_candidates",
]

#: Signal strength, strongest first. A canonical id is unambiguous by
#: construction; an exact display name is nearly so; the alias family is
#: deliberate human vocabulary the source system recorded; a fuzzy label is a
#: guess, and the frozen contract already refuses to let one carry a
#: commitment on its own.
#:
#: Ordered explicitly rather than by enum declaration order, because the
#: ranking is a product decision and reordering the enum for an unrelated
#: reason must not silently re-rank subjects.
SIGNAL_RANK: Mapping[SubjectMatchSignal, int] = {
    SubjectMatchSignal.EXACT_CANONICAL_ID: 0,
    SubjectMatchSignal.EXACT_DISPLAY_NAME: 1,
    SubjectMatchSignal.PROVIDER_IDENTIFIER: 2,
    SubjectMatchSignal.ACRONYM: 3,
    SubjectMatchSignal.ALIAS: 4,
    SubjectMatchSignal.PREVIOUS_NAME: 5,
    # Neither of these can be produced by THIS capability -- both need
    # conversation or surface context the search does not receive, and
    # ``CONVERSATIONAL_REFERENCE`` is inherently semantic so the emission
    # guard refuses it under the hash embedder anyway. They are ranked
    # regardless, because a signal missing from this map would sort by
    # whatever ``min`` happened to see first the day someone produced it, and
    # that is a re-ranking nobody would notice.
    SubjectMatchSignal.SURFACE_CONTEXT_REFERENCE: 6,
    SubjectMatchSignal.CONVERSATIONAL_REFERENCE: 7,
    SubjectMatchSignal.FUZZY_LABEL: 8,
}

#: Which mechanism produced each signal. Every one of these is a lookup over
#: stored text; none is retrieval. The map exists so the mechanism travels
#: with the finding and can be checked at emission rather than assumed.
_SIGNAL_MECHANISM: Mapping[SubjectMatchSignal, MatchMechanism] = {
    SubjectMatchSignal.EXACT_CANONICAL_ID: MatchMechanism.EXACT_LOOKUP,
    SubjectMatchSignal.EXACT_DISPLAY_NAME: MatchMechanism.EXACT_LOOKUP,
    SubjectMatchSignal.PROVIDER_IDENTIFIER: MatchMechanism.ALIAS_LOOKUP,
    SubjectMatchSignal.ACRONYM: MatchMechanism.ALIAS_LOOKUP,
    SubjectMatchSignal.ALIAS: MatchMechanism.ALIAS_LOOKUP,
    SubjectMatchSignal.PREVIOUS_NAME: MatchMechanism.ALIAS_LOOKUP,
    SubjectMatchSignal.FUZZY_LABEL: MatchMechanism.LEXICAL_FUZZY,
}

_PUNCTUATION = re.compile(r"[^\w\s]+")
_WHITESPACE = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class CandidateMatch:
    """One authorized subject the query matched, and how.

    ``matched_text`` is the *stored* text that matched, not the query. A
    packet reader needs to see what the arm recognised — "the auth work" —
    rather than what was typed, which they already have.
    """

    canonical_id: str
    kind: GraphEntityKind
    display_label: str
    signal: SubjectMatchSignal
    mechanism: MatchMechanism
    matched_text: str
    source_class: SourceClass

    @property
    def rank_key(self) -> tuple[int, str]:
        """Total order: signal strength, then canonical id.

        Deliberately no score float and no timestamp. A float invites ties
        broken by whatever ``sorted`` happened to see first, and a timestamp
        makes the ranking depend on ingestion rather than on the world.
        """

        return (SIGNAL_RANK[self.signal], self.canonical_id)


def _normalize(text: str) -> str:
    """Casefold, strip punctuation, collapse whitespace.

    Used for *comparison only* — the stored text is what a finding reports.
    NFKD first so that "Nightfall–Migration" (en dash) and
    "Nightfall-Migration" compare equal rather than producing a spurious
    near-miss a reader cannot see.
    """

    folded = unicodedata.normalize("NFKD", text).casefold()
    return _WHITESPACE.sub(" ", _PUNCTUATION.sub(" ", folded)).strip()


def _signal_for_alias(kind: object) -> SubjectMatchSignal:
    for alias_kind, signal in ALIAS_SIGNAL.items():
        if alias_kind is kind:
            return signal
    raise ValueError(f"alias kind {kind!r} has no match signal")


def search_candidates(
    query: str,
    nodes: Iterable[GraphNode],
    authorized_entity_ids: Sequence[str] | frozenset[str],
    *,
    limit: int = 25,
) -> tuple[tuple[CandidateMatch, ...], int]:
    """Ranked authorized candidates for ``query``, and how many were withheld.

    Returns ``(candidates, authorization_filtered_count)``. The count is of
    **distinct entities that matched and were not authorized** — a real
    disclosure, not a constant: it is what tells a packet reader that the
    answer they are looking at was narrowed, without telling them what was
    removed.

    ``limit`` bounds the returned list at the frozen contract's own candidate
    bound. Truncation is the caller's to disclose; this returns at most
    ``limit`` and the caller compares against what it asked for.
    """

    normalized_query = _normalize(query)
    if not normalized_query:
        return (), 0

    authorized = frozenset(authorized_entity_ids)
    matches: dict[str, CandidateMatch] = {}
    withheld: set[str] = set()

    for node in nodes:
        if node.entity_kind is None or node.entity_kind is GraphEntityKind.ORGANIZATION:
            continue

        found: list[tuple[SubjectMatchSignal, str]] = []
        if _normalize(node.canonical_id) == normalized_query:
            found.append((SubjectMatchSignal.EXACT_CANONICAL_ID, node.canonical_id))
        if _normalize(node.display_label) == normalized_query:
            found.append((SubjectMatchSignal.EXACT_DISPLAY_NAME, node.display_label))
        for alias in node.aliases:
            if _normalize(alias.value) == normalized_query:
                found.append((_signal_for_alias(alias.kind), alias.value))
        if not found and _is_lexical_near_match(normalized_query, node):
            found.append((SubjectMatchSignal.FUZZY_LABEL, node.display_label))

        if not found:
            continue
        if node.canonical_id not in authorized:
            # Withheld BEFORE ranking. A restricted entity that occupied a
            # rank -- or a clarification slot, or a truncation count -- would
            # be visible to the caller without ever being "returned".
            withheld.add(node.canonical_id)
            continue

        signal, matched_text = min(found, key=lambda item: SIGNAL_RANK[item[0]])
        matches[node.canonical_id] = CandidateMatch(
            canonical_id=node.canonical_id,
            kind=node.entity_kind,
            display_label=node.display_label,
            signal=signal,
            mechanism=_SIGNAL_MECHANISM[signal],
            matched_text=matched_text,
            source_class=node.source_class,
        )

    ranked = sorted(matches.values(), key=lambda item: item.rank_key)
    return tuple(ranked[:limit]), len(withheld)


def _is_lexical_near_match(normalized_query: str, node: GraphNode) -> bool:
    """Whole-token containment against the label and aliases. No vectors.

    Token-level rather than substring: substring matching makes "ACR" match
    "sacred", which is how a fuzzy match becomes a wrong-but-confident
    subject. Requiring the query's tokens to appear as tokens keeps
    "the auth work" matching "auth work hardening" while refusing accidental
    infixes.

    The bar is deliberately conservative. ``FUZZY_LABEL`` is the one signal
    the frozen contract refuses to let carry a commitment by itself, so a
    generous fuzzy matcher does not buy recall — it buys clarification
    prompts and unexaminable rankings.
    """

    query_tokens = set(normalized_query.split())
    if not query_tokens:
        return False
    haystacks = [_normalize(node.display_label)]
    haystacks.extend(_normalize(alias.value) for alias in node.aliases)
    return any(query_tokens <= set(text.split()) for text in haystacks)
