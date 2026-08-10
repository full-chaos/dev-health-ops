"""CHAOS-3678: EXACT-only live subject resolution for production.

The trial's :func:`~.discovery.search_candidates` resolves a reference
against canonical id, display name, every alias kind (acronym/previous-name/
provider-identifier) AND a token-containment fuzzy fallback -- and it
operates over an in-memory :class:`~.projection.GraphProjection` built from a
fixture corpus, never the live store.

Neither shape fits this first production slice. Per team-lead's ruling on
CHAOS-3678 (binding, cited as "§4"): production's COMPLETED path resolves a
mention on ``EXACT_CANONICAL_ID`` or ``EXACT_DISPLAY_NAME`` only -- no alias,
no acronym, no previous-name, and emphatically no fuzzy widening. A mention
that only fuzzy- or alias-matches gets nothing here, and the caller
(:mod:`.query_service`) is expected to carry that through to an honest
refusal/degradation outcome rather than ever guessing. Widening this to the
trial's full signal set is separate, explicitly-scoped follow-up work, not a
quietly-expanding default.

Reuses :data:`~.readback._ENTITY_QUERY` and :func:`~.readback._rows`
verbatim -- the same partition-scoped, unconditional entity fetch
:class:`~.readback.LiveGraphReader` already runs for every traversal, so this
module adds no new Cypher and no new live-store surface, only exact matching
and authorization filtering over rows that query already proves correct.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Sequence
from re import compile as _compile
from typing import Any

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import SubjectMatchSignal

from .backend import MatchMechanism
from .discovery import CandidateMatch
from .readback import _ENTITY_QUERY, _rows
from .vocabulary import GraphEntityKind

#: Deliberately empty: this module's one function is package-private (see
#: its own docstring on why it takes a raw ``partition`` -- CHAOS-3617's
#: "no caller-supplied partition parameter" guard scans public callables
#: only, and a raw partition parameter is exactly the shape that guard
#: exists to catch).
__all__: list[str] = []

#: Both signals this module can produce are unambiguous lookups over stored
#: text, never retrieval -- mirrors ``discovery._SIGNAL_MECHANISM`` for the
#: same two entries.
_SIGNAL_MECHANISM = {
    SubjectMatchSignal.EXACT_CANONICAL_ID: MatchMechanism.EXACT_LOOKUP,
    SubjectMatchSignal.EXACT_DISPLAY_NAME: MatchMechanism.EXACT_LOOKUP,
}

_PUNCTUATION = _compile(r"[^\w\s]+")
_WHITESPACE = _compile(r"\s+")


def _normalize(text: str) -> str:
    """Casefold, strip punctuation, collapse whitespace -- comparison only.

    Identical normalization to ``discovery._normalize``, duplicated rather
    than imported: it is three lines, and importing a private helper across
    a semantic boundary (fuzzy-capable vs. exact-only) invites the two to
    drift apart silently later. The behavior staying identical today is
    covered by ``test_case_and_punctuation_insensitive_but_still_exact``.
    """

    folded = unicodedata.normalize("NFKD", text).casefold()
    return _WHITESPACE.sub(" ", _PUNCTUATION.sub(" ", folded)).strip()


async def _resolve_exact_subjects(
    driver: Any,
    partition: str,
    queries: Sequence[str],
    authorized_entity_ids: Sequence[str] | frozenset[str],
) -> tuple[CandidateMatch, ...]:
    """Exact canonical-id or display-label matches only, authorized only.

    Package-private (leading underscore) rather than exported: it takes a
    raw ``partition`` directly, which CHAOS-3617's "no caller-supplied
    partition parameter" guard (``test_chaos_3617_no_caller_supplied_
    partition.py``) forbids on any PUBLIC callable in this package -- the
    server derives the partition, a caller must never be able to name one.
    ``query_service.py`` (same package) is this function's only caller and
    derives ``partition`` from the store it already holds, never from a
    caller-supplied value.

    ``queries`` is normally the caller's mention texts. Every query is
    checked against every entity; a query matching nothing, or matching only
    an unauthorized entity, contributes no result -- there is no partial
    credit and no fallback signal weaker than the two this module supports.

    Order among results (when more than one query matches) is NOT specified
    beyond entity iteration order: callers with exactly one query -- this
    slice's only caller -- do not need one, and inventing an ordering
    contract for a first slice that has no multi-query caller yet would be
    a commitment nothing here can honor. ``discovery.CandidateMatch`` is
    reused directly (not a parallel type) so its already-tested
    ``rank_key`` is available the moment a caller needs to rank multiple
    queries' worth of results.
    """

    normalized_queries = {_normalize(query) for query in queries if _normalize(query)}
    if not normalized_queries:
        return ()

    authorized = frozenset(authorized_entity_ids)
    matches: list[CandidateMatch] = []
    for record in await _rows(driver, _ENTITY_QUERY, partition=partition):
        kind_value = record.get("entity_kind")
        if kind_value is None:
            continue
        kind = GraphEntityKind(kind_value)
        if kind is GraphEntityKind.ORGANIZATION:
            continue

        canonical_id = record["canonical_id"]
        display_label = record["display_label"]
        if _normalize(canonical_id) in normalized_queries:
            signal = SubjectMatchSignal.EXACT_CANONICAL_ID
            matched_text = canonical_id
        elif _normalize(display_label) in normalized_queries:
            signal = SubjectMatchSignal.EXACT_DISPLAY_NAME
            matched_text = display_label
        else:
            continue

        if canonical_id not in authorized:
            # Withheld before it is ever returned -- mirrors
            # discovery.search_candidates's own "authorization applied
            # before ranking" rule. Not counted here: this slice's one
            # caller (query_service, SEEDED_SINGULAR_SUBJECT) has exactly
            # one mention, so an authorization-filtered count belongs on
            # its own SubjectDiscovery section, not invented in this
            # function ahead of a caller that needs it.
            continue

        matches.append(
            CandidateMatch(
                canonical_id=canonical_id,
                kind=kind,
                display_label=display_label,
                signal=signal,
                mechanism=_SIGNAL_MECHANISM[signal],
                matched_text=matched_text,
                source_class=SourceClass(record["source_class"]),
            )
        )

    return tuple(matches)
