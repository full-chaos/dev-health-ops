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

Reuses :data:`~.readback._ENTITY_QUERY`/:data:`~.readback._EDGE_QUERY` and
:func:`~.readback._rows` verbatim -- the same partition-scoped, unconditional
entity/edge fetch :class:`~.readback.LiveGraphReader` already runs for every
traversal, so this module adds no new Cypher and no new live-store surface,
only interpretation over rows those queries already prove correct.

CHAOS-3688 adds two more live-data helpers for :func:`~.cohort.build_cohort`
(the ``SEEDED_EXPLICIT_COHORT`` mechanism): an edge fetch shaped to
``cohort.CohortEdgeLike`` and an ``entity_labels`` map, both derived from the
SAME live queries this module already runs -- not a second live-store
integration, an extension of the one this module is.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass
from re import compile as _compile
from typing import Any

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import (
    RelationshipType,
    SubjectMatchSignal,
)

from .backend import MatchMechanism, parse_triple_fact
from .discovery import CandidateMatch
from .readback import _EDGE_QUERY, _ENTITY_QUERY, _rows
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


@dataclass(frozen=True, slots=True)
class _LiveEntity:
    """One partition entity, as fetched by :func:`_live_entities`.

    Deliberately narrower than :class:`~.readback.DiscoveredEntity`: this
    carries nothing about traversal (no ``alias_values``, no
    ``attributes``) -- both this module's callers only ever read identity
    and classification.
    """

    canonical_id: str
    kind: GraphEntityKind
    display_label: str
    source_class: SourceClass


async def _live_entities(driver: Any, partition: str) -> tuple[_LiveEntity, ...]:
    """Every entity in the partition, unconditionally -- shared by
    :func:`_resolve_exact_subjects` (search target) and
    :func:`_live_entity_labels` (``build_cohort``'s label lookup). One
    fetch, two interpretations, rather than two separate live-store reads
    of the same query.
    """

    entities: list[_LiveEntity] = []
    for record in await _rows(driver, _ENTITY_QUERY, partition=partition):
        kind_value = record.get("entity_kind")
        if kind_value is None:
            continue
        entities.append(
            _LiveEntity(
                canonical_id=record["canonical_id"],
                kind=GraphEntityKind(kind_value),
                display_label=record["display_label"],
                source_class=SourceClass(record["source_class"]),
            )
        )
    return tuple(entities)


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

    ``queries`` is normally the caller's mention texts -- one per resolved
    seed, so ``SEEDED_EXPLICIT_COHORT`` (CHAOS-3688) passes every mention's
    text in one call rather than one call per mention. Every query is
    checked against every entity; a query matching nothing, or matching only
    an unauthorized entity, contributes no result -- there is no partial
    credit and no fallback signal weaker than the two this module supports.

    Order among results (when more than one query matches) is NOT specified
    beyond entity iteration order -- ordering results to match caller intent
    (e.g. "the Nth match corresponds to the Nth mention") is the caller's
    job, since two different mentions can resolve to the same entity or one
    mention's text can match nothing at all. ``discovery.CandidateMatch`` is
    reused directly (not a parallel type) so its already-tested
    ``rank_key`` is available the moment a caller needs to rank multiple
    queries' worth of results.
    """

    normalized_queries = {_normalize(query) for query in queries if _normalize(query)}
    if not normalized_queries:
        return ()

    authorized = frozenset(authorized_entity_ids)
    matches: list[CandidateMatch] = []
    for entity in await _live_entities(driver, partition):
        if entity.kind is GraphEntityKind.ORGANIZATION:
            continue

        if _normalize(entity.canonical_id) in normalized_queries:
            signal = SubjectMatchSignal.EXACT_CANONICAL_ID
            matched_text = entity.canonical_id
        elif _normalize(entity.display_label) in normalized_queries:
            signal = SubjectMatchSignal.EXACT_DISPLAY_NAME
            matched_text = entity.display_label
        else:
            continue

        if entity.canonical_id not in authorized:
            # Withheld before it is ever returned -- mirrors
            # discovery.search_candidates's own "authorization applied
            # before ranking" rule. Not counted here: an
            # authorization-filtered count belongs on the caller's own
            # SubjectDiscovery section, not invented ahead of a caller
            # that needs it.
            continue

        matches.append(
            CandidateMatch(
                canonical_id=entity.canonical_id,
                kind=entity.kind,
                display_label=entity.display_label,
                signal=signal,
                mechanism=_SIGNAL_MECHANISM[signal],
                matched_text=matched_text,
                source_class=entity.source_class,
            )
        )

    return tuple(matches)


def _live_entity_labels(
    entities: Sequence[_LiveEntity],
) -> dict[str, tuple[GraphEntityKind, str]]:
    """The ``canonical_id -> (kind, label)`` map :func:`~.cohort.build_cohort`
    takes. Excludes ``ORGANIZATION`` -- mirrors the trial's own
    ``graph_leg._entity_labels``: the partition root is an entity node with
    no emittable subject kind, and a cohort that could contain it would be
    a cohort containing the tenant.
    """

    return {
        entity.canonical_id: (entity.kind, entity.display_label)
        for entity in entities
        if entity.kind is not GraphEntityKind.ORGANIZATION
    }


@dataclass(frozen=True, slots=True)
class _CohortEdge:
    """One partition edge, shaped to ``cohort.CohortEdgeLike`` -- the three
    fields :func:`~.cohort.build_cohort` reads and nothing else. Never a
    ``projection.GraphEdge`` with placeholder values for the fields this
    has no live-store-cheap source for (uuid, source/target uuid and kind):
    fabricating those would be more dishonest than a narrower type.
    """

    relationship: RelationshipType
    source_canonical_id: str
    target_canonical_id: str


async def _live_cohort_edges(driver: Any, partition: str) -> tuple[_CohortEdge, ...]:
    """Every edge in the partition, unconditionally -- the same
    unconditional, partition-scoped fetch :class:`~.readback.LiveGraphReader`
    already runs for every traversal (:data:`~.readback._EDGE_QUERY`), reused
    verbatim. ``build_cohort`` walks two hops from an arbitrary subject, so
    it needs the full adjacency, not a traversal-bounded subset.
    """

    edges: list[_CohortEdge] = []
    for record in await _rows(driver, _EDGE_QUERY, partition=partition):
        source_id, relationship, target_id = parse_triple_fact(record["fact"])
        edges.append(
            _CohortEdge(
                relationship=relationship,
                source_canonical_id=source_id,
                target_canonical_id=target_id,
            )
        )
    return tuple(edges)
