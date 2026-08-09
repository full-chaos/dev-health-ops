"""The three subject-resolution legs, expressed over one common result shape.

Every leg answers the same question — *given this question string and this
principal's grant, which authorized canonical subjects does the arm rank, and
in what order* — and nothing else. Packet assembly, driver synthesis and
evidence citation are deliberately out of scope: the review finding this
package answers is about **retrieval**, and a leg that also rebuilt packets
would mix a retrieval delta with a synthesis delta and be unable to say which
moved.

The legs:

``DETERMINISTIC_MENTIONS``
    The pinned CHAOS-3619 path, unchanged: production mention extraction,
    then :func:`discovery.search_candidates` over stored text. Re-run here
    rather than read from the baseline file so the comparison is same-process
    and same-world; ``test_chaos_3647_baseline_reproduction`` asserts it
    agrees with the pinned artifact, which is what makes re-running safe.

``DETERMINISTIC_QUESTION``
    The same matcher, handed the raw question. Exists to separate an
    extraction gap from a matching gap. Whole-token containment against a
    whole sentence matches almost nothing, and that near-zero result is the
    control that makes the semantic leg's input fair rather than generous.

``SEMANTIC_HYBRID``
    Graphiti's ``node_fulltext_search`` and ``node_similarity_search`` over
    the live store, fused with Graphiti's own ``rrf``, with the query
    embedded by the same model that wrote the node vectors.

**No leg is handed the answer.** None of these functions takes a case id, a
seed, an expected subject or an oracle — the same structural fairness rule
``trials.chaos_3619.graph_leg`` enforces, and for the same reason: a
parameter that could carry the answer is a parameter someone eventually
passes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from dev_health_ops.context_fabric.graph_arm.discovery import search_candidates
from dev_health_ops.context_fabric.graph_arm.semantic_retrieval import (
    retrieve_candidates,
)
from trials.chaos_3619.graph_leg import mention_texts

if TYPE_CHECKING:  # pragma: no cover - typing only
    from dev_health_ops.context_fabric.graph_arm.projection import GraphProjection
    from dev_health_ops.context_fabric.graph_arm.semantic_retrieval import (
        RetrievalStore,
    )

__all__ = [
    "LEG_LIMIT",
    "LegId",
    "RankedSubject",
    "LegResolution",
    "prose_disclosures_in",
    "resolve_deterministic",
    "resolve_semantic",
]

#: The candidate horizon every leg is measured over. One number, shared, so
#: a leg cannot look better by having been asked for more.
LEG_LIMIT = 25


class LegId(StrEnum):
    """Which configuration produced a resolution."""

    DETERMINISTIC_MENTIONS = "deterministic_mentions"
    DETERMINISTIC_QUESTION = "deterministic_question"
    SEMANTIC_HYBRID = "semantic_hybrid"


@dataclass(frozen=True, slots=True)
class RankedSubject:
    """One authorized canonical subject a leg ranked, and how it found it."""

    canonical_id: str
    display_label: str
    rank: int
    #: The frozen contract's ``SubjectMatchSignal`` value.
    signal: str
    #: The arm-internal ``MatchMechanism`` value — how, as opposed to what.
    #: This is the field that separates "resolved by alias registry" from
    #: "resolved by nearest neighbour", which score identically and differ
    #: completely in what they prove.
    mechanism: str
    #: Which retrieval primitives surfaced it. Empty for the deterministic
    #: legs, which run no primitive.
    methods: tuple[str, ...] = ()
    #: RRF score, semantic leg only. Meaningful only within one fusion.
    rrf_score: float | None = None


@dataclass(frozen=True, slots=True)
class LegResolution:
    """What one leg resolved for one question."""

    leg: LegId
    query: str
    subjects: tuple[RankedSubject, ...]
    authorization_filtered_count: int
    #: Trial diagnostic. See ``semantic_retrieval._canonical_ids``: a bare
    #: count cannot distinguish "authorization removed a restricted entity"
    #: from "retrieval never surfaced one", and that distinction is the
    #: authorization measurement.
    withheld_canonical_ids: tuple[str, ...] = ()
    #: Per-primitive order before authorization, semantic leg only.
    bm25_order: tuple[str, ...] = ()
    cosine_order: tuple[str, ...] = ()
    #: Observation nodes retrieval returned. Not subjects — the deterministic
    #: leg excludes them too — but recorded, because a leg whose top hits are
    #: all observations has found something and dropped it, which is a
    #: different finding from having found nothing.
    observation_hits: tuple[str, ...] = ()
    #: What the production interpreter extracted, deterministic-mentions leg
    #: only. Recorded because an empty tuple here is the single most
    #: load-bearing fact in the whole comparison.
    mentions: tuple[str, ...] = ()

    @property
    def resolved(self) -> bool:
        return bool(self.subjects)

    @property
    def top_1(self) -> str:
        return self.subjects[0].canonical_id if self.subjects else ""

    def top_n(self, count: int) -> tuple[str, ...]:
        return tuple(subject.canonical_id for subject in self.subjects[:count])


def prose_disclosures_in(
    resolution: LegResolution, principal_id: str
) -> tuple[str, ...]:
    """CHAOS-3635 oracle v2, applied to what this leg would hand a consumer.

    The leg builds no packet, so ``audit_authorization`` has nothing to walk.
    What it *does* produce is the prose a packet's subject-discovery section
    is built from: every ranked subject carries a ``display_label``, and the
    contract copies that into ``candidates[].display_label`` and
    ``match_signals[].matched_text``. Those are exactly the channels
    CHAOS-3635 exists for, and a canonical-id check is blind to them — a leg
    that never names ``proj_quarry`` but ranks something labelled "Quarry
    Compliance" has disclosed it.

    Deliberately scans the ranked output and NOT the diagnostic fields. A
    retrieval record's ``withheld_canonical_ids`` and per-primitive orders
    hold restricted ids on purpose: they are the evidence the filter fired,
    they never reach a consumer, and reporting them here would make the check
    fail on precisely the runs where authorization worked.
    """

    from dev_health_ops.api.dev.investigation_corpus.authorization import (
        prose_sightings_in_text,
    )

    rendered = "\n".join(
        f"{subject.canonical_id}\n{subject.display_label}\n{subject.signal}"
        for subject in resolution.subjects
    )
    return tuple(
        f"{sighting.channel}:{sighting.token}"
        for sighting in prose_sightings_in_text(
            rendered, principal_id, include_evidence_slugs=False
        )
    )


def resolve_deterministic(
    *,
    question: str,
    projection: GraphProjection,
    authorized_entity_ids: frozenset[str],
    over_mentions: bool,
    limit: int = LEG_LIMIT,
) -> LegResolution:
    """Stored-text resolution, over extracted mentions or the raw question.

    The withheld count is the **maximum** across queries rather than the sum,
    matching ``graph_leg.discover_subjects`` exactly. ``search_candidates``
    returns a count and not identities, so summing double-counts one
    restricted entity that matched two mentions and overstates how much the
    answer was narrowed. Reproducing the baseline's arithmetic matters more
    here than improving on it: a different rule would make the pinned
    comparison invalid for a reason unrelated to retrieval.
    """

    mentions = mention_texts(question) if over_mentions else ()
    queries = mentions if over_mentions else (question,)

    merged: dict[str, Any] = {}
    filtered_counts: list[int] = []
    for text in queries:
        candidates, filtered = search_candidates(
            text, projection.nodes, authorized_entity_ids, limit=limit
        )
        for match in candidates:
            existing = merged.get(match.canonical_id)
            if existing is None or match.rank_key < existing.rank_key:
                merged[match.canonical_id] = match
        if filtered:
            filtered_counts.append(filtered)

    ranked = sorted(merged.values(), key=lambda item: item.rank_key)[:limit]
    return LegResolution(
        leg=(
            LegId.DETERMINISTIC_MENTIONS
            if over_mentions
            else LegId.DETERMINISTIC_QUESTION
        ),
        query=question,
        subjects=tuple(
            RankedSubject(
                canonical_id=match.canonical_id,
                display_label=match.display_label,
                rank=index,
                signal=match.signal.value,
                mechanism=match.mechanism.value,
            )
            for index, match in enumerate(ranked)
        ),
        authorization_filtered_count=max(filtered_counts, default=0),
        mentions=mentions,
    )


async def resolve_semantic(
    *,
    question: str,
    store: RetrievalStore,
    authorized_entity_ids: frozenset[str],
    limit: int = LEG_LIMIT,
) -> LegResolution:
    """Hybrid BM25 + cosine resolution over the live store.

    Handed the raw question rather than extracted mentions on purpose. A
    retriever's input is a query, and feeding it the deterministic leg's
    empty mention tuple would guarantee an empty result and measure nothing.
    The cost of that choice is that the two legs no longer share an input,
    which is exactly what ``DETERMINISTIC_QUESTION`` exists to price.
    """

    retrieval = await retrieve_candidates(
        question,
        store=store,
        authorized_entity_ids=authorized_entity_ids,
        limit=limit,
    )
    return LegResolution(
        leg=LegId.SEMANTIC_HYBRID,
        query=question,
        subjects=tuple(
            RankedSubject(
                canonical_id=candidate.canonical_id,
                display_label=candidate.display_label,
                rank=candidate.rank,
                signal=candidate.signal.value,
                mechanism=candidate.mechanism.value,
                methods=tuple(sorted(method.value for method in candidate.methods)),
                rrf_score=candidate.rrf_score,
            )
            for candidate in retrieval.candidates
        ),
        authorization_filtered_count=retrieval.authorization_filtered_count,
        withheld_canonical_ids=retrieval.withheld_canonical_ids,
        bm25_order=retrieval.bm25_order,
        cosine_order=retrieval.cosine_order,
        observation_hits=retrieval.observation_hits,
    )
