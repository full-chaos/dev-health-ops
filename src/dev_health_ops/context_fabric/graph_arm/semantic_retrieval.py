"""CHAOS-3647: Graphiti's real semantic/hybrid retrieval, as a second leg.

The CHAOS-3619 trial resolved subjects with :func:`discovery.search_candidates`
— exact canonical id, exact display name, the alias family, and whole-token
containment — over a store written by
:class:`~.backend.DeterministicEmbedder`. That measured graph traversal and
structured alias association, and it could not measure retrieval: BLAKE2b
vectors carry no similarity, so nearest-neighbour search over them is
arbitrary. Independent review said so, and this module is the answer.

**What is different here, and it is the only thing that is different.** The
projection, the partition, the authorization set, the corpus and the
questions are all unchanged. The one variable is *how a human reference
becomes a ranked canonical subject*: stored-text lookup on the deterministic
leg, Graphiti's own BM25 and cosine primitives over live vectors here. The
deterministic run stays the pinned baseline; nothing in this module edits it
or is reachable from it.

**A non-semantic embedder is refused, loudly.** :class:`NonSemanticEmbedderError`
is raised before any query runs. A leg that silently accepted
``DeterministicEmbedder`` would produce a full set of records under a
"semantic" heading whose every number came from hash vectors — the
deterministic baseline wearing a costume, and the single most expensive
mistake this module could make. The check is on ``embedder.semantic``, which
:class:`~.backend.CloudEmbedder` keys on the API key rather than on the class,
so an unusable instance is refused too.

**A hybrid leg whose BM25 half is dark must fail, not quietly become a
cosine leg.** FalkorDB's full-text index populates after the bulk write, and
a recorded run of this trial queried it too early: ``bm25_order`` was empty
on all eight cases, the leg was cosine-only under a heading that said hybrid,
and nothing failed. :func:`wait_for_fulltext_index` is the positive control
that closes that, and it raises rather than warning.

**Per-candidate mechanism is derived from what actually surfaced it.**
Hybrid retrieval fuses two result sets, and a node that only BM25 found was
found lexically. Labelling it ``EMBEDDING_SIMILARITY`` because the *leg* is
called semantic would be exactly the confusion
:class:`~.backend.MatchMechanism` exists to prevent. So the two primitives
are called separately, their per-method membership is kept, and the fusion is
Graphiti's own :func:`rrf`. A reader of the artifact can therefore ask "would
BM25 alone have found this?" and get an answer, rather than a claim.

**Authorization is applied before ranking**, identically to
:func:`discovery.search_candidates` and for the same reason: a restricted
entity that occupies a rank, a clarification slot or a truncation count is
visible to a caller who was never allowed to see it. Retrieval makes this
sharper, not softer — a vector search does not know what a grant is, and will
happily return the restricted project inside the caller's own tenant.

**The partition is never a parameter, and is asserted on the way out.**
:func:`retrieve_candidates` takes a :class:`RetrievalStore` — the read-only
three-member view a :class:`~.store.GraphArmStore` satisfies — rather than a
driver and a partition string. That is not ergonomics: a partition
parameter is exactly what ``test_chaos_3617_no_caller_supplied_partition``
forbids, and the first version of this module had one. Taking the store
keeps the partition server-derived — it is the one :meth:`for_org`
constructed from the organization id — and closes a second hole the two-
argument form opened, where a caller could pass one organization's driver
with another's partition, or a query vector from a model that did not write
the store's vectors.

``group_ids`` is then passed to both primitives, and every returned node's
``group_id`` is re-checked here. The corpus plants a same-named project in a
different tenant (``lumen_proj_acr``, label "Agent Context Runtime",
identical to ``proj_acr``), which is precisely the shape a similarity search
would cross partitions on. A filter that is only an argument is a filter
nobody has measured.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import SubjectMatchSignal

from .backend import (
    OBSERVATION_SUBJECTS_ATTRIBUTE,
    EmbeddingBackend,
    MatchMechanism,
    graphiti_module,
)
from .discovery import CandidateMatch
from .readback import _entities_by_canonical_id
from .vocabulary import GraphEntityKind

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Sequence

__all__ = [
    "DEFAULT_CLARIFICATION_LIMIT",
    "DEFAULT_MARGIN_RATIO",
    "DEFAULT_SEMANTIC_LIMIT",
    "DEFAULT_TIE_RATIO",
    "ConversationalReferenceResult",
    "CrossPartitionRetrievalError",
    "FulltextIndexNotReadyError",
    "FulltextReadiness",
    "RetrievalMethod",
    "RetrievalStore",
    "SemanticCandidate",
    "SemanticDisposition",
    "SemanticDispositionResult",
    "SemanticRetrieval",
    "NonSemanticEmbedderError",
    "assess_disposition",
    "resolve_conversational_reference",
    "retrieve_candidates",
    "wait_for_fulltext_index",
]

#: How many candidates a semantic leg returns. Matches
#: ``discovery.search_candidates``' own default so the two legs are compared
#: over the same horizon rather than over different ones.
DEFAULT_SEMANTIC_LIMIT = 25

#: The node attribute carrying the canonical id. Written by
#: ``backend.to_graphiti_nodes``; read back rather than parsed out of the
#: node name, which is the *display label* and is not unique.
_CANONICAL_ID_ATTRIBUTE = "cf_canonical_id"
_ENTITY_KIND_ATTRIBUTE = "cf_entity_kind"
_SOURCE_CLASS_ATTRIBUTE = "cf_source_class"


@runtime_checkable
class RetrievalStore(Protocol):
    """Exactly what retrieval reads off a store, and nothing more.

    A protocol rather than :class:`~.store.GraphArmStore` itself because the
    three members below are the whole dependency, and naming them is what
    lets a reader see that this module cannot write, purge, or open a
    connection for an organization of its own choosing.

    The critical member is :attr:`partition`, and it is a **property of the
    object, never a parameter of the call**. That is the difference between
    this signature and the one
    ``test_chaos_3617_no_caller_supplied_partition`` rejected: a store's
    partition was derived by :meth:`GraphArmStore.for_org` from a
    server-known organization id, and there is no constructor a caller can
    reach that produces a store for a partition they named.
    """

    @property
    def driver(self) -> Any: ...

    @property
    def partition(self) -> str: ...

    @property
    def embedder(self) -> EmbeddingBackend: ...


class NonSemanticEmbedderError(RuntimeError):
    """A semantic retrieval leg was asked to run on meaningless vectors.

    Raised before any query is issued. See the module docstring: the whole
    value of this leg is that its numbers came from a real embedding model,
    and a run that quietly degraded to hash vectors would be indistinguishable
    from one that did not, in the artifact, forever.
    """


class CrossPartitionRetrievalError(RuntimeError):
    """A retrieved node belongs to a partition other than the queried one."""


class RetrievalMethod(StrEnum):
    """Which Graphiti primitive surfaced a candidate.

    Kept per candidate rather than per run. "The hybrid leg found it" is not
    a finding; "cosine found it and BM25 did not" is.
    """

    BM25 = "bm25"
    COSINE = "cosine_similarity"


#: Which mechanism a set of surfacing methods honestly supports. A node BM25
#: alone found was found by lexical overlap and nothing else, whatever the
#: leg is called; cosine involvement is what makes the claim semantic.
def _mechanism_for(methods: frozenset[RetrievalMethod]) -> MatchMechanism:
    if RetrievalMethod.COSINE in methods:
        return MatchMechanism.EMBEDDING_SIMILARITY
    return MatchMechanism.LEXICAL_FUZZY


@dataclass(frozen=True, slots=True)
class SemanticCandidate:
    """One authorized subject retrieval surfaced, and how it was surfaced.

    ``signal`` is :attr:`SubjectMatchSignal.FUZZY_LABEL` for every candidate
    here, and that is a measured statement rather than a placeholder. The
    frozen contract refuses to let ``FUZZY_LABEL`` carry a commitment on its
    own; retrieval over node names produces a ranked guess and nothing
    stronger, so any other signal would be a claim this leg cannot support.
    An alias hit that reaches the top of a similarity ranking is still a
    similarity hit — the registry is what makes it an alias, and this leg
    does not consult the registry.
    """

    canonical_id: str
    kind: GraphEntityKind
    display_label: str
    signal: SubjectMatchSignal
    mechanism: MatchMechanism
    matched_text: str
    source_class: SourceClass
    #: Every primitive that returned this node, so a reader can attribute the
    #: find to lexical overlap or to embedding similarity.
    methods: frozenset[RetrievalMethod]
    #: The fused Reciprocal Rank Fusion score. Recorded, never compared
    #: across queries: RRF scores are rank-derived and are only meaningful
    #: within one fusion.
    rrf_score: float
    #: Zero-based rank within this query's authorized result.
    rank: int
    #: CHAOS-3653: set when this candidate was not itself surfaced by BM25
    #: or cosine, but reached via an authorized observation's own
    #: ``cf_subject_canonical_ids`` -- retrieval found the right evidence,
    #: not the entity's own name. ``None`` for a direct hit. Provenance a
    #: caller needs: "inferred from an attached observation" and "matched
    #: the entity's own label" are different claims about the same rank.
    via_observation_id: str | None = None


@dataclass(frozen=True, slots=True)
class SemanticRetrieval:
    """What one semantic query returned, and what it was not allowed to."""

    query: str
    candidates: tuple[SemanticCandidate, ...]
    #: Distinct entities retrieval surfaced that the principal may not see.
    #: A real disclosure figure, counted before ranking, exactly as
    #: ``discovery.search_candidates`` counts it.
    authorization_filtered_count: int
    #: Which entities those were. A trial diagnostic, never packet content —
    #: see :func:`_canonical_ids`. Carried because "authorization removed a
    #: restricted project" and "retrieval never found one" are the two
    #: results this leg exists to distinguish, and a bare count cannot.
    withheld_canonical_ids: tuple[str, ...]
    #: Canonical ids each primitive returned before authorization, in the
    #: primitive's own order. Recorded because "authorization removed
    #: nothing" and "retrieval found nothing to remove" are different
    #: results that an empty candidate list renders identically.
    retrieved_before_authorization: tuple[str, ...]
    bm25_order: tuple[str, ...]
    cosine_order: tuple[str, ...]
    #: Non-entity nodes (observations) retrieval returned. Excluded from
    #: subject candidates — the deterministic leg excludes them too — but
    #: counted, because a retrieval leg whose top hits are all observations
    #: is a different finding from one that returned nothing.
    observation_hits: tuple[str, ...]

    @property
    def resolved(self) -> bool:
        return bool(self.candidates)


class SemanticDisposition(StrEnum):
    """What this leg is willing to do with its own ranked result.

    **Not** a contract-level ``SubjectCommitmentState``. Every candidate this
    leg produces carries :attr:`SubjectMatchSignal.FUZZY_LABEL` — see
    :class:`SemanticCandidate` — and ``WEAK_SUBJECT_MATCH_SIGNALS`` already
    forbids a candidate from reaching ``COMMITTED`` on that signal alone
    (``vocabulary.py:191-199``). This leg cannot commit a subject and was
    never going to; what varies here is how confidently its own ranking is
    worth handing to whatever resolves the subject: as a single lead, as a
    bounded set that needs disambiguating, or not at all.

    ``PROPOSE`` is therefore "worth proposing as the lead candidate", never
    "resolved". A caller that reads ``PROPOSE`` as commitment has made the
    same mistake ``WEAK_SUBJECT_MATCH_SIGNALS`` exists to catch.
    """

    PROPOSE = "propose"
    CLARIFY = "clarify"
    REFUSE = "refuse"


@dataclass(frozen=True, slots=True)
class SemanticDispositionResult:
    """The disposition this leg's own result earns, and what to show for it."""

    disposition: SemanticDisposition
    #: Bounded to ``clarification_limit`` on CLARIFY, one candidate on
    #: PROPOSE, empty on REFUSE. Never longer than what a caller may present.
    presented: tuple[SemanticCandidate, ...]
    #: Trial/operator diagnostic. Never packet content -- see
    #: :func:`_canonical_ids` for why a rationale channel here is not a wire
    #: field.
    reason: str


#: Rank-1 must beat rank-2 by at least this fraction of rank-1's own score to
#: read as a genuine leader rather than noise. Derived from Reciprocal Rank
#: Fusion's own decay curve -- ``score = sum(1 / (k + rank))`` per method,
#: which drops fastest between the first few ranks -- so a real front-runner
#: separates from its nearest competitor by a wide margin on that curve and a
#: coincidental one does not. The constant describes that curve's shape, not
#: any corpus's query text, and is validated in
#: ``test_chaos_3654_refusal_threshold.py`` against synthetic cases built to
#: exercise the shape rather than reproduce a measured question.
DEFAULT_MARGIN_RATIO = 0.20

#: A candidate scoring at or above this fraction of the leader's own score is
#: "tied" with it for disambiguation purposes -- close enough on the RRF
#: curve that picking one over the other is not defensible without more
#: information.
DEFAULT_TIE_RATIO = 0.85

#: How many tied candidates a CLARIFY disposition will present. Matches the
#: contract's own clarification-candidate bound in spirit: past a small
#: number, "clarify" has degraded into "here is the authorized world",
#: exactly what REFUSE exists to prevent.
DEFAULT_CLARIFICATION_LIMIT = 5


def _tied_candidates(
    candidates: Sequence[SemanticCandidate], *, tie_ratio: float
) -> list[SemanticCandidate]:
    """Every candidate within ``tie_ratio`` of the leader's own score.

    Always includes the leader itself (ratio 1.0 of itself). A guard against
    ``top_score <= 0`` falls back to exact equality, since a ratio against a
    non-positive score is not a meaningful fraction.
    """

    top_score = candidates[0].rrf_score
    if top_score <= 0:
        return [item for item in candidates if item.rrf_score == top_score]
    return [item for item in candidates if item.rrf_score >= tie_ratio * top_score]


def assess_disposition(
    retrieval: SemanticRetrieval,
    *,
    clarification_limit: int = DEFAULT_CLARIFICATION_LIMIT,
    margin_ratio: float = DEFAULT_MARGIN_RATIO,
    tie_ratio: float = DEFAULT_TIE_RATIO,
) -> SemanticDispositionResult:
    """Whether this leg's own ranking is safe to propose, clarify, or refuse.

    **The measured failure this closes.** "How is Halcyon doing?" — no such
    entity exists — returned 20 confidently ranked candidates, because
    cosine similarity against a real embedding model is rarely exactly zero
    for *any* query, and nothing in :func:`retrieve_candidates` ever refused
    on that basis. "What's holding it up?" with no conversational referent
    returned 11. Both runs had one thing in common: no lexical evidence
    anywhere in the corpus for the query text, and a leading candidate whose
    only support was a vector nearest-neighbour guess.

    **The rule is about the fused result's own shape, not about any query's
    content.** Three signals, checked in order:

    1. *Lexical grounding.* If ``retrieval.bm25_order`` is empty — BM25 found
       nothing for this query anywhere in the authorized partition — and the
       leading candidate has no BM25 support of its own, there is no
       evidence anyone ever wrote this term near a real node. That is
       exactly the shape a nonexistent entity or an orphan pronoun produces,
       and it refuses regardless of how confident the cosine ranking looks.
    2. *Rank-1/rank-2 margin.* A genuine front-runner separates from its
       nearest competitor on RRF's own score-decay curve; noise does not.
       ``DEFAULT_MARGIN_RATIO`` reads that separation, not any absolute
       score.
    3. *Tie count.* When several candidates cluster near the leader, the
       honest answer is "these look similar" (CLARIFY, bounded) rather than
       an arbitrary pick — and past ``clarification_limit`` tied candidates,
       clarification has degraded into an enumeration, which REFUSE exists
       to prevent.

    Authorization has already run inside :func:`retrieve_candidates`, so
    every candidate reaching this function is one the caller may see; this
    function only decides how much of that authorized ranking to trust.
    """

    if not retrieval.candidates:
        return SemanticDispositionResult(
            disposition=SemanticDisposition.REFUSE,
            presented=(),
            reason="no authorized candidate survived retrieval and fusion",
        )

    candidates = retrieval.candidates
    top = candidates[0]
    top_corroborated = RetrievalMethod.BM25 in top.methods
    lexically_grounded = bool(retrieval.bm25_order)

    if not lexically_grounded and not top_corroborated:
        return SemanticDispositionResult(
            disposition=SemanticDisposition.REFUSE,
            presented=(),
            reason=(
                "no lexical evidence for this query anywhere in the "
                "authorized partition, and the leading candidate has only "
                "vector-similarity support: the shape a nonexistent entity "
                "or an unresolved reference produces"
            ),
        )

    tied = _tied_candidates(candidates, tie_ratio=tie_ratio)

    if len(candidates) == 1:
        if top_corroborated:
            return SemanticDispositionResult(
                disposition=SemanticDisposition.PROPOSE,
                presented=(top,),
                reason="one authorized candidate, with lexical support",
            )
        return SemanticDispositionResult(
            disposition=SemanticDisposition.REFUSE,
            presented=(),
            reason=(
                "one authorized candidate with only vector-similarity "
                "support and no competing candidate to weigh it against"
            ),
        )

    second_score = candidates[1].rrf_score
    top_score = top.rrf_score
    margin = (top_score - second_score) / top_score if top_score > 0 else 0.0

    if top_corroborated and len(tied) == 1 and margin >= margin_ratio:
        return SemanticDispositionResult(
            disposition=SemanticDisposition.PROPOSE,
            presented=(top,),
            reason=(
                f"leader has lexical support and clears the next candidate "
                f"by {margin:.0%}, at or above the {margin_ratio:.0%} margin"
            ),
        )

    if len(tied) <= clarification_limit:
        return SemanticDispositionResult(
            disposition=SemanticDisposition.CLARIFY,
            presented=tuple(tied[:clarification_limit]),
            reason=(
                f"{len(tied)} candidate(s) within {tie_ratio:.0%} of the "
                "leader's own score; no defensible single pick"
            ),
        )

    return SemanticDispositionResult(
        disposition=SemanticDisposition.REFUSE,
        presented=(),
        reason=(
            f"{len(tied)} candidates cluster near the leader's own score, "
            f"past the {clarification_limit}-candidate clarification bound: "
            "a diffuse ranking with no defensible subject, not a genuine "
            "field of finalists"
        ),
    )


def _attribute(node: Any, key: str) -> Any:
    attributes = getattr(node, "attributes", None) or {}
    return attributes.get(key)


def _subject_canonical_ids(node: Any) -> tuple[str, ...]:
    """What an observation node declares itself to be about.

    Mirrors :func:`readback._rows`'s own parse of the same attribute
    exactly (comma-joined, empty items dropped) so the two readers of this
    property cannot silently disagree about what it means. A node that
    never carries the attribute at all -- a partition written before
    CHAOS-3619 (H3) added it -- returns ``()``: absence of the capability,
    not absence of subjects, and the hop below simply has nothing to do for
    it, which is the safe default.
    """

    raw = _attribute(node, OBSERVATION_SUBJECTS_ATTRIBUTE)
    if not isinstance(raw, str) or not raw:
        return ()
    return tuple(item for item in raw.split(",") if item)


@dataclass(frozen=True, slots=True)
class _HopSource:
    """The authorized observation that earned one subject its hop candidate."""

    observation_id: str
    matched_text: str
    score: float
    methods: frozenset[RetrievalMethod]


@dataclass(frozen=True, slots=True)
class FulltextReadiness:
    """Evidence that the BM25 half of a hybrid leg is actually live."""

    probe_query: str
    expected_canonical_id: str
    attempts_used: int
    ready: bool


class FulltextIndexNotReadyError(RuntimeError):
    """The full-text index never populated, so BM25 would measure nothing.

    Raised rather than warned, and this is the most important refusal in this
    module after :class:`NonSemanticEmbedderError`.

    **Observed, not hypothetical.** A recorded CHAOS-3647 run had
    ``bm25_order == []`` on all eight cases: FalkorDB's full-text index had
    not populated after the bulk write, so a leg labelled *hybrid* was
    cosine-only in every row and nothing failed. The delta classifications
    happened to survive; one case's rank-1 did not. A measurement that did
    not happen has to fail loudly, so this is the failure.
    """


async def wait_for_fulltext_index(
    store: RetrievalStore,
    *,
    probe_query: str,
    expected_canonical_id: str,
    attempts: int = 30,
    delay_seconds: float = 0.5,
) -> FulltextReadiness:
    """Block until BM25 can find a node the caller knows is there.

    The probe is a **positive control**: a query whose correct answer the
    caller already knows, checked for *that answer* rather than for a
    non-empty result. "The index returned something" is satisfied by a stale
    index left by a previous run; "the index returned the node I just wrote"
    is not.

    Raises :class:`FulltextIndexNotReadyError` on timeout. There is no warn
    mode and no falsy return for the failure case: a caller able to ignore
    this would produce exactly the run it exists to prevent.
    """

    import asyncio

    search_utils = graphiti_module("search.search_utils")
    empty_filter = graphiti_module("search.search_filters").SearchFilters()
    for attempt in range(1, attempts + 1):
        nodes = await search_utils.node_fulltext_search(
            store.driver, probe_query, empty_filter, [store.partition], 25
        )
        if any(
            _attribute(node, _CANONICAL_ID_ATTRIBUTE) == expected_canonical_id
            for node in nodes
        ):
            return FulltextReadiness(
                probe_query=probe_query,
                expected_canonical_id=expected_canonical_id,
                attempts_used=attempt,
                ready=True,
            )
        await asyncio.sleep(delay_seconds)

    raise FulltextIndexNotReadyError(
        f"after {attempts} attempts over {attempts * delay_seconds:.1f}s, "
        f"full-text search for {probe_query!r} in partition "
        f"{store.partition!r} never returned {expected_canonical_id!r}. The "
        "BM25 half of a hybrid leg would contribute nothing, and the run "
        "would record a cosine-only result under a hybrid heading"
    )


async def retrieve_candidates(
    query: str,
    *,
    store: RetrievalStore,
    authorized_entity_ids: Sequence[str] | frozenset[str],
    limit: int = DEFAULT_SEMANTIC_LIMIT,
    min_score: float | None = None,
) -> SemanticRetrieval:
    """Ranked authorized subjects for ``query``, via BM25 + cosine + RRF.

    Everything about *where* and *with what* comes from ``store``: its
    driver, its server-derived partition, and the embedder it was opened
    with. The query is therefore embedded by the same model that wrote the
    node vectors, by construction rather than by discipline — comparing a
    query vector from one model against node vectors from another produces a
    confident ordering of nothing at all, and no caller could notice.
    """

    driver = store.driver
    embedder = store.embedder
    partition = store.partition

    if not embedder.semantic:
        raise NonSemanticEmbedderError(
            f"embedder {embedder.model_id!r} reports semantic=False, so "
            "nearest-neighbour search over the vectors it wrote is "
            "meaningless. Refusing to run a semantic retrieval leg on it: "
            "the resulting records would be indistinguishable from a real "
            "semantic run and would be wrong in every row"
        )

    search_utils = graphiti_module("search.search_utils")
    search_filters = graphiti_module("search.search_filters")
    empty_filter = search_filters.SearchFilters()
    group_ids = [partition]
    # Both primitives are asked for a wider slice than the leg returns, so
    # authorization filtering narrows a real candidate set rather than
    # silently competing with the limit for the same slots.
    candidate_limit = max(limit * 2, limit)

    bm25_nodes = await search_utils.node_fulltext_search(
        driver, query, empty_filter, group_ids, candidate_limit
    )
    query_vector = await embedder.create(query.replace("\n", " "))
    cosine_kwargs: dict[str, Any] = {}
    if min_score is not None:
        cosine_kwargs["min_score"] = min_score
    cosine_nodes = await search_utils.node_similarity_search(
        driver,
        query_vector,
        empty_filter,
        group_ids,
        candidate_limit,
        **cosine_kwargs,
    )

    by_uuid: dict[str, Any] = {}
    methods: dict[str, set[RetrievalMethod]] = {}
    for method, nodes in (
        (RetrievalMethod.BM25, bm25_nodes),
        (RetrievalMethod.COSINE, cosine_nodes),
    ):
        for node in nodes:
            # Asserted on the way OUT, not only passed on the way in. See the
            # module docstring: the corpus plants an identically labelled
            # project in another tenant, and a group filter that is only an
            # argument is one nobody has measured.
            if node.group_id != partition:
                raise CrossPartitionRetrievalError(
                    f"retrieval for partition {partition!r} returned node "
                    f"{node.uuid!r} from partition {node.group_id!r}; the "
                    "group filter did not hold and this result set is not "
                    "safe to rank"
                )
            by_uuid[node.uuid] = node
            methods.setdefault(node.uuid, set()).add(method)

    fused_uuids, fused_scores = search_utils.rrf(
        [
            [node.uuid for node in bm25_nodes],
            [node.uuid for node in cosine_nodes],
        ]
    )
    score_by_uuid = dict(zip(fused_uuids, fused_scores, strict=True))

    retrieved: list[str] = []
    observations: list[str] = []
    withheld: dict[str, None] = {}
    authorized = frozenset(authorized_entity_ids)
    candidates: list[SemanticCandidate] = []
    # CHAOS-3653: the best authorized observation found for each subject a
    # retrieved observation named. "Best" is the observation with the
    # higher fused score, so a subject named by several observations hops
    # via the one retrieval trusted most, not an arbitrary one.
    hop_sources: dict[str, _HopSource] = {}

    for uuid in fused_uuids:
        node = by_uuid[uuid]
        canonical_id = _attribute(node, _CANONICAL_ID_ATTRIBUTE)
        if not isinstance(canonical_id, str) or not canonical_id:
            # A node without the arm's own canonical-id attribute did not
            # come from this projection. Skipped rather than guessed at:
            # ranking a node whose identity is unknown is how a retrieval
            # leg reports something it cannot name.
            continue
        kind_value = _attribute(node, _ENTITY_KIND_ATTRIBUTE)
        if kind_value is None:
            observations.append(canonical_id)
            score = float(score_by_uuid[uuid])
            for subject_id in _subject_canonical_ids(node):
                if subject_id not in authorized:
                    # Same disclosure as a direct hit: an unauthorized
                    # subject named by an observation is withheld, not
                    # silently absent -- the count is real either way.
                    withheld.setdefault(subject_id, None)
                    continue
                existing = hop_sources.get(subject_id)
                if existing is None or score > existing.score:
                    hop_sources[subject_id] = _HopSource(
                        observation_id=canonical_id,
                        matched_text=node.name,
                        score=score,
                        methods=frozenset(methods[uuid]),
                    )
            continue
        kind = GraphEntityKind(kind_value)
        if kind is GraphEntityKind.ORGANIZATION:
            # Excluded on the deterministic leg too: the organization is
            # never a subject, and offering it is the widening the
            # clarification cases are built to catch.
            continue
        retrieved.append(canonical_id)
        if canonical_id not in authorized:
            # Withheld BEFORE ranking, exactly as on the deterministic leg.
            # A restricted entity that occupied rank 0 and was dropped later
            # has already shifted every rank below it, which is a disclosure
            # by arithmetic even when the id itself never appears.
            withheld[canonical_id] = None
            continue
        source_class_value = _attribute(node, _SOURCE_CLASS_ATTRIBUTE)
        if source_class_value is None:
            raise ValueError(
                f"node {uuid!r} ({canonical_id!r}) carries no "
                f"{_SOURCE_CLASS_ATTRIBUTE!r}; it was not written by this "
                "arm's projection and its provenance cannot be stated"
            )
        candidates.append(
            SemanticCandidate(
                canonical_id=canonical_id,
                kind=kind,
                display_label=node.name,
                signal=SubjectMatchSignal.FUZZY_LABEL,
                mechanism=_mechanism_for(frozenset(methods[uuid])),
                # The STORED text that matched, not the query. A packet
                # reader already has what they typed.
                matched_text=node.name,
                source_class=SourceClass(source_class_value),
                methods=frozenset(methods[uuid]),
                rrf_score=float(score_by_uuid[uuid]),
                rank=len(candidates),
            )
        )

    # CHAOS-3653: observation -> entity hop. Only for subjects an authorized
    # observation named AND that did not already reach ``candidates``
    # directly -- a direct hit already carries the stronger claim ("this
    # entity's own name/text matched"), and duplicating it under a weaker
    # inferred claim would be reporting the same subject twice.
    already_present = {item.canonical_id for item in candidates}
    hop_needed = tuple(
        subject_id for subject_id in hop_sources if subject_id not in already_present
    )
    if hop_needed:
        hop_entities = await _entities_by_canonical_id(driver, partition, hop_needed)
        # Highest-scoring source observation first, so the hop tail is
        # ranked exactly as the direct candidates are: strongest evidence
        # first, ties broken by canonical id for a total, content-derived
        # order.
        hop_entities = tuple(
            sorted(
                hop_entities,
                key=lambda item: (
                    -hop_sources[item.canonical_id].score,
                    item.canonical_id,
                ),
            )
        )
        for entity in hop_entities:
            if entity.kind is GraphEntityKind.ORGANIZATION:
                continue
            source = hop_sources[entity.canonical_id]
            candidates.append(
                SemanticCandidate(
                    canonical_id=entity.canonical_id,
                    kind=entity.kind,
                    display_label=entity.display_label,
                    signal=SubjectMatchSignal.FUZZY_LABEL,
                    mechanism=_mechanism_for(source.methods),
                    # The observation's own stored text, not the entity's:
                    # what actually matched was the evidence, and the
                    # provenance field is what says so.
                    matched_text=source.matched_text,
                    source_class=entity.source_class,
                    methods=source.methods,
                    rrf_score=source.score,
                    rank=len(candidates),
                    via_observation_id=source.observation_id,
                )
            )

    return SemanticRetrieval(
        query=query,
        candidates=tuple(candidates[:limit]),
        authorization_filtered_count=len(withheld),
        withheld_canonical_ids=tuple(withheld),
        retrieved_before_authorization=tuple(retrieved),
        bm25_order=tuple(
            _canonical_ids(bm25_nodes),
        ),
        cosine_order=tuple(
            _canonical_ids(cosine_nodes),
        ),
        observation_hits=tuple(observations),
    )


def _canonical_ids(nodes: Any) -> list[str]:
    """Canonical ids in a primitive's own returned order, **unfiltered**.

    Deliberately not authorization filtered, and the danger in that is worth
    stating rather than leaving for a reader to discover. These tuples are
    the only channel that can answer "did cosine surface the restricted
    project, and did authorization then remove it?" — a question an empty
    candidate list and a clean candidate list answer identically. That is
    the whole authorization measurement, so the identities have to survive
    to the point where the comparison is made.

    They are a **trial diagnostic and never packet content**. Nothing in
    ``packet_builder`` reads a :class:`SemanticRetrieval`; the trial applies
    :func:`authorization filtering <retrieve_candidates>` before anything is
    ranked, and only ``candidates`` crosses into packet assembly. A caller
    that renders these into something a principal receives has defeated the
    filter, which is why they live on the retrieval record rather than on a
    candidate.
    """

    ids: list[str] = []
    for node in nodes:
        canonical_id = _attribute(node, _CANONICAL_ID_ATTRIBUTE)
        if isinstance(canonical_id, str) and canonical_id:
            ids.append(canonical_id)
    return ids


# ---------------------------------------------------------------------------
# CHAOS-3666: conversation-context resolution for pronouns and prior-turn
# references
# ---------------------------------------------------------------------------

#: The words a query built entirely from a conversational reference is made
#: of: pronouns, demonstratives, and generic entity-KIND nouns that name a
#: category without naming anything specific ("project", "team"). Closed and
#: ordinary English vocabulary -- not tuned to any corpus's phrasing, and
#: deliberately conservative: "the auth work" or "the payments project"
#: still contains identifying content once these are stripped, and must
#: fall through to exact/alias/fuzzy resolution rather than this leg.
_DEICTIC_TOKENS = frozenset(
    {
        "it",
        "its",
        "that",
        "this",
        "them",
        "those",
        "these",
        "one",
        "ones",
        "other",
        "another",
        "same",
        "thing",
        "things",
        "project",
        "team",
    }
)

#: Question scaffolding stripped before checking whether what remains is
#: purely deictic. Also closed, also ordinary vocabulary.
_QUESTION_SCAFFOLD = frozenset(
    {
        "what",
        "whats",
        "why",
        "how",
        "when",
        "where",
        "who",
        "is",
        "are",
        "was",
        "were",
        "did",
        "does",
        "do",
        "the",
        "a",
        "an",
        "of",
        "with",
        "about",
        "on",
        "up",
        "s",
        "status",
        "state",
        "happening",
        "happened",
        "happen",
        "still",
        "yet",
        "going",
        "doing",
    }
)

_WORD = re.compile(r"[a-z0-9]+")


def _is_pure_conversational_reference(query: str) -> bool:
    """Whether ``query`` names nothing but refers to something already
    established in conversation.

    Deliberately narrow: every token that survives stripping
    :data:`_QUESTION_SCAFFOLD` must be in :data:`_DEICTIC_TOKENS`, or this
    refuses. "What's holding it up?" leaves ``holding`` behind and is
    refused here — CHAOS-3654's disposition policy is what handles a bare
    pronoun with no referent at all; this function's job is the narrower
    one of recognising a query that is confidently ONLY a reference, so a
    single unambiguous prior subject may be proposed for it.
    """

    tokens = _WORD.findall(query.casefold())
    residual = [token for token in tokens if token not in _QUESTION_SCAFFOLD]
    return bool(residual) and all(token in _DEICTIC_TOKENS for token in residual)


@dataclass(frozen=True, slots=True)
class ConversationalReferenceResult:
    """Whether a bare conversational reference resolved, and why (not).

    ``candidate`` is ``None`` on every refusal path. This leg proposes at
    most one subject and never a list: the whole point of a PURE pronoun
    query is that it carries no content of its own to rank candidates by,
    so an ambiguous prior context is a refusal here, not a clarification
    with options — a caller wanting clarification candidates has the
    semantic leg's own :func:`assess_disposition` for that.
    """

    candidate: CandidateMatch | None
    reason: str


async def resolve_conversational_reference(
    query: str,
    *,
    store: RetrievalStore,
    prior_subject_ids: Sequence[str],
    authorized_entity_ids: Sequence[str] | frozenset[str],
) -> ConversationalReferenceResult:
    """Resolve a bare pronoun/deictic query to the prior turn's subject.

    **What this is not.** Not retrieval, not embedding similarity, not a
    read of conversation TEXT: ``prior_subject_ids`` is an explicit,
    already-resolved list of canonical ids a caller (the conversation-state
    owner, upstream of this arm) supplies. This function's only jobs are
    (1) deciding whether ``query`` is confidently a bare reference and
    nothing else, and (2) checking that exactly one authorized, currently-
    real subject is available to resolve it to. Both are deterministic;
    neither reads a vector.

    **Why the signal is still gated as semantic.** The emitted candidate
    carries :attr:`SubjectMatchSignal.CONVERSATIONAL_REFERENCE`, which
    ``packet_builder._INHERENTLY_SEMANTIC_SIGNALS`` refuses under a
    non-semantic embedder regardless of the mechanism that produced it.
    That policy is honoured here rather than routed around: a pronoun
    resolved with no corroborating content is exactly the kind of claim
    this arm's Phase 2A safety posture reserves for a run that can actually
    support a semantic capability, deterministic mechanism or not.

    **Authorization is rechecked against the current grant, never trusted
    from the prior turn.** A subject that was authorized last turn and is
    not authorized now (a revoked grant, a organisation-scope change) must
    not be resolved silently forward.

    **Existence is reread from the store, never trusted from the caller.**
    ``prior_subject_ids`` names what conversation state remembers; whether
    that entity still exists, under what kind and label, is asked of the
    store fresh — a deleted or renamed entity must not be resolved as if
    nothing changed.
    """

    if not _is_pure_conversational_reference(query):
        return ConversationalReferenceResult(
            candidate=None,
            reason=(
                "the query carries content beyond a bare pronoun/deictic "
                "reference; this leg only resolves a query that is "
                "confidently nothing else"
            ),
        )

    authorized = frozenset(authorized_entity_ids)
    # De-duplicated, order-preserving, then authorization-filtered -- same
    # posture as every other candidate set in this module: withheld before
    # ranking, never silently absorbed into "no prior subject".
    candidates = [
        subject_id
        for subject_id in dict.fromkeys(prior_subject_ids)
        if subject_id in authorized
    ]
    if not prior_subject_ids:
        return ConversationalReferenceResult(
            candidate=None,
            reason="no prior-turn subject is available to refer to",
        )
    if not candidates:
        return ConversationalReferenceResult(
            candidate=None,
            reason=(
                "every prior-turn subject is outside this caller's "
                "currently authorized scope"
            ),
        )
    if len(candidates) > 1:
        return ConversationalReferenceResult(
            candidate=None,
            reason=(
                f"{len(candidates)} prior-turn subjects are authorized and "
                "equally plausible; a bare reference cannot choose between "
                "them"
            ),
        )

    subject_id = candidates[0]
    rows = await _entities_by_canonical_id(store.driver, store.partition, (subject_id,))
    if not rows:
        return ConversationalReferenceResult(
            candidate=None,
            reason=(
                "the prior-turn subject no longer exists in this "
                "partition (deleted, revoked, or never real)"
            ),
        )
    row = rows[0]
    if row.kind is GraphEntityKind.ORGANIZATION:
        return ConversationalReferenceResult(
            candidate=None,
            reason=(
                "the prior-turn subject is the organization; the "
                "organization is never a conversational referent"
            ),
        )
    return ConversationalReferenceResult(
        candidate=CandidateMatch(
            canonical_id=row.canonical_id,
            kind=row.kind,
            display_label=row.display_label,
            signal=SubjectMatchSignal.CONVERSATIONAL_REFERENCE,
            # A deterministic lookup by an explicit prior-turn id -- not a
            # fuzzy or embedding match. The SIGNAL is what carries the
            # semantic-capability gate (see the module docstring above);
            # the mechanism stays honest about what actually happened.
            mechanism=MatchMechanism.EXACT_LOOKUP,
            matched_text=row.display_label,
            source_class=row.source_class,
        ),
        reason="resolved to the prior turn's single authorized, current subject",
    )
