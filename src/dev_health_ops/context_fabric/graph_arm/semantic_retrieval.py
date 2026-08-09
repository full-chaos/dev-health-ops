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

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import SubjectMatchSignal

from .backend import EmbeddingBackend, MatchMechanism, graphiti_module
from .vocabulary import GraphEntityKind

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Sequence

__all__ = [
    "DEFAULT_SEMANTIC_LIMIT",
    "CrossPartitionRetrievalError",
    "RetrievalMethod",
    "RetrievalStore",
    "SemanticCandidate",
    "SemanticRetrieval",
    "NonSemanticEmbedderError",
    "retrieve_candidates",
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


def _attribute(node: Any, key: str) -> Any:
    attributes = getattr(node, "attributes", None) or {}
    return attributes.get(key)


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
