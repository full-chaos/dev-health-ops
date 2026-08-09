"""CHAOS-3647: the semantic retrieval leg's guards, observed failing.

Every test here plants the specific defect its guard exists to catch. A
retrieval leg is unusually easy to test into meaninglessness — a fake driver
that returns an empty list makes every safety assertion pass — so the shape
throughout is: construct a result set that WOULD leak, and require the guard
to be what stops it.

The one thing deliberately not tested here is whether retrieval finds the
right subjects. That is a property of a live store, a real embedding model
and the corpus, it is what ``trials/chaos_3647/results/`` measures, and a
unit test that asserted it would be asserting its own fixture.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import SubjectMatchSignal
from dev_health_ops.context_fabric.graph_arm.backend import (
    DeterministicEmbedder,
    MatchMechanism,
)
from dev_health_ops.context_fabric.graph_arm.semantic_retrieval import (
    CrossPartitionRetrievalError,
    NonSemanticEmbedderError,
    RetrievalMethod,
    retrieve_candidates,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind

from . import live_gate

pytestmark = [pytest.mark.graphiti, pytest.mark.asyncio]

_PARTITION = "cf_trial_org_test"
_OTHER_PARTITION = "cf_trial_org_other"


@dataclass
class _FakeNode:
    """The subset of ``EntityNode`` the retrieval path reads."""

    uuid: str
    name: str
    group_id: str = _PARTITION
    attributes: dict[str, Any] = field(default_factory=dict)


def _node(
    uuid: str,
    canonical_id: str,
    label: str,
    *,
    kind: str | None = GraphEntityKind.PROJECT.value,
    partition: str = _PARTITION,
    source_class: str | None = SourceClass.WORK_GRAPH.value,
) -> _FakeNode:
    attributes: dict[str, Any] = {"cf_canonical_id": canonical_id}
    if kind is not None:
        attributes["cf_entity_kind"] = kind
    if source_class is not None:
        attributes["cf_source_class"] = source_class
    return _FakeNode(uuid=uuid, name=label, group_id=partition, attributes=attributes)


@dataclass(frozen=True)
class _SemanticEmbedder:
    """A stand-in that reports ``semantic=True`` and embeds nothing real.

    Legitimate ONLY here. These tests never rank by similarity — the result
    sets are supplied — so the vector's content is irrelevant and the flag is
    the only thing under test. The trial runner refuses this shape outright;
    see ``runner._require_preconditions``.
    """

    model_id: str = "test_semantic_double.v1"
    semantic: bool = True

    async def create(self, input_data: Any) -> list[float]:
        return [0.0] * 8


@dataclass(frozen=True)
class _FakeStore:
    """The three things :func:`retrieve_candidates` reads off a store.

    Not a mock of ``GraphArmStore``: it is the read-only triple the retrieval
    path uses, and spelling it out here keeps these tests from depending on a
    live driver they never call. The real store's contribution — that the
    partition is server-derived and cannot be named by a caller — is asserted
    by ``test_chaos_3617_no_caller_supplied_partition``, which is exactly the
    guard that made this signature take a store in the first place.
    """

    driver: Any
    partition: str
    embedder: Any


def _install(monkeypatch: pytest.MonkeyPatch, *, bm25: list, cosine: list) -> dict:
    """Replace the two Graphiti primitives with fixed result sets.

    ``rrf``, ``SearchFilters`` and the group-filter argument plumbing stay
    real: the fusion and the assertions are what is under test, and stubbing
    the fusion would test the stub.
    """

    live_gate.require_graphiti_extra()
    from graphiti_core.search import search_utils

    calls: dict[str, Any] = {}

    async def fake_fulltext(driver, query, search_filter, group_ids, limit):
        calls["bm25_group_ids"] = group_ids
        calls["bm25_query"] = query
        return list(bm25)

    async def fake_similarity(
        driver, search_vector, search_filter, group_ids, limit, *args, **kwargs
    ):
        calls["cosine_group_ids"] = group_ids
        calls["cosine_vector"] = search_vector
        return list(cosine)

    monkeypatch.setattr(search_utils, "node_fulltext_search", fake_fulltext)
    monkeypatch.setattr(search_utils, "node_similarity_search", fake_similarity)
    return calls


class TestTheEmbedderGuard:
    """A non-semantic embedder must stop the leg before it queries anything."""

    async def test_a_hash_embedder_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = _install(
            monkeypatch,
            bm25=[_node("u1", "proj_a", "Alpha")],
            cosine=[],
        )
        with pytest.raises(NonSemanticEmbedderError) as excinfo:
            await retrieve_candidates(
                "anything",
                store=_FakeStore(object(), _PARTITION, DeterministicEmbedder()),
                authorized_entity_ids=frozenset({"proj_a"}),
            )
        assert "deterministic_blake2b" in str(excinfo.value)
        # The refusal happens BEFORE any query. A leg that raised after
        # querying would still have spent the call, and — worse — a caller
        # catching the error would have a half-built result set to be
        # tempted by.
        assert calls == {}, "the leg queried the store before refusing"

    async def test_a_semantic_embedder_is_accepted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The control. A guard that refuses everything is not a guard."""

        _install(monkeypatch, bm25=[_node("u1", "proj_a", "Alpha")], cosine=[])
        result = await retrieve_candidates(
            "alpha",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_a"}),
        )
        assert [c.canonical_id for c in result.candidates] == ["proj_a"]


class TestAuthorizationIsAppliedBeforeRanking:
    async def test_a_restricted_top_hit_never_occupies_a_rank(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The planted defect: retrieval puts the restricted entity first.

        Rank 0 must go to the authorized entity that retrieval ranked
        SECOND. A filter applied after ranking would leave a hole at 0 or
        shift everything down while the restricted id had already decided
        the order — both of which disclose by arithmetic even though the id
        itself is absent from the output.
        """

        restricted = _node("u_restricted", "proj_quarry", "Quarry Compliance")
        allowed = _node("u_allowed", "proj_alpha", "Alpha")
        _install(monkeypatch, bm25=[restricted, allowed], cosine=[restricted, allowed])

        result = await retrieve_candidates(
            "quarry",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_alpha"}),
        )

        assert [c.canonical_id for c in result.candidates] == ["proj_alpha"]
        assert result.candidates[0].rank == 0, "ranks must be contiguous from zero"
        assert result.authorization_filtered_count == 1
        assert result.withheld_canonical_ids == ("proj_quarry",)

    async def test_the_withheld_count_is_distinct_entities_not_hits(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One restricted entity found by both primitives is one withholding.

        Counting hits would report 2 here, and that number reaches a packet
        as ``authorization_filtered_count`` — where an inflated value is a
        false claim about how much the answer was narrowed.
        """

        restricted = _node("u_restricted", "proj_quarry", "Quarry Compliance")
        _install(monkeypatch, bm25=[restricted], cosine=[restricted])
        result = await retrieve_candidates(
            "quarry",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_alpha"}),
        )
        assert result.authorization_filtered_count == 1

    async def test_nothing_restricted_means_nothing_withheld(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The control for the two tests above."""

        _install(
            monkeypatch,
            bm25=[_node("u1", "proj_alpha", "Alpha")],
            cosine=[_node("u1", "proj_alpha", "Alpha")],
        )
        result = await retrieve_candidates(
            "alpha",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_alpha"}),
        )
        assert result.authorization_filtered_count == 0
        assert result.withheld_canonical_ids == ()


class TestThePartitionAssertion:
    async def test_a_foreign_partition_node_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The planted defect: the group filter did not hold.

        Passed as an argument on every call, so this can only happen if
        Graphiti's filter regressed or a driver ignored it — which is
        precisely the class of failure an argument-only filter cannot
        detect. The corpus plants an identically labelled project in another
        tenant, so the result would be a same-named entity from a foreign
        organization ranked as the caller's own.
        """

        foreign = _node(
            "u_foreign",
            "lumen_proj_acr",
            "Agent Context Runtime",
            partition=_OTHER_PARTITION,
        )
        _install(monkeypatch, bm25=[foreign], cosine=[])
        with pytest.raises(CrossPartitionRetrievalError) as excinfo:
            await retrieve_candidates(
                "agent context runtime",
                store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
                authorized_entity_ids=frozenset({"lumen_proj_acr"}),
            )
        # Authorized, and still refused: the partition is not an
        # authorization question and must not be answerable by one.
        assert _OTHER_PARTITION in str(excinfo.value)

    async def test_the_group_filter_is_actually_passed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The assertion above is the backstop, not the mechanism."""

        calls = _install(monkeypatch, bm25=[], cosine=[])
        await retrieve_candidates(
            "anything",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset(),
        )
        assert calls["bm25_group_ids"] == [_PARTITION]
        assert calls["cosine_group_ids"] == [_PARTITION]


class TestMechanismAttribution:
    """ "The semantic leg found it" is not a finding; which primitive is."""

    async def test_bm25_only_is_reported_as_lexical(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        node = _node("u1", "proj_alpha", "Alpha")
        _install(monkeypatch, bm25=[node], cosine=[])
        result = await retrieve_candidates(
            "alpha",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_alpha"}),
        )
        candidate = result.candidates[0]
        assert candidate.methods == frozenset({RetrievalMethod.BM25})
        assert candidate.mechanism is MatchMechanism.LEXICAL_FUZZY, (
            "a node only BM25 found was found lexically; calling it an "
            "embedding similarity because the LEG is semantic is the exact "
            "confusion MatchMechanism exists to prevent"
        )

    async def test_cosine_involvement_is_reported_as_embedding_similarity(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        node = _node("u1", "proj_alpha", "Alpha")
        _install(monkeypatch, bm25=[], cosine=[node])
        result = await retrieve_candidates(
            "alpha",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_alpha"}),
        )
        assert result.candidates[0].mechanism is MatchMechanism.EMBEDDING_SIMILARITY

    async def test_every_candidate_carries_the_fuzzy_label_signal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retrieval produces a ranked guess, and the signal must say so.

        The frozen contract refuses to let ``FUZZY_LABEL`` carry a commitment
        alone. An alias hit that happens to top a similarity ranking is still
        a similarity hit — this leg never consults the alias registry — and
        emitting ``ALIAS`` for it would claim registry resolution the leg did
        not perform.
        """

        node = _node("u1", "proj_identity_rewrite", "Identity Platform Rewrite")
        _install(monkeypatch, bm25=[node], cosine=[node])
        result = await retrieve_candidates(
            "the auth work",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_identity_rewrite"}),
        )
        assert result.candidates[0].signal is SubjectMatchSignal.FUZZY_LABEL


class TestNonSubjectNodes:
    async def test_observations_are_excluded_from_subjects_but_counted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The finding H06 turns on.

        An observation node carries no ``cf_entity_kind``. Retrieval ranking
        one first and the leg silently returning nothing would read as "the
        retriever found nothing", when what happened is that it found the
        right evidence and the subject path had no hop to an entity. The
        count is what makes the two distinguishable in the artifact.
        """

        observation = _node("u_obs", "rv_vertex_cycles", "Vertex review", kind=None)
        _install(monkeypatch, bm25=[observation], cosine=[observation])
        result = await retrieve_candidates(
            "kept cycling in review",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"rv_vertex_cycles"}),
        )
        assert result.candidates == ()
        assert result.observation_hits == ("rv_vertex_cycles",)

    async def test_the_organization_is_never_a_subject(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        org = _node(
            "u_org", "org_helio", "Helio", kind=GraphEntityKind.ORGANIZATION.value
        )
        _install(monkeypatch, bm25=[org], cosine=[])
        result = await retrieve_candidates(
            "helio",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"org_helio"}),
        )
        assert result.candidates == ()

    async def test_a_node_without_a_canonical_id_is_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A node this arm did not write must not be ranked under a guess."""

        stray = _FakeNode(uuid="u_stray", name="Something", attributes={})
        _install(monkeypatch, bm25=[stray], cosine=[])
        result = await retrieve_candidates(
            "something",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset(),
        )
        assert result.candidates == ()

    async def test_a_node_without_a_source_class_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Provenance is not optional on something about to be ranked."""

        node = _node("u1", "proj_alpha", "Alpha", source_class=None)
        _install(monkeypatch, bm25=[node], cosine=[])
        with pytest.raises(ValueError, match="cf_source_class"):
            await retrieve_candidates(
                "alpha",
                store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
                authorized_entity_ids=frozenset({"proj_alpha"}),
            )
