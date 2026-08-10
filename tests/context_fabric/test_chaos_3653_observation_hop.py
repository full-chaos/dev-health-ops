"""CHAOS-3653: the observation -> entity hop.

Retrieval can find the right EVIDENCE — an observation whose stored text
matches the question — without the entity it is about ever matching the
query itself. Before this, the resolver had no route from "the right
observation" to "the right entity": the observation was counted in
``observation_hits`` and thrown away, and the caller was left with whatever
unrelated entities happened to rank nearby.

Every test here plants the specific defect its guard exists to catch, same
shape as ``test_chaos_3647_semantic_leg.py``: a fake driver returning an
observation node that WOULD produce a wrong or absent hop unless the guard
under test is what stops it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.context_fabric.graph_arm import semantic_retrieval
from dev_health_ops.context_fabric.graph_arm.backend import (
    OBSERVATION_SUBJECTS_ATTRIBUTE,
)
from dev_health_ops.context_fabric.graph_arm.readback import EntityLookupRow
from dev_health_ops.context_fabric.graph_arm.semantic_retrieval import (
    RetrievalMethod,
    retrieve_candidates,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind

from . import live_gate

pytestmark = [pytest.mark.graphiti, pytest.mark.asyncio]

_PARTITION = "cf_trial_org_test"


@dataclass
class _FakeNode:
    uuid: str
    name: str
    group_id: str = _PARTITION
    attributes: dict[str, Any] = field(default_factory=dict)


def _entity_node(
    uuid: str,
    canonical_id: str,
    label: str,
    *,
    kind: str = GraphEntityKind.PROJECT.value,
    source_class: str = SourceClass.WORK_GRAPH.value,
) -> _FakeNode:
    return _FakeNode(
        uuid=uuid,
        name=label,
        attributes={
            "cf_canonical_id": canonical_id,
            "cf_entity_kind": kind,
            "cf_source_class": source_class,
        },
    )


def _observation_node(
    uuid: str,
    canonical_id: str,
    title: str,
    *,
    subject_canonical_ids: tuple[str, ...] = (),
) -> _FakeNode:
    attributes: dict[str, Any] = {"cf_canonical_id": canonical_id}
    if subject_canonical_ids:
        attributes[OBSERVATION_SUBJECTS_ATTRIBUTE] = ",".join(subject_canonical_ids)
    return _FakeNode(uuid=uuid, name=title, attributes=attributes)


@dataclass(frozen=True)
class _SemanticEmbedder:
    model_id: str = "test_semantic_double.v1"
    semantic: bool = True

    async def create(self, input_data: Any) -> list[float]:
        return [0.0] * 8


@dataclass(frozen=True)
class _FakeStore:
    driver: Any
    partition: str
    embedder: Any


def _install(monkeypatch: pytest.MonkeyPatch, *, bm25: list, cosine: list) -> dict:
    live_gate.require_graphiti_extra()
    from graphiti_core.search import search_utils

    calls: dict[str, Any] = {}

    async def fake_fulltext(driver, query, search_filter, group_ids, limit):
        calls["bm25_group_ids"] = group_ids
        return list(bm25)

    async def fake_similarity(
        driver, search_vector, search_filter, group_ids, limit, *args, **kwargs
    ):
        return list(cosine)

    monkeypatch.setattr(search_utils, "node_fulltext_search", fake_fulltext)
    monkeypatch.setattr(search_utils, "node_similarity_search", fake_similarity)
    return calls


def _install_entity_lookup(
    monkeypatch: pytest.MonkeyPatch, rows: tuple[EntityLookupRow, ...]
) -> dict:
    """Replace the hop's one lookup query. Recorded, never executed for real."""

    calls: dict[str, Any] = {}

    async def fake_lookup(driver, partition, canonical_ids):
        calls["partition"] = partition
        calls["canonical_ids"] = tuple(canonical_ids)
        return tuple(row for row in rows if row.canonical_id in canonical_ids)

    monkeypatch.setattr(semantic_retrieval, "_entities_by_canonical_id", fake_lookup)
    return calls


class TestTheHopResolvesTheCorrectSubject:
    async def test_a_retrieved_observation_hops_to_its_subject(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The measured case: "kept cycling in review" finds the review
        observation and must resolve the project it is about."""

        observation = _observation_node(
            "u_obs",
            "rv_vertex_cycles",
            "Vertex review",
            subject_canonical_ids=("proj_vertex",),
        )
        _install(monkeypatch, bm25=[observation], cosine=[observation])
        lookup_calls = _install_entity_lookup(
            monkeypatch,
            (
                EntityLookupRow(
                    canonical_id="proj_vertex",
                    kind=GraphEntityKind.PROJECT,
                    display_label="Vertex Platform",
                    source_class=SourceClass.WORK_GRAPH,
                ),
            ),
        )

        result = await retrieve_candidates(
            "kept cycling in review",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )

        assert [c.canonical_id for c in result.candidates] == ["proj_vertex"]
        candidate = result.candidates[0]
        assert candidate.via_observation_id == "rv_vertex_cycles"
        assert candidate.display_label == "Vertex Platform"
        assert candidate.matched_text == "Vertex review", (
            "the matched text must be the observation's own stored text, "
            "not the entity's name -- that is what actually matched"
        )
        assert lookup_calls["canonical_ids"] == ("proj_vertex",)
        assert result.observation_hits == ("rv_vertex_cycles",)

    async def test_an_observation_with_no_subject_attribute_hops_nowhere(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A partition written before the attachment attribute existed."""

        observation = _observation_node("u_obs", "rv_old", "Old review")
        _install(monkeypatch, bm25=[observation], cosine=[])
        lookup_calls = _install_entity_lookup(monkeypatch, ())

        result = await retrieve_candidates(
            "anything",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )

        assert result.candidates == ()
        assert "canonical_ids" not in lookup_calls, (
            "an observation with nothing to hop to must not trigger a "
            "lookup round trip at all"
        )


class TestTheHopIsAuthorized:
    async def test_an_unauthorized_subject_is_withheld_not_returned(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        observation = _observation_node(
            "u_obs",
            "rv_restricted_review",
            "Restricted review",
            subject_canonical_ids=("proj_restricted",),
        )
        _install(monkeypatch, bm25=[observation], cosine=[])
        lookup_calls = _install_entity_lookup(
            monkeypatch,
            (
                EntityLookupRow(
                    canonical_id="proj_restricted",
                    kind=GraphEntityKind.PROJECT,
                    display_label="Restricted Compliance",
                    source_class=SourceClass.WORK_GRAPH,
                ),
            ),
        )

        result = await retrieve_candidates(
            "restricted",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset(),
        )

        assert result.candidates == ()
        assert result.authorization_filtered_count == 1
        assert result.withheld_canonical_ids == ("proj_restricted",)
        assert "canonical_ids" not in lookup_calls, (
            "an unauthorized subject must never reach the lookup query -- "
            "authorization is applied before the hop is even attempted, "
            "not after"
        )

    async def test_the_organization_is_never_a_hop_candidate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        observation = _observation_node(
            "u_obs",
            "rv_org_note",
            "Org-wide note",
            subject_canonical_ids=("org_helio",),
        )
        _install(monkeypatch, bm25=[observation], cosine=[])
        _install_entity_lookup(
            monkeypatch,
            (
                EntityLookupRow(
                    canonical_id="org_helio",
                    kind=GraphEntityKind.ORGANIZATION,
                    display_label="Helio",
                    source_class=SourceClass.WORK_GRAPH,
                ),
            ),
        )

        result = await retrieve_candidates(
            "helio",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"org_helio"}),
        )
        assert result.candidates == ()


class TestTheHopDoesNotDuplicateADirectHit:
    async def test_a_subject_found_directly_is_not_also_added_via_hop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        direct = _entity_node("u_direct", "proj_vertex", "Vertex Platform")
        observation = _observation_node(
            "u_obs",
            "rv_vertex_cycles",
            "Vertex review",
            subject_canonical_ids=("proj_vertex",),
        )
        _install(monkeypatch, bm25=[direct, observation], cosine=[direct, observation])
        lookup_calls = _install_entity_lookup(
            monkeypatch,
            (
                EntityLookupRow(
                    canonical_id="proj_vertex",
                    kind=GraphEntityKind.PROJECT,
                    display_label="Vertex Platform",
                    source_class=SourceClass.WORK_GRAPH,
                ),
            ),
        )

        result = await retrieve_candidates(
            "vertex",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )

        assert [c.canonical_id for c in result.candidates] == ["proj_vertex"]
        assert result.candidates[0].via_observation_id is None, (
            "a subject that matched directly carries the stronger claim; "
            "the weaker observation-derived one must not overwrite or "
            "duplicate it"
        )
        assert "canonical_ids" not in lookup_calls, (
            "the direct hit already satisfies this subject, so the hop "
            "lookup must not even be attempted for it"
        )


class TestTheHopPicksTheStrongestSource:
    async def test_two_observations_naming_the_same_subject_use_the_higher_score(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Both observations are BM25 hits; only one is also a cosine hit,
        so it fuses to a strictly higher RRF score and must win the hop."""

        weak = _observation_node(
            "u_weak", "rv_weak", "Weak mention", subject_canonical_ids=("proj_vertex",)
        )
        strong = _observation_node(
            "u_strong",
            "rv_strong",
            "Strong mention",
            subject_canonical_ids=("proj_vertex",),
        )
        _install(monkeypatch, bm25=[weak, strong], cosine=[strong])
        _install_entity_lookup(
            monkeypatch,
            (
                EntityLookupRow(
                    canonical_id="proj_vertex",
                    kind=GraphEntityKind.PROJECT,
                    display_label="Vertex Platform",
                    source_class=SourceClass.WORK_GRAPH,
                ),
            ),
        )

        result = await retrieve_candidates(
            "vertex",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )

        assert len(result.candidates) == 1
        assert result.candidates[0].via_observation_id == "rv_strong"
        assert result.candidates[0].methods == frozenset(
            {RetrievalMethod.BM25, RetrievalMethod.COSINE}
        )
