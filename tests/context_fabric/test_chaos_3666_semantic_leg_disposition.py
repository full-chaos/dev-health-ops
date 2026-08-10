"""CHAOS-3666: the CHAOS-3647 trial's semantic leg must report what a caller
would actually receive, not the raw unbounded candidate list.

**The gap this closes.** ``trials/chaos_3647/legs.py::resolve_semantic``
called ``semantic_retrieval.retrieve_candidates`` and reported every
authorized candidate it returned as the leg's "answer" -- never calling
``semantic_retrieval.assess_disposition``. CHAOS-3654 built the
refusal/clarification disposition policy specifically so a caller never
receives a fabricated top-1 pick from a diffuse, ungrounded ranking; a trial
harness that skips it measures the wrong thing. Concretely: regenerating
``trials/chaos_3647/results/semantic-leg.records.json`` live (2026-08-10,
current tip) still showed the H08 case ("How is Halcyon doing?", a
nonexistent entity) "ranking 25 candidates anyway, led by repo_beacon" --
the exact measured failure CHAOS-3654's own module docstring says is fixed,
reproduced here at unit level because the fix was never wired into what the
trial reports.

Every test plants the specific shape (no lexical grounding / a genuine
leader / several tied candidates) ``assess_disposition`` exists to
distinguish, same harness convention as
``test_chaos_3653_observation_hop.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind
from trials.chaos_3647.legs import resolve_semantic

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


def _install(monkeypatch: pytest.MonkeyPatch, *, bm25: list, cosine: list) -> None:
    live_gate.require_graphiti_extra()
    from graphiti_core.search import search_utils

    async def fake_fulltext(driver, query, search_filter, group_ids, limit):
        return list(bm25)

    async def fake_similarity(
        driver, search_vector, search_filter, group_ids, limit, *args, **kwargs
    ):
        return list(cosine)

    monkeypatch.setattr(search_utils, "node_fulltext_search", fake_fulltext)
    monkeypatch.setattr(search_utils, "node_similarity_search", fake_similarity)


class TestTheLegReportsDispositionGatedSubjectsNotRawCandidates:
    async def test_no_lexical_grounding_refuses_empty_not_a_ranked_list(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The H08 shape exactly: BM25 finds nothing, cosine alone returns a
        pile of vector-similarity guesses. Before this fix, every one of
        them was reported as the leg's ranked answer.
        """

        weak_guesses = [
            _entity_node(f"u{i}", f"proj_guess_{i}", f"Guess {i}") for i in range(20)
        ]
        _install(monkeypatch, bm25=[], cosine=weak_guesses)

        resolution = await resolve_semantic(
            question="How is Halcyon doing?",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset(f"proj_guess_{i}" for i in range(20)),
        )

        assert resolution.subjects == (), (
            "no lexical evidence anywhere and only vector-similarity "
            "guesses is exactly the REFUSE shape; the leg must report "
            "nothing resolved, not 20 confident candidates"
        )
        assert resolution.disposition == "refuse"

    async def test_a_genuine_leader_with_lexical_support_proposes_one(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        leader = _entity_node("u_leader", "proj_vertex", "Vertex Platform")
        also_ran = _entity_node("u_other", "proj_other", "Other Project")
        _install(monkeypatch, bm25=[leader], cosine=[leader, also_ran])

        resolution = await resolve_semantic(
            question="vertex platform",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_vertex", "proj_other"}),
        )

        assert resolution.top_n(10) == ("proj_vertex",), (
            "a leader with lexical support and a real margin over the "
            "field must PROPOSE exactly one subject, not the whole "
            "authorized candidate set"
        )
        assert resolution.disposition == "propose"

    async def test_tied_candidates_clarify_bounded_not_the_whole_field(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        node_a = _entity_node("u_a", "proj_a", "Project A")
        node_b = _entity_node("u_b", "proj_b", "Project B")
        # Each leads one primitive and trails the other, so their fused RRF
        # scores come out equal (a genuine tie) rather than one leading both
        # primitives and producing a clean, non-tied margin.
        _install(monkeypatch, bm25=[node_a, node_b], cosine=[node_b, node_a])

        resolution = await resolve_semantic(
            question="one of the projects",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_a", "proj_b"}),
        )

        assert set(resolution.top_n(10)) == {"proj_a", "proj_b"}
        assert resolution.disposition == "clarify"
