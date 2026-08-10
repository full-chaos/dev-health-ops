"""issue 3666: the semantic trial leg gets a fair chance to use conversation
context before falling through to retrieval.

``semantic_retrieval.resolve_conversational_reference`` (issue 3686) is real,
merged, and unit-tested in isolation. ``resolve_semantic_with_context`` is
the seam that would let a caller with real prior-turn state use it before
falling through to the existing disposition-gated semantic leg -- proven
correct here with synthetic bare-reference queries ("how's it going?",
"what about it?"), deliberately NOT this trial's own corpus questions.

**Why not the corpus questions.** ``_is_pure_conversational_reference``
returns ``False`` on all eight corpus ambiguity cases, including the two
``follows_case_id`` cases (H04 "what's holding it up?", H05 "what about the
other project we discussed?") -- both carry real content ("holding", "we
discussed") beyond a bare pronoun by the function's own narrow, deliberately
non-corpus-tuned definition. So this function is NOT wired into
``trials/chaos_3647/runner.py``: doing so would move zero lines of the
trial artifact, since no corpus question would ever route through it. See
``resolve_semantic_with_context``'s own docstring for the full reasoning.
These tests exist to prove the mechanism is correct and ready for a real
conversation-state source, independent of this trial's corpus.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.context_fabric.graph_arm import semantic_retrieval
from dev_health_ops.context_fabric.graph_arm.readback import EntityLookupRow
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind
from trials.chaos_3647.legs import resolve_semantic_with_context

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


def _install_entity_lookup(
    monkeypatch: pytest.MonkeyPatch, rows: tuple[EntityLookupRow, ...]
) -> None:
    async def fake_lookup(driver, partition, canonical_ids):
        return tuple(row for row in rows if row.canonical_id in canonical_ids)

    monkeypatch.setattr(semantic_retrieval, "_entities_by_canonical_id", fake_lookup)


class TestResolveSemanticWithContext:
    async def test_a_bare_reference_with_a_valid_prior_subject_resolves_via_context(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A genuine bare reference (not H04's literal text -- see the
        module docstring for why) with a real, single, authorized prior
        subject must resolve to it -- not refuse.
        """

        _install(monkeypatch, bm25=[], cosine=[])
        _install_entity_lookup(
            monkeypatch,
            (
                EntityLookupRow(
                    canonical_id="proj_identity_rewrite",
                    kind=GraphEntityKind.PROJECT,
                    display_label="Identity Platform Rewrite",
                    source_class=SourceClass.WORK_GRAPH,
                ),
            ),
        )

        resolution = await resolve_semantic_with_context(
            question="how's it going?",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_identity_rewrite"}),
            prior_subject_ids=("proj_identity_rewrite",),
        )

        assert resolution.top_n(10) == ("proj_identity_rewrite",)
        assert resolution.disposition == "conversational_reference"

    async def test_a_bare_reference_with_no_prior_context_falls_through_to_refusal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No prior turn at all -- the exact H04 shape before this change.
        Must behave identically to plain ``resolve_semantic``: refuse, not
        crash, not silently invent a candidate.
        """

        _install(monkeypatch, bm25=[], cosine=[])

        resolution = await resolve_semantic_with_context(
            question="what's holding it up?",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_identity_rewrite"}),
            prior_subject_ids=(),
        )

        assert resolution.subjects == ()
        assert resolution.disposition == "refuse"

    async def test_an_ambiguous_prior_context_falls_through_not_a_guess(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two equally plausible prior subjects -- a bare reference cannot
        choose between them, so this must fall through rather than pick one.
        """

        _install(monkeypatch, bm25=[], cosine=[])

        resolution = await resolve_semantic_with_context(
            question="what about it?",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_a", "proj_b"}),
            prior_subject_ids=("proj_a", "proj_b"),
        )

        assert resolution.subjects == ()
        assert resolution.disposition != "conversational_reference"

    async def test_a_substantive_query_is_untouched_even_with_prior_context(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The H06 shape: real content in the query ("kept cycling in
        review") must never be hijacked into a bare-reference resolution
        just because a prior turn exists -- it needs the hop, not this leg.
        """

        # A direct entity hit stands in for whatever ordinary retrieval would
        # find -- the fallthrough path is what's under test here, not the
        # hop mechanism itself (that's covered by test_chaos_3653_observation_hop.py).
        direct = _entity_node("u_direct", "proj_vertex", "Vertex Platform")
        _install(monkeypatch, bm25=[direct], cosine=[direct])

        resolution = await resolve_semantic_with_context(
            question="What happened with the project that kept cycling in review?",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_vertex", "proj_identity_rewrite"}),
            prior_subject_ids=("proj_identity_rewrite",),
        )

        assert resolution.disposition != "conversational_reference"
        assert resolution.top_n(10) == ("proj_vertex",), (
            "a substantive query must resolve through ordinary retrieval, "
            "never be redirected to the prior turn's subject"
        )
