"""CHAOS-3632 (read-side): a document-derived observation must clear the
same trust bar as any other attributed record before it may hop a subject
into a candidate.

**Why the write side alone is not enough.** ``backend.to_graphiti_document_
nodes`` (CHAOS-3632, write side) only ever writes a node for a document
already in ``projection.approved_documents`` -- a rejected document never
reaches the graph at all. But approval is computed once, at projection-build
time; nothing re-checks it on read. A document approved when a projection
was built and later withdrawn, redacted, or revoked stays in the store,
findable, until the next full reprojection sweep purges it. This module's
retrieval hop is where a live, per-read trust check closes that window --
mirroring :func:`drivers._is_trusted`'s exact semantics (no trust level, or
one outside ``TRUSTED_ATTRIBUTION_LEVELS``, is not trusted; no silent
default) rather than trusting a one-time write-side gate to still be
accurate.

**Why this is scoped to DOCUMENT-kind observations only.** Every other
observation kind (review, measurement, evidence-backed record, ...) already
passes through its own, different quality gate before it ever reaches this
arm's projection -- canonical-service admission, evidence-reference
signing, the CHAOS-3627/3633/3650 machinery. Applying a blanket trust
re-check to every observation kind here would silently change hop behaviour
for records this ticket has no mandate over. A document is different:
CHAOS-3632 exists specifically because documents are raw, human-authored
prose with no such upstream gate of their own, and this arm is deciding for
the first time whether one may act as evidence.

Every test plants the specific defect this gate exists to catch, same shape
as ``test_chaos_3653_observation_hop.py``: a fake driver returning a node
that WOULD produce a wrong hop unless the guard under test is what stops it.
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
from dev_health_ops.context_fabric.graph_arm.drivers import TRUSTED_ATTRIBUTION_LEVELS
from dev_health_ops.context_fabric.graph_arm.readback import EntityLookupRow
from dev_health_ops.context_fabric.graph_arm.semantic_retrieval import (
    retrieve_candidates,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import (
    GraphEntityKind,
    GraphObservationKind,
)

from . import live_gate

pytestmark = [pytest.mark.graphiti, pytest.mark.asyncio]

_PARTITION = "cf_trial_org_test"


@dataclass
class _FakeNode:
    uuid: str
    name: str
    group_id: str = _PARTITION
    attributes: dict[str, Any] = field(default_factory=dict)


def _observation_node(
    uuid: str,
    canonical_id: str,
    title: str,
    *,
    subject_canonical_ids: tuple[str, ...] = (),
    observation_kind: GraphObservationKind | None = None,
    corpus_trust: str | None = None,
) -> _FakeNode:
    """Mirrors ``test_chaos_3653_observation_hop``'s helper, extended with
    the two attributes this gate reads: ``cf_observation_kind`` (does the
    document-only scope even apply?) and ``cf_attr_corpus_trust`` (the raw,
    ``cf_attr_``-prefixed key ``backend.to_graphiti_document_nodes`` writes
    every ``document.attributes`` entry under -- retrieval reads raw
    Graphiti nodes directly, never through ``readback``'s stripped shape).
    """

    attributes: dict[str, Any] = {"cf_canonical_id": canonical_id}
    if subject_canonical_ids:
        attributes[OBSERVATION_SUBJECTS_ATTRIBUTE] = ",".join(subject_canonical_ids)
    if observation_kind is not None:
        attributes["cf_observation_kind"] = observation_kind.value
    if corpus_trust is not None:
        attributes["cf_attr_corpus_trust"] = corpus_trust
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
    calls: dict[str, Any] = {}

    async def fake_lookup(driver, partition, canonical_ids):
        calls["partition"] = partition
        calls["canonical_ids"] = tuple(canonical_ids)
        return tuple(row for row in rows if row.canonical_id in canonical_ids)

    monkeypatch.setattr(semantic_retrieval, "_entities_by_canonical_id", fake_lookup)
    return calls


_VERTEX_LOOKUP = (
    EntityLookupRow(
        canonical_id="proj_vertex",
        kind=GraphEntityKind.PROJECT,
        display_label="Vertex Platform",
        source_class=SourceClass.WORK_GRAPH,
    ),
)


class TestApprovedDocumentTrustGate:
    async def test_a_trusted_document_hops_its_subject(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Positive control: nothing in TRUSTED_ATTRIBUTION_LEVELS is a
        frozen-corpus value today (every corpus document is unapproved, per
        corpus_adapter._document_is_approved), so this is a synthetic,
        held-out fixture rather than a corpus case -- proving the gate
        passes a genuinely trusted document, not merely that it never fires.
        """

        document = _observation_node(
            "u_doc",
            "doc_vertex_design_review",
            "Vertex design review notes",
            subject_canonical_ids=("proj_vertex",),
            observation_kind=GraphObservationKind.DOCUMENT,
            corpus_trust=next(iter(TRUSTED_ATTRIBUTION_LEVELS)),
        )
        _install(monkeypatch, bm25=[document], cosine=[document])
        _install_entity_lookup(monkeypatch, _VERTEX_LOOKUP)

        result = await retrieve_candidates(
            "vertex design review",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )

        assert [c.canonical_id for c in result.candidates] == ["proj_vertex"]
        assert result.candidates[0].via_observation_id == "doc_vertex_design_review"

    @pytest.mark.parametrize(
        "corpus_trust",
        [
            "withdrawn",
            "redacted",
            "untrusted_content",
            None,  # the attribute is absent entirely -- today's actual corpus shape
        ],
    )
    async def test_a_document_whose_trust_does_not_clear_the_bar_hops_nothing(
        self, monkeypatch: pytest.MonkeyPatch, corpus_trust: str | None
    ) -> None:
        """The defect this gate exists to catch: without it, a withdrawn,
        redacted, or simply never-trusted document that named a subject
        would still resolve that subject into a candidate -- exactly the
        stale-approval window the module docstring above describes.
        """

        document = _observation_node(
            "u_doc",
            "doc_vertex_leaked_draft",
            "Vertex leaked draft",
            subject_canonical_ids=("proj_vertex",),
            observation_kind=GraphObservationKind.DOCUMENT,
            corpus_trust=corpus_trust,
        )
        _install(monkeypatch, bm25=[document], cosine=[document])
        lookup_calls = _install_entity_lookup(monkeypatch, _VERTEX_LOOKUP)

        result = await retrieve_candidates(
            "vertex leaked draft",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )

        assert result.candidates == ()
        assert "canonical_ids" not in lookup_calls, (
            "an untrusted document's named subject must never even reach "
            "the hop lookup -- the gate applies before the round trip, not "
            "as a post-hoc filter on its result"
        )

    async def test_the_untrusted_document_is_still_counted_as_an_observation_hit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retrieval genuinely found this node -- that stays a true,
        recorded fact (``observation_hits`` is a trial diagnostic of what
        retrieval touched) even though it earns no hop. Silently dropping it
        from the diagnostic would make an untrusted hit indistinguishable
        from retrieval finding nothing at all.
        """

        document = _observation_node(
            "u_doc",
            "doc_untrusted",
            "Untrusted note",
            subject_canonical_ids=("proj_vertex",),
            observation_kind=GraphObservationKind.DOCUMENT,
            corpus_trust="withdrawn",
        )
        _install(monkeypatch, bm25=[document], cosine=[])
        _install_entity_lookup(monkeypatch, _VERTEX_LOOKUP)

        result = await retrieve_candidates(
            "untrusted",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )

        assert result.observation_hits == ("doc_untrusted",)
        assert result.candidates == ()

    async def test_an_untrusted_document_does_not_shadow_a_trusted_source_for_the_same_subject(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The gate skips only THIS document's own contribution -- it must
        not suppress a different, trusted observation naming the identical
        subject. Proves the check is scoped per-node, not a wholesale "any
        untrusted hit poisons the subject" rule.
        """

        untrusted = _observation_node(
            "u_untrusted",
            "doc_untrusted",
            "Untrusted mention",
            subject_canonical_ids=("proj_vertex",),
            observation_kind=GraphObservationKind.DOCUMENT,
            corpus_trust="withdrawn",
        )
        trusted = _observation_node(
            "u_trusted",
            "rv_trusted_review",
            "Trusted review",
            subject_canonical_ids=("proj_vertex",),
        )
        _install(monkeypatch, bm25=[untrusted, trusted], cosine=[trusted])
        _install_entity_lookup(monkeypatch, _VERTEX_LOOKUP)

        result = await retrieve_candidates(
            "vertex",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )

        assert [c.canonical_id for c in result.candidates] == ["proj_vertex"]
        assert result.candidates[0].via_observation_id == "rv_trusted_review"

    async def test_the_trust_gate_does_not_apply_to_non_document_observation_kinds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Scope boundary regression guard: an observation that is NOT a
        document (no ``cf_observation_kind`` attribute at all -- the shape
        every non-document observation in this projection has today) must
        hop exactly as it did before this gate existed, regardless of what
        its own ``corpus_trust`` reads. This ticket's mandate is the
        document approval gate, not a new blanket trust re-check over every
        observation kind this arm reads.
        """

        observation = _observation_node(
            "u_obs",
            "rv_vertex_cycles",
            "Vertex review",
            subject_canonical_ids=("proj_vertex",),
            corpus_trust="withdrawn",  # would fail the bar, if the gate applied here
        )
        _install(monkeypatch, bm25=[observation], cosine=[observation])
        _install_entity_lookup(monkeypatch, _VERTEX_LOOKUP)

        result = await retrieve_candidates(
            "vertex review",
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )

        assert [c.canonical_id for c in result.candidates] == ["proj_vertex"]
