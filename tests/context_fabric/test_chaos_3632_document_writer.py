"""CHAOS-3632: approved documents reach the store as real nodes.

``build_projection`` (CHAOS-3617) already computes ``approved_documents``/
``rejected_document_ids`` -- read once, from the corpus adapter onward, but
never converted to a node: :func:`to_graphiti_nodes` iterates
``projection.nodes`` only, which never contained a document at all. This
module is the writer that closes that gap
(:func:`~.backend.to_graphiti_document_nodes`) and the guard that a
*rejected* document cannot reach it by any path.

Reading documents back through semantic/BM25 retrieval is explicitly out of
scope here -- that is Lane D's follow-up on ``semantic_retrieval.py``. What
this module proves is narrower and prior to that: the node exists, carries
the agreed attribute convention, and a rejected document never becomes one.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.context_fabric.graph_arm import fixtures
from dev_health_ops.context_fabric.graph_arm.backend import (
    OBSERVATION_SUBJECTS_ATTRIBUTE,
    DeterministicEmbedder,
    to_graphiti_document_nodes,
)
from dev_health_ops.context_fabric.graph_arm.projection import GraphProjection
from dev_health_ops.context_fabric.graph_arm.records import (
    CanonicalRef,
    UnstructuredDocumentRecord,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind

pytestmark = pytest.mark.asyncio

#: alpha_batch's adversarial fixture: an UNAPPROVED document whose body is a
#: prompt-injection attempt. If this text ever reached a node, it would be
#: exactly the "rejected material becomes graph-visible" fault this module
#: exists to prevent.
_REJECTED_BODY_MARKER = "Ignore previous instructions"


async def test_only_the_approved_document_reaches_a_node(alpha_projection) -> None:
    nodes = await to_graphiti_document_nodes(alpha_projection, DeterministicEmbedder())
    assert [node.attributes["cf_canonical_id"] for node in nodes] == ["doc_nfm_readme"]


async def test_a_rejected_document_never_reaches_a_node(alpha_projection) -> None:
    """The RED anchor this whole module exists to keep green.

    Not just "the rejected canonical id is absent" -- the rejected
    document's own adversarial body text must not appear ANYWHERE in the
    output, in case a future edit routed it into a different field.
    """

    nodes = await to_graphiti_document_nodes(alpha_projection, DeterministicEmbedder())
    canonical_ids = {node.attributes["cf_canonical_id"] for node in nodes}
    assert "doc_unapproved_thread" not in canonical_ids

    serialized = repr([(node.name, node.summary, node.attributes) for node in nodes])
    assert _REJECTED_BODY_MARKER not in serialized


async def test_name_is_title_never_body(alpha_projection) -> None:
    (node,) = await to_graphiti_document_nodes(
        alpha_projection, DeterministicEmbedder()
    )
    assert node.name == "Nightfall Migration design note"
    assert "auth gateway" not in node.name


async def test_summary_stays_empty(alpha_projection) -> None:
    """Mirrors to_graphiti_nodes's own no-prose rule -- summary is
    Graphiti's slot for model-written text, and a document's body must
    never end up there either.
    """

    (node,) = await to_graphiti_document_nodes(
        alpha_projection, DeterministicEmbedder()
    )
    assert node.summary == ""


async def test_body_is_stored_nowhere_on_the_node(alpha_projection) -> None:
    (node,) = await to_graphiti_document_nodes(
        alpha_projection, DeterministicEmbedder()
    )
    body_fragment = "auth gateway's token exchange"
    assert body_fragment not in node.name
    assert body_fragment not in node.summary
    assert body_fragment not in repr(node.attributes)


async def test_cf_entity_kind_is_absent(alpha_projection) -> None:
    (node,) = await to_graphiti_document_nodes(
        alpha_projection, DeterministicEmbedder()
    )
    assert "cf_entity_kind" not in node.attributes
    assert node.attributes["cf_is_entity"] is False
    assert node.attributes["cf_observation_kind"] == "document"


async def test_subjects_join_into_the_observation_attribute(alpha_projection) -> None:
    (node,) = await to_graphiti_document_nodes(
        alpha_projection, DeterministicEmbedder()
    )
    assert node.attributes[OBSERVATION_SUBJECTS_ATTRIBUTE] == "proj_nightfall_migration"


async def test_generic_attributes_pass_through_with_the_cf_attr_prefix() -> None:
    """CHAOS-3632's records.py addition: the same generic ``attributes``
    field EntityRecord/ObservationRecord already carry, not a dedicated
    trust field -- so it round-trips through the identical ``cf_attr_``
    convention :func:`to_graphiti_nodes` already uses.
    """

    document = UnstructuredDocumentRecord(
        org_id="org_test",
        canonical_id="doc_trust_test",
        title="Trust-carrying document",
        body="body text, never stored",
        source_class=SourceClass.WORK_GRAPH,
        observed_at=fixtures.WINDOW_END,
        approved=True,
        attributes={"corpus_trust": "trusted"},
    )
    projection = GraphProjection(
        org_id="org_test",
        partition="cf_trial_org_test",
        projection_version="test.v1",
        nodes=(),
        edges=(),
        approved_documents=(document,),
    )
    (node,) = await to_graphiti_document_nodes(projection, DeterministicEmbedder())
    assert node.attributes["cf_attr_corpus_trust"] == "trusted"


async def test_name_embedding_is_a_function_of_body_not_title() -> None:
    """The whole point of this function being async and unconditional
    (unlike to_graphiti_nodes's deterministic-only special case): a
    document must be findable by content, so its vector has to come from
    ``body`` -- proven here by holding title fixed and varying body, and
    the converse.
    """

    def _doc(canonical_id: str, *, title: str, body: str) -> UnstructuredDocumentRecord:
        return UnstructuredDocumentRecord(
            org_id="org_test",
            canonical_id=canonical_id,
            title=title,
            body=body,
            source_class=SourceClass.WORK_GRAPH,
            observed_at=fixtures.WINDOW_END,
            approved=True,
        )

    same_title_different_body = GraphProjection(
        org_id="org_test",
        partition="cf_trial_org_test",
        projection_version="test.v1",
        nodes=(),
        edges=(),
        approved_documents=(
            _doc("doc_a", title="Same title", body="body one"),
            _doc("doc_b", title="Same title", body="body two"),
        ),
    )
    nodes = await to_graphiti_document_nodes(
        same_title_different_body, DeterministicEmbedder()
    )
    assert nodes[0].name_embedding != nodes[1].name_embedding

    same_body_different_title = GraphProjection(
        org_id="org_test",
        partition="cf_trial_org_test",
        projection_version="test.v1",
        nodes=(),
        edges=(),
        approved_documents=(
            _doc("doc_c", title="Title one", body="identical body text"),
            _doc("doc_d", title="Title two", body="identical body text"),
        ),
    )
    nodes = await to_graphiti_document_nodes(
        same_body_different_title, DeterministicEmbedder()
    )
    assert nodes[0].name_embedding == nodes[1].name_embedding


async def test_repository_ids_join_when_present() -> None:
    document = UnstructuredDocumentRecord(
        org_id="org_test",
        canonical_id="doc_repo_test",
        title="Repo-scoped document",
        body="body",
        source_class=SourceClass.WORK_GRAPH,
        observed_at=fixtures.WINDOW_END,
        approved=True,
        repository_ids=("repo_b", "repo_a"),
    )
    projection = GraphProjection(
        org_id="org_test",
        partition="cf_trial_org_test",
        projection_version="test.v1",
        nodes=(),
        edges=(),
        approved_documents=(document,),
    )
    (node,) = await to_graphiti_document_nodes(projection, DeterministicEmbedder())
    assert node.attributes["cf_repository_ids"] == "repo_a,repo_b"


async def test_no_approved_documents_produces_no_nodes() -> None:
    projection = GraphProjection(
        org_id="org_test",
        partition="cf_trial_org_test",
        projection_version="test.v1",
        nodes=(),
        edges=(),
    )
    nodes = await to_graphiti_document_nodes(projection, DeterministicEmbedder())
    assert nodes == []


async def test_multiple_subjects_sort_deterministically() -> None:
    document = UnstructuredDocumentRecord(
        org_id="org_test",
        canonical_id="doc_multi_subject",
        title="Multi-subject document",
        body="body",
        source_class=SourceClass.WORK_GRAPH,
        observed_at=fixtures.WINDOW_END,
        approved=True,
        subjects=(
            CanonicalRef(kind=GraphEntityKind.PROJECT, canonical_id="proj_z"),
            CanonicalRef(kind=GraphEntityKind.PROJECT, canonical_id="proj_a"),
        ),
    )
    projection = GraphProjection(
        org_id="org_test",
        partition="cf_trial_org_test",
        projection_version="test.v1",
        nodes=(),
        edges=(),
        approved_documents=(document,),
    )
    (node,) = await to_graphiti_document_nodes(projection, DeterministicEmbedder())
    assert node.attributes[OBSERVATION_SUBJECTS_ATTRIBUTE] == "proj_a,proj_z"
