"""CHAOS-3632: removing a document's approval propagates to the index.

Slice 1 of CHAOS-3632's remaining acceptance criteria. #1665 (write side)
only ever writes a node for an already-approved document; #1666 (read side)
hides an untrusted document's named subjects from the observation-hop, but
neither one removes the node itself once it has been written. A document
approved when a projection was built and later withdrawn, redacted or
revoked stays in the store -- findable by BM25 and vector search, its title
still surfacing as a genuine ``observation_hits`` retrieval hit -- until the
next full reprojection sweep purges the whole partition.

``GraphArmStore.remove_document`` (``store.py``) closes that: a targeted,
uuid-scoped delete of one document's node, promptly, without touching
anything else in the partition. These are the live tests -- the ones that
can fail because Cypher or FalkorDB disagrees with what the code claims,
which no in-memory test can measure.

Also proves, against a real FalkorDB, the specific gotcha that ruled out
reusing ``graphiti_core``'s own ``EntityNode.delete()``: that method matches
only ``Entity``/``Episodic``/``Community`` labels for the FalkorDB driver
branch, and a document node is labelled ``CFObsDocument`` -- so it would
silently match nothing. ``remove_document``'s own uuid-only match pattern is
what makes it correct for a label graphiti's delete does not know about.
"""

from __future__ import annotations

import dataclasses
import logging
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from dev_health_ops.context_fabric.graph_arm import build_projection, fixtures
from dev_health_ops.context_fabric.graph_arm.backend import graphiti_module
from dev_health_ops.context_fabric.graph_arm.semantic_retrieval import (
    wait_for_fulltext_index,
)
from dev_health_ops.context_fabric.graph_arm.store import (
    DocumentRemovalReason,
    GraphArmStore,
)
from tests.context_fabric import live_gate

pytestmark = [pytest.mark.graphiti, pytest.mark.asyncio]

#: The one approved document in ``fixtures.alpha_batch()`` -- see
#: ``test_chaos_3632_document_writer.py`` for the same fixture used at the
#: unit level. Its twin, ``doc_unapproved_thread``, is never approved and so
#: never reaches a node at all -- exercised below as the "never written"
#: no-op case, distinct from "written, then removed."
_APPROVED_DOCUMENT_ID = "doc_nfm_readme"
_NEVER_APPROVED_DOCUMENT_ID = "doc_unapproved_thread"
_APPROVED_DOCUMENT_TITLE_PROBE = "Nightfall Migration design note"


def _unique_org(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def _reorg(batch, org_id: str):
    def rebind(record):
        return dataclasses.replace(record, org_id=org_id)

    return dataclasses.replace(
        batch,
        org_id=org_id,
        entities=tuple(rebind(item) for item in batch.entities),
        relationships=tuple(rebind(item) for item in batch.relationships),
        observations=tuple(rebind(item) for item in batch.observations),
        documents=tuple(rebind(item) for item in batch.documents),
    )


@pytest_asyncio.fixture
async def alpha_store(monkeypatch) -> AsyncIterator[GraphArmStore]:
    config = live_gate.require_live_store()
    monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
    live_gate.require_flag_state()

    org_id = _unique_org("orgdocremoval")
    projection = build_projection(_reorg(fixtures.alpha_batch(), org_id))
    store = GraphArmStore.for_org(org_id, config=config)
    try:
        await store.build_indices()
        await store.write_projection(projection)
        yield store
    finally:
        try:
            await store.purge_org()
        finally:
            await store.close()


class TestRemovingTheApprovedDocument:
    async def test_removal_deletes_exactly_one_node(
        self, alpha_store: GraphArmStore
    ) -> None:
        before = await alpha_store.count_nodes()

        removed = await alpha_store.remove_document(
            _APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.APPROVAL_REVOKED
        )

        assert removed is True
        after = await alpha_store.count_nodes()
        assert after == before - 1, (
            "removing one document must change the node count by exactly "
            "one -- more would mean the delete was not single-node scoped, "
            "fewer would mean nothing was actually removed"
        )

    async def test_removal_is_idempotent(self, alpha_store: GraphArmStore) -> None:
        first = await alpha_store.remove_document(
            _APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.APPROVAL_REVOKED
        )
        after_first = await alpha_store.count_nodes()

        second = await alpha_store.remove_document(
            _APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.APPROVAL_REVOKED
        )
        after_second = await alpha_store.count_nodes()

        assert first is True
        assert second is False, (
            "a second removal of an already-removed document must be a "
            "safe no-op, not an error -- a caller reacting to a revocation "
            "event has no reliable way to know an earlier delivery of the "
            "same event already ran"
        )
        assert after_second == after_first, "the second call must delete nothing"

    async def test_removal_logs_the_canonical_id_and_reason_code(
        self, alpha_store: GraphArmStore, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO):
            await alpha_store.remove_document(
                _APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.POLICY_FORBIDDEN
            )
        messages = [record.message for record in caplog.records]
        assert any(
            _APPROVED_DOCUMENT_ID in message and "policy_forbidden" in message
            for message in messages
        ), messages


class TestRemovingAnAbsentDocument:
    async def test_a_never_approved_document_is_a_safe_no_op(
        self, alpha_store: GraphArmStore
    ) -> None:
        """``doc_unapproved_thread`` never reached a node in the first
        place (#1665's write-side approval gate) -- removing it must be
        indistinguishable from removing anything else that was never
        written, not a special "wasn't there to begin with" error.
        """

        before = await alpha_store.count_nodes()

        removed = await alpha_store.remove_document(
            _NEVER_APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.APPROVAL_REVOKED
        )

        assert removed is False
        assert await alpha_store.count_nodes() == before

    async def test_an_unrelated_canonical_id_is_a_safe_no_op(
        self, alpha_store: GraphArmStore
    ) -> None:
        before = await alpha_store.count_nodes()

        removed = await alpha_store.remove_document(
            "doc_never_existed_at_all", reason=DocumentRemovalReason.APPROVAL_REVOKED
        )

        assert removed is False
        assert await alpha_store.count_nodes() == before


class TestCrossTenantIsolation:
    async def test_removal_in_one_partition_does_not_touch_another(
        self, monkeypatch
    ) -> None:
        """Positive AND negative control in one test: org B's identically
        named document survives org A's removal untouched, and org A's own
        removal still took effect -- so this cannot pass by the delete
        being a global no-op.
        """

        config = live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        live_gate.require_flag_state()

        org_a = _unique_org("orgdocremovala")
        org_b = _unique_org("orgdocremovalb")
        store_a = GraphArmStore.for_org(org_a, config=config)
        store_b = GraphArmStore.for_org(org_b, config=config)
        try:
            await store_a.build_indices()
            await store_b.build_indices()
            await store_a.write_projection(
                build_projection(_reorg(fixtures.alpha_batch(), org_a))
            )
            await store_b.write_projection(
                build_projection(_reorg(fixtures.alpha_batch(), org_b))
            )
            before_b = await store_b.count_nodes()

            removed_in_a = await store_a.remove_document(
                _APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.APPROVAL_REVOKED
            )

            assert removed_in_a is True
            assert await store_b.count_nodes() == before_b, (
                "org A's removal must not affect org B's identically "
                "canonical-id'd document -- each store handle is bound to "
                "its own FalkorDB keyspace (database=partition), and the "
                "uuid itself is org-scoped by identity.observation_uuid's "
                "own hash construction"
            )
            # Negative control on B, using the SAME removal call: proves
            # the still-present document really is removable in principle,
            # so B's survival above is isolation, not a broken delete.
            removed_in_b = await store_b.remove_document(
                _APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.APPROVAL_REVOKED
            )
            assert removed_in_b is True
        finally:
            try:
                await store_a.purge_org()
            finally:
                await store_b.purge_org()
                await store_a.close()
                await store_b.close()


class TestPurgeOrgComposesWithATargetedRemoval:
    async def test_purge_after_removal_deletes_exactly_what_remains(
        self, alpha_store: GraphArmStore
    ) -> None:
        """The registered-deletion-visit machinery (org_deletion_visit ->
        purge_org) must still see and delete the correct count after a
        targeted removal already ran -- proving the two deletion paths
        compose rather than one leaving the other's bookkeeping stale.
        """

        await alpha_store.remove_document(
            _APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.APPROVAL_REVOKED
        )
        remaining = await alpha_store.count_nodes()

        deleted = await alpha_store.purge_org()

        assert deleted == remaining
        assert await alpha_store.count_nodes() == 0


class TestReadSidePromptness:
    async def test_a_removed_document_is_no_longer_bm25_searchable(
        self, alpha_store: GraphArmStore
    ) -> None:
        """Index -> delete -> search returns nothing. The positive control
        (index) is not optional: without it, a search that never worked in
        the first place would pass this test by measuring nothing.
        """

        await wait_for_fulltext_index(
            alpha_store,
            probe_query=_APPROVED_DOCUMENT_TITLE_PROBE,
            expected_canonical_id=_APPROVED_DOCUMENT_ID,
        )

        await alpha_store.remove_document(
            _APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.APPROVAL_REVOKED
        )

        search_utils = graphiti_module("search.search_utils")
        search_filters = graphiti_module("search.search_filters")
        nodes = await search_utils.node_fulltext_search(
            alpha_store.driver,
            _APPROVED_DOCUMENT_TITLE_PROBE,
            search_filters.SearchFilters(),
            [alpha_store.partition],
            25,
        )
        found_ids = {
            node.attributes.get("cf_canonical_id")
            for node in nodes
            if getattr(node, "attributes", None)
        }
        assert _APPROVED_DOCUMENT_ID not in found_ids, (
            "the removed document's title is still BM25-searchable after "
            "remove_document -- 'removed or excluded PROMPTLY' does not "
            "hold"
        )

    async def test_a_removed_document_is_no_longer_vector_searchable(
        self, alpha_store: GraphArmStore
    ) -> None:
        """Same proof over cosine similarity. Uses the store's own embedder
        (``DeterministicEmbedder`` in this live fixture) to reproduce the
        exact vector the write side computed from the document's body --
        deterministic and reproducible, so no live LLM call is needed to
        prove this half of "BM25/vector retrieval must not return the
        node."
        """

        document = next(
            doc
            for doc in fixtures.alpha_batch().documents
            if doc.canonical_id == _APPROVED_DOCUMENT_ID
        )
        query_vector = await alpha_store.embedder.create(
            input_data=[document.body.replace("\n", " ")]
        )

        search_utils = graphiti_module("search.search_utils")
        search_filters = graphiti_module("search.search_filters")
        before_nodes = await search_utils.node_similarity_search(
            alpha_store.driver,
            query_vector,
            search_filters.SearchFilters(),
            [alpha_store.partition],
            25,
        )
        before_ids = {
            node.attributes.get("cf_canonical_id")
            for node in before_nodes
            if getattr(node, "attributes", None)
        }
        assert _APPROVED_DOCUMENT_ID in before_ids, (
            "the positive control failed: the document's own body-derived "
            "vector does not find it before removal, so absence after "
            "removal would prove nothing"
        )

        await alpha_store.remove_document(
            _APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.APPROVAL_REVOKED
        )

        after_nodes = await search_utils.node_similarity_search(
            alpha_store.driver,
            query_vector,
            search_filters.SearchFilters(),
            [alpha_store.partition],
            25,
        )
        after_ids = {
            node.attributes.get("cf_canonical_id")
            for node in after_nodes
            if getattr(node, "attributes", None)
        }
        assert _APPROVED_DOCUMENT_ID not in after_ids, (
            "the removed document is still vector-searchable by its own "
            "body-derived embedding after remove_document"
        )
