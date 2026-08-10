"""Issue 3632's remaining two open acceptance items: production config
fail-closed behavior, and reason-coded document telemetry.

**Config.** ``GraphArmStore.write_projection`` reads the acceptance text's
"fail closed ... when providers/text indexing are disallowed" through two
already-existing, independently-tested mechanisms:

* :func:`~.flags.graph_projection_enabled` defaults off
  (``test_chaos_3617_operational_controls.py::TestFlagsDefaultOff``);
* :meth:`~.backend.CloudEmbedder.from_environment` refuses to degrade to a
  hash embedder when no credential is configured
  (``test_chaos_3617_semantic_claims.py::TestEmbedderContracts::
  test_the_cloud_embedder_refuses_to_degrade_silently``, and this ticket's
  own ``test_a_bare_cloud_embedder_cannot_embed_via_an_ambient_credential``,
  which closed a real gap where a bare, unconfigured ``CloudEmbedder()``
  could still embed via an ambient environment credential it never
  explicitly received).

What neither of those proves on its own: that a real
``GraphArmStore.write_projection`` call carrying an approved document
actually surfaces that refusal, and does so *before* any data reaches the
store -- not after a partial write. That is what
``TestWriteProjectionFailsClosed`` below measures.

**Telemetry.** ``record_context_fabric_documents_indexed`` /
``record_context_fabric_document_removed`` (``metrics/prometheus.py``) are
unit-tested in isolation in ``tests/test_prometheus_metrics.py``. What that
cannot measure: whether ``write_projection``/``remove_document`` actually
call them, with the real counts and reason codes a live run produces. That
is ``TestTelemetryReflectsRealOperations`` below.
"""

from __future__ import annotations

import dataclasses
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from dev_health_ops.context_fabric.graph_arm import build_projection, fixtures
from dev_health_ops.context_fabric.graph_arm.backend import CloudEmbedder
from dev_health_ops.context_fabric.graph_arm.store import (
    DocumentRemovalReason,
    GraphArmStore,
)
from dev_health_ops.metrics.prometheus import (
    CONTEXT_FABRIC_DOCUMENTS_INDEXED_TOTAL,
    CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL,
)
from tests.context_fabric import live_gate

pytestmark = [pytest.mark.graphiti, pytest.mark.asyncio]

_APPROVED_DOCUMENT_ID = "doc_nfm_readme"
_NEVER_APPROVED_DOCUMENT_ID = "doc_unapproved_thread"


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


class TestWriteProjectionFailsClosed:
    async def test_a_bare_cloud_embedder_writes_nothing_and_refuses(
        self, monkeypatch
    ) -> None:
        """A real ``write_projection`` call, not just the isolated guard.

        Against the real live store rather than a deliberately-unreachable
        endpoint: constructing ``GraphArmStore.for_org`` schedules FalkorDB's
        own background connection task as a side effect of construction
        (confirmed against ``partition_exists_for``'s own docstring), which
        made an artificially-unreachable endpoint race a spurious
        connection-refused error against the embedder guard this test
        actually wants to prove. The live store sidesteps that race, and the
        proof is stronger for it: ``count_nodes() == 0`` afterward means the
        refusal happened strictly before ``add_nodes_and_edges_bulk`` ever
        ran, not merely that the call eventually raised.
        """

        config = live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        live_gate.require_flag_state()
        monkeypatch.delenv("LLM_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)

        org_id = _unique_org("orgfailclosed")
        projection = build_projection(_reorg(fixtures.alpha_batch(), org_id))
        assert projection.approved_documents, (
            "the fixture must carry at least one approved document, or this "
            "test cannot reach the document-embedding path at all"
        )

        store = GraphArmStore.for_org(
            org_id,
            config=config,
            embedder=CloudEmbedder(),  # bare -- api_key=None, semantic=False
        )
        try:
            with pytest.raises(RuntimeError, match="refusing to fall back"):
                await store.write_projection(projection)
            assert await store.count_nodes() == 0, (
                "the refusal must happen before any node reaches the store -- "
                "a partial write here would mean 'fail closed' only bounded "
                "how much leaked, not whether anything did"
            )
        finally:
            try:
                await store.purge_org()
            finally:
                await store.close()


@pytest_asyncio.fixture
async def alpha_store(monkeypatch) -> AsyncIterator[GraphArmStore]:
    config = live_gate.require_live_store()
    monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
    live_gate.require_flag_state()

    org_id = _unique_org("orgtelemetry")
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


class TestTelemetryReflectsRealOperations:
    async def test_write_projection_records_the_real_indexed_count(
        self, monkeypatch
    ) -> None:
        """Positive control: a fresh org's write increments by exactly the
        number of approved documents the fixture carries (one), not by a
        fixed or guessed amount.
        """

        config = live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        live_gate.require_flag_state()

        org_id = _unique_org("orgtelemetryindex")
        projection = build_projection(_reorg(fixtures.alpha_batch(), org_id))
        assert len(projection.approved_documents) == 1, (
            "test assumes exactly one approved document; fixture drifted"
        )

        before = CONTEXT_FABRIC_DOCUMENTS_INDEXED_TOTAL._value.get()

        store = GraphArmStore.for_org(org_id, config=config)
        try:
            await store.build_indices()
            await store.write_projection(projection)
        finally:
            try:
                await store.purge_org()
            finally:
                await store.close()

        after = CONTEXT_FABRIC_DOCUMENTS_INDEXED_TOTAL._value.get()
        assert after == before + 1

    async def test_remove_document_records_the_reason_it_was_given(
        self, alpha_store: GraphArmStore
    ) -> None:
        before = CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL.labels(
            reason=DocumentRemovalReason.POLICY_FORBIDDEN.value
        )._value.get()

        removed = await alpha_store.remove_document(
            _APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.POLICY_FORBIDDEN
        )

        assert removed is True
        after = CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL.labels(
            reason=DocumentRemovalReason.POLICY_FORBIDDEN.value
        )._value.get()
        assert after == before + 1

    async def test_a_no_op_removal_does_not_inflate_the_counter(
        self, alpha_store: GraphArmStore
    ) -> None:
        """Negative control, paired with the test above: removing a document
        that was never approved (never reached a node) must not be counted
        as a real removal -- the counter measures index shrinkage, not call
        volume.
        """

        before = CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL.labels(
            reason=DocumentRemovalReason.APPROVAL_REVOKED.value
        )._value.get()

        removed = await alpha_store.remove_document(
            _NEVER_APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.APPROVAL_REVOKED
        )

        assert removed is False
        after = CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL.labels(
            reason=DocumentRemovalReason.APPROVAL_REVOKED.value
        )._value.get()
        assert after == before
