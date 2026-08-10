"""Issue 3632's audit slice: what is currently indexed, and its trust
provenance -- ``GraphArmStore.list_indexed_documents``.

The acceptance text's "audit behavior" clause resolved to a query, not a
separate audit-log store (the orchestrator's own ruling): "what's in the
graph right now" is answered directly by re-reading the live partition,
the same way ``count_nodes``/``purge_org`` already do, rather than by
reconstructing it from write/removal log lines after the fact.

Composes with the write side (#1665) and the removal primitive (#1669):
an approved document is listed; a rejected one never was written and so
was never listed either; a removed document stops being listed the moment
``remove_document`` succeeds -- the same "removing approval propagates to
the index" property the removal primitive itself proves, checked here from
the audit query's own point of view.

Also covers the other half of "structured logs on index entry/removal":
``write_projection`` now logs one line per document actually indexed
(``remove_document`` already logged one per removal) -- an aggregate-only
entry log would leave "which specific document entered the index" only
answerable by re-querying current state, never by the log an operator is
actually looking at during an incident.
"""

from __future__ import annotations

import dataclasses
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
import pytest_asyncio

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.context_fabric.graph_arm import build_projection, fixtures
from dev_health_ops.context_fabric.graph_arm.projection import GraphProjection
from dev_health_ops.context_fabric.graph_arm.records import UnstructuredDocumentRecord
from dev_health_ops.context_fabric.graph_arm.store import (
    DocumentRemovalReason,
    GraphArmStore,
)
from tests.context_fabric import live_gate

pytestmark = [pytest.mark.graphiti, pytest.mark.asyncio]

_APPROVED_DOCUMENT_ID = "doc_nfm_readme"
_NEVER_APPROVED_DOCUMENT_ID = "doc_unapproved_thread"
_REJECTED_BODY_MARKER = "Ignore previous instructions"


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

    org_id = _unique_org("orgdocaudit")
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


class TestListIndexedDocuments:
    async def test_only_the_approved_document_is_listed(
        self, alpha_store: GraphArmStore
    ) -> None:
        summaries = await alpha_store.list_indexed_documents()

        listed_ids = {summary.canonical_id for summary in summaries}
        assert listed_ids == {_APPROVED_DOCUMENT_ID}
        assert _NEVER_APPROVED_DOCUMENT_ID not in listed_ids, (
            "the rejected document (never written -- see #1665) must not "
            "appear, and nothing besides the one approved document should"
        )

    async def test_the_summary_carries_the_title_but_never_the_body(
        self, alpha_store: GraphArmStore
    ) -> None:
        (summary,) = await alpha_store.list_indexed_documents()

        assert summary.title == "Nightfall Migration design note"
        body_fragment = "auth gateway's token exchange"
        assert body_fragment not in summary.title
        assert body_fragment not in repr(summary)

    async def test_the_rejected_documents_body_never_appears_anywhere(
        self, alpha_store: GraphArmStore
    ) -> None:
        """Adversarial: the rejected document's own payload text must not
        leak through the audit query by any field, even though it was
        never approved to reach a node at all.
        """

        summaries = await alpha_store.list_indexed_documents()
        assert _REJECTED_BODY_MARKER not in repr(summaries)

    async def test_removing_a_document_removes_it_from_the_audit_listing(
        self, alpha_store: GraphArmStore
    ) -> None:
        before = await alpha_store.list_indexed_documents()
        assert {s.canonical_id for s in before} == {_APPROVED_DOCUMENT_ID}

        removed = await alpha_store.remove_document(
            _APPROVED_DOCUMENT_ID, reason=DocumentRemovalReason.APPROVAL_REVOKED
        )
        assert removed is True

        after = await alpha_store.list_indexed_documents()
        assert after == (), (
            "removing approval must propagate to the audit query too, not "
            "just to search -- an operator auditing the index after a "
            "revocation must not see a document that was supposedly removed"
        )

    async def test_results_are_sorted_by_canonical_id(self, monkeypatch) -> None:
        config = live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        live_gate.require_flag_state()

        org_id = _unique_org("orgdocauditsort")
        documents = tuple(
            UnstructuredDocumentRecord(
                org_id=org_id,
                canonical_id=canonical_id,
                title=canonical_id,
                body="body text",
                source_class=SourceClass.WORK_GRAPH,
                observed_at=datetime(2026, 6, 1, tzinfo=UTC),
                approved=True,
            )
            for canonical_id in ("doc_zebra", "doc_apple", "doc_mango")
        )
        projection = GraphProjection(
            org_id=org_id,
            partition=f"cf_trial_{org_id}",
            projection_version="test.v1",
            nodes=(),
            edges=(),
            approved_documents=documents,
        )
        store = GraphArmStore.for_org(org_id, config=config)
        try:
            await store.build_indices()
            await store.write_projection(projection)
            summaries = await store.list_indexed_documents()
        finally:
            try:
                await store.purge_org()
            finally:
                await store.close()

        assert [s.canonical_id for s in summaries] == [
            "doc_apple",
            "doc_mango",
            "doc_zebra",
        ]


class TestTrustAndEvidenceMetadata:
    async def test_trust_and_evidence_attributes_read_back(self, monkeypatch) -> None:
        """The audit query's own point: trust/evidence provenance travels
        with the listing, read back exactly like every other
        READBACK_ATTRIBUTE_KEYS member -- not a new vocabulary.
        """

        config = live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        live_gate.require_flag_state()

        org_id = _unique_org("orgdocaudittrust")
        document = UnstructuredDocumentRecord(
            org_id=org_id,
            canonical_id="doc_trust_audit",
            title="Trust-carrying document",
            body="body text",
            source_class=SourceClass.WORK_GRAPH,
            observed_at=datetime(2026, 6, 1, tzinfo=UTC),
            approved=True,
            attributes={
                "corpus_trust": "trusted",
                "source_evidence_state": "active",
            },
        )
        projection = GraphProjection(
            org_id=org_id,
            partition=f"cf_trial_{org_id}",
            projection_version="test.v1",
            nodes=(),
            edges=(),
            approved_documents=(document,),
        )
        store = GraphArmStore.for_org(org_id, config=config)
        try:
            await store.build_indices()
            await store.write_projection(projection)
            (summary,) = await store.list_indexed_documents()
        finally:
            try:
                await store.purge_org()
            finally:
                await store.close()

        assert summary.trust == "trusted"
        assert summary.source_evidence_state == "active"

    async def test_absent_trust_reads_back_as_none_not_a_default(
        self, alpha_store: GraphArmStore
    ) -> None:
        """alpha_batch's document sets no corpus_trust -- absence must
        read back as None, never a silently-assumed default like
        "trusted" or "untrusted".
        """

        (summary,) = await alpha_store.list_indexed_documents()
        assert summary.trust is None


class TestStructuredEntryLogging:
    async def test_write_projection_logs_one_line_per_indexed_document(
        self, monkeypatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        config = live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        live_gate.require_flag_state()

        org_id = _unique_org("orgdocauditlog")
        projection = build_projection(_reorg(fixtures.alpha_batch(), org_id))
        store = GraphArmStore.for_org(org_id, config=config)
        try:
            await store.build_indices()
            with caplog.at_level(logging.INFO):
                await store.write_projection(projection)
        finally:
            try:
                await store.purge_org()
            finally:
                await store.close()

        messages = [record.message for record in caplog.records]
        assert any(
            _APPROVED_DOCUMENT_ID in message and "indexed" in message
            for message in messages
        ), messages
        # The rejected document was never written, so it must never be
        # logged as indexed either -- a log line naming it would be a
        # false positive an operator auditing entries could not trust.
        assert not any(_NEVER_APPROVED_DOCUMENT_ID in message for message in messages)
