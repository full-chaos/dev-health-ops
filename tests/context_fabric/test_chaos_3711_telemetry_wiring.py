"""CHAOS-3711: production graph seams cannot silently lose telemetry."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from dev_health_ops.api.dev.contracts_v2.base import Cardinality, QuestionIntentID
from dev_health_ops.api.dev.graph_investigation_query import (
    CohortDiscoveryFamily,
    GraphInvestigationRequest,
    GraphQueryOutcome,
)
from dev_health_ops.context_fabric.graph_arm import query_service as query_module
from dev_health_ops.context_fabric.graph_arm import store as store_module
from dev_health_ops.context_fabric.graph_arm.projection import GraphProjection
from dev_health_ops.context_fabric.graph_arm.store import (
    DocumentRemovalReason,
    GraphArmStore,
    WriteResult,
)
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark


class _FakeProjectionStore:
    def __init__(self, *, purge_error: BaseException | None = None) -> None:
        self.purge_error = purge_error
        self.closed = False

    async def purge_org(self, *, dry_run: bool) -> int:
        if self.purge_error is not None:
            raise self.purge_error
        return 2

    async def close(self) -> None:
        self.closed = True


class _FakeQueryStore:
    def __init__(self, *, close_error: BaseException | None = None) -> None:
        self.close_error = close_error
        self.close_calls = 0

    async def read_watermark(self) -> IndexWatermark:
        return IndexWatermark(indexed_through=None)

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


def _bare_store() -> GraphArmStore:
    store = object.__new__(GraphArmStore)
    store._org_id = "org_telemetry_test"
    store._partition = "cf_org_telemetry_test"
    store._driver = SimpleNamespace()
    return store


def _query_request() -> GraphInvestigationRequest:
    now = datetime.now(UTC)
    return GraphInvestigationRequest(
        org_id="org_1",
        run_id="run_1",
        intent_id=QuestionIntentID.PROJECT_HEALTH,
        cardinality=Cardinality.SINGULAR,
        mentions=(),
        question_text="status",
        authorized_entity_ids=frozenset(),
        window_start=now - timedelta(days=1),
        window_end=now,
        cohort_discovery_family=CohortDiscoveryFamily.TEAM_PRESSURE,
        deadline=now + timedelta(seconds=1),
    )


@pytest.mark.asyncio
async def test_projection_records_success_failure_and_cancel_once(monkeypatch) -> None:
    outcomes: list[str] = []
    monkeypatch.setattr(
        store_module,
        "record_context_fabric_graph_projection",
        outcomes.append,
    )
    store = _bare_store()

    projection = cast(GraphProjection, object())

    async def succeed(*_args: Any, **_kwargs: Any) -> WriteResult:
        return cast(WriteResult, object())

    cast(Any, store)._write_projection = succeed
    await store.write_projection(projection)
    assert outcomes == ["completed"]

    async def fail(*_args: Any, **_kwargs: Any) -> WriteResult:
        raise RuntimeError("write failed")

    cast(Any, store)._write_projection = fail
    with pytest.raises(RuntimeError):
        await store.write_projection(projection)
    assert outcomes == ["completed", "failed"]

    async def cancel(*_args: Any, **_kwargs: Any) -> WriteResult:
        raise asyncio.CancelledError

    cast(Any, store)._write_projection = cancel
    with pytest.raises(asyncio.CancelledError):
        await store.write_projection(projection)
    assert outcomes == ["completed", "failed", "cancelled"]


@pytest.mark.asyncio
async def test_purge_records_cancel_once(monkeypatch) -> None:
    outcomes: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        store_module,
        "record_context_fabric_graph_purge",
        lambda *, outcome, dry_run: outcomes.append((outcome, dry_run)),
    )
    store = _bare_store()

    async def cancel_partition() -> bool:
        raise asyncio.CancelledError

    cast(Any, store).partition_exists = cancel_partition
    with pytest.raises(asyncio.CancelledError):
        await store.purge_org()
    assert outcomes == [("cancelled", False)]

    async def absent() -> bool:
        return False

    cast(Any, store).partition_exists = absent

    async def delete_watermark() -> None:
        pass

    cast(Any, store)._delete_watermark = delete_watermark
    assert await store.purge_org() == 0
    assert outcomes[-1] == ("absent", False)

    async def fail_partition() -> bool:
        raise RuntimeError("partition probe failed")

    cast(Any, store).partition_exists = fail_partition
    with pytest.raises(RuntimeError, match="partition probe failed"):
        await store.purge_org()
    assert outcomes[-1] == ("failed", False)


@pytest.mark.asyncio
async def test_remove_document_records_decode_failure_and_cancel_once(
    monkeypatch,
) -> None:
    outcomes: list[str] = []
    monkeypatch.setattr(
        store_module,
        "record_context_fabric_graph_document_removal",
        lambda *, outcome, reason: outcomes.append(outcome),
    )
    store = _bare_store()
    store._driver.execute_query = lambda *_args, **_kwargs: None

    async def malformed(*_args: Any, **_kwargs: Any) -> tuple[list[Any]]:
        return ([],)

    cast(Any, store)._bounded_write = malformed
    with pytest.raises(ValueError):
        await store.remove_document(
            "doc_1", reason=DocumentRemovalReason.APPROVAL_REVOKED
        )
    assert outcomes == ["failed"]

    async def cancel(*_args: Any, **_kwargs: Any) -> Any:
        raise asyncio.CancelledError

    cast(Any, store)._bounded_write = cancel
    with pytest.raises(asyncio.CancelledError):
        await store.remove_document(
            "doc_1", reason=DocumentRemovalReason.APPROVAL_REVOKED
        )
    assert outcomes == ["failed", "cancelled"]


@pytest.mark.asyncio
async def test_org_deletion_visit_records_success_and_close_failure_once(
    monkeypatch,
) -> None:
    monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_STORE_URI", "falkor://127.0.0.1:1")
    outcomes: list[str] = []
    monkeypatch.setattr(
        store_module,
        "record_context_fabric_graph_org_deletion_visit",
        lambda *, outcome, dry_run: outcomes.append(outcome),
    )

    async def exists(*_args, **_kwargs) -> bool:
        return True

    monkeypatch.setattr(store_module, "partition_exists_for", exists)
    healthy = _FakeProjectionStore()
    monkeypatch.setattr(
        store_module.GraphArmStore,
        "for_org",
        staticmethod(lambda *_args, **_kwargs: healthy),
    )
    assert await store_module.org_deletion_visit("org_1", False) == 2
    assert outcomes == ["purged"]
    assert healthy.closed is True

    class CloseFails(_FakeProjectionStore):
        async def close(self) -> None:
            raise RuntimeError("close failed")

    failing = CloseFails()
    monkeypatch.setattr(
        store_module.GraphArmStore,
        "for_org",
        staticmethod(lambda *_args, **_kwargs: failing),
    )
    with pytest.raises(RuntimeError, match="close failed"):
        await store_module.org_deletion_visit("org_1", False)
    assert outcomes == ["purged", "failed"]


@pytest.mark.asyncio
async def test_query_wiring_records_closed_outcome_and_latency(monkeypatch) -> None:
    observations: list[tuple[str, float]] = []
    monkeypatch.setattr(
        query_module,
        "record_context_fabric_graph_query",
        lambda *, outcome, duration_seconds: observations.append(
            (outcome, duration_seconds)
        ),
    )
    monkeypatch.delenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", raising=False)
    service = query_module.ProductionGraphInvestigationQuery()

    # The disabled path is an actual production seam path, not a direct
    # recorder test; removing the wrapper makes this assertion fail.
    result = await service.investigate(_query_request())
    assert result.outcome is GraphQueryOutcome.DISABLED
    assert observations and observations == [("disabled", observations[0][1])]
    assert observations[0][1] >= 0


@pytest.mark.asyncio
async def test_query_close_cancellation_returns_cancelled_and_records_once(
    monkeypatch,
) -> None:
    outcomes: list[str] = []

    def record(*, outcome: str, duration_seconds: float) -> None:
        assert duration_seconds >= 0
        outcomes.append(outcome)

    monkeypatch.setattr(query_module, "record_context_fabric_graph_query", record)
    monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
    store = _FakeQueryStore(close_error=asyncio.CancelledError())
    service = query_module.ProductionGraphInvestigationQuery(
        store_factory=lambda _org_id: store
    )

    result = await service.investigate(_query_request())

    assert result.outcome is GraphQueryOutcome.CANCELLED
    assert result.packet is None
    assert result.diagnostic is not None and "cancellation" in result.diagnostic
    assert store.close_calls == 1
    assert outcomes == ["cancelled"]


@pytest.mark.asyncio
async def test_query_ordinary_close_failure_preserves_result_and_records_once(
    monkeypatch, caplog
) -> None:
    outcomes: list[str] = []
    monkeypatch.setattr(
        query_module,
        "record_context_fabric_graph_query",
        lambda *, outcome, duration_seconds: outcomes.append(outcome),
    )
    monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
    store = _FakeQueryStore(close_error=RuntimeError("close failed"))
    service = query_module.ProductionGraphInvestigationQuery(
        store_factory=lambda _org_id: store
    )

    with caplog.at_level("WARNING"):
        result = await service.investigate(_query_request())

    assert result.outcome is GraphQueryOutcome.UNAVAILABLE
    assert store.close_calls == 1
    assert outcomes == ["unavailable"]
    assert "store.close() failed" in caplog.text


@pytest.mark.asyncio
async def test_org_deletion_visit_cancel_records_once(monkeypatch) -> None:
    monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_STORE_URI", "falkor://127.0.0.1:1")
    outcomes: list[str] = []
    monkeypatch.setattr(
        store_module,
        "record_context_fabric_graph_org_deletion_visit",
        lambda *, outcome, dry_run: outcomes.append(outcome),
    )

    async def cancel(*_args, **_kwargs) -> bool:
        raise asyncio.CancelledError

    monkeypatch.setattr(store_module, "partition_exists_for", cancel)
    with pytest.raises(asyncio.CancelledError):
        await store_module.org_deletion_visit("org_1", False)
    assert outcomes == ["cancelled"]


def test_watermark_metric_is_not_emitted_for_never_projected_lag() -> None:
    # Keep this structural and local: the recorder's unavailable branch must
    # not manufacture a zero-lag observation for a partition with no watermark.
    from dev_health_ops.metrics.prometheus import (
        CONTEXT_FABRIC_GRAPH_WATERMARK_LAG_SECONDS,
        record_context_fabric_graph_watermark,
    )

    before = CONTEXT_FABRIC_GRAPH_WATERMARK_LAG_SECONDS._sum.get()
    record_context_fabric_graph_watermark(state="unavailable", lag_seconds=0)
    assert CONTEXT_FABRIC_GRAPH_WATERMARK_LAG_SECONDS._sum.get() == before
