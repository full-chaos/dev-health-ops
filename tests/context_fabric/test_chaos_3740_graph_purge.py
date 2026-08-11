"""CHAOS-3740: graph purge retries must remove orphaned watermark keys."""

from __future__ import annotations

from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.org_deletion import (
    DeletionResult,
    OrganizationDeletionService,
)
from dev_health_ops.context_fabric.graph_arm import store as store_module
from dev_health_ops.context_fabric.graph_arm.backend import DeterministicEmbedder
from dev_health_ops.context_fabric.graph_arm.flags import TrialStoreConfig
from dev_health_ops.context_fabric.graph_arm.store import GraphArmStore


class _FakeConnection:
    def __init__(self, *, fail_first_delete: bool = False) -> None:
        self.watermark_present = True
        self.delete_calls = 0
        self._fail_first_delete = fail_first_delete

    async def delete(self, _key: str) -> int:
        self.delete_calls += 1
        if self._fail_first_delete:
            self._fail_first_delete = False
            raise RuntimeError("simulated watermark deletion failure")
        self.watermark_present = False
        return 1

    async def get(self, _key: str) -> bytes | None:
        return b"{}" if self.watermark_present else None


class _FakeGraph:
    def __init__(self, client: _FakeClient) -> None:
        self._client = client

    async def delete(self) -> None:
        self._client.graph_present = False


class _FakeClient:
    def __init__(self) -> None:
        self.graph_present = True
        self.connection = _FakeConnection(fail_first_delete=True)
        self.partition = ""

    async def list_graphs(self) -> list[str]:
        return [self.partition] if self.graph_present else []

    def select_graph(self, _partition: str) -> _FakeGraph:
        return _FakeGraph(self)


class _FakeDriver:
    def __init__(self) -> None:
        self.client = _FakeClient()

    async def execute_query(
        self, _query: str
    ) -> tuple[list[dict[str, Any]], None, None]:
        return ([{"total": 3}], None, None)


class _FakeBareClient:
    def __init__(self) -> None:
        self.connection = _FakeConnection()
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


def _store() -> tuple[GraphArmStore, _FakeDriver]:
    driver = _FakeDriver()
    store = GraphArmStore(
        org_id="org_chaos_3740",
        driver=driver,
        embedder=DeterministicEmbedder(),
    )
    driver.client.partition = store.partition
    return store, driver


@pytest.mark.asyncio
async def test_retry_cleans_watermark_after_partition_delete_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcomes: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        store_module,
        "record_context_fabric_graph_purge",
        lambda *, outcome, dry_run: outcomes.append((outcome, dry_run)),
    )
    store, driver = _store()

    with pytest.raises(RuntimeError, match="simulated watermark deletion failure"):
        await store.purge_org()

    assert driver.client.graph_present is False
    assert driver.client.connection.watermark_present is True
    assert outcomes == [("failed", False)]

    assert await store.purge_org() == 0
    assert driver.client.connection.delete_calls == 2
    assert driver.client.connection.watermark_present is False
    assert outcomes == [("failed", False), ("absent", False)]


@pytest.mark.asyncio
async def test_dry_run_does_not_clean_an_absent_partition_watermark() -> None:
    store, driver = _store()
    driver.client.graph_present = False

    assert await store.purge_org(dry_run=True) == 0
    assert driver.client.connection.delete_calls == 0
    assert driver.client.connection.watermark_present is True


@pytest.mark.asyncio
@pytest.mark.parametrize("dry_run", [False, True])
async def test_registered_org_deletion_handles_absent_partition_watermark(
    monkeypatch: pytest.MonkeyPatch,
    dry_run: bool,
) -> None:
    """The registry path repairs retries but keeps dry-run read-only."""

    config = TrialStoreConfig(uri="falkor://127.0.0.1:6389")
    bare_client = _FakeBareClient()
    created_clients: list[_FakeBareClient] = []

    class _FakeFalkorDBModule:
        @staticmethod
        def FalkorDB(**_kwargs: Any) -> _FakeBareClient:
            created_clients.append(bare_client)
            return bare_client

        @staticmethod
        def FalkorDriver(**_kwargs: Any) -> None:
            raise AssertionError("absent cleanup must not construct a graph driver")

    monkeypatch.setattr(store_module, "trial_store_config", lambda: config)

    async def absent(*_args: Any, **_kwargs: Any) -> bool:
        return False

    monkeypatch.setattr(store_module, "partition_exists_for", absent)
    monkeypatch.setattr(
        store_module,
        "graphiti_module",
        lambda dotted="": _FakeFalkorDBModule,
    )

    service = OrganizationDeletionService(cast(AsyncSession, None))
    result = DeletionResult(organization_id="org_chaos_3740", dry_run=dry_run)
    await service._purge_external_stores(
        "org_chaos_3740", dry_run=dry_run, result=result
    )

    assert created_clients == ([] if dry_run else [bare_client])
    assert bare_client.connection.delete_calls == (0 if dry_run else 1)
    assert bare_client.connection.watermark_present is dry_run
    assert bare_client.closed is (not dry_run)
    assert result.external.tables["context_fabric_graph_trial"] == 0
    assert result.warnings == []
