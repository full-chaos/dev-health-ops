from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.api.ingest.persist import (
    _get_ingest_settings,
    _repo_id_from_url,
    persist_items,
)
from dev_health_ops.storage.clickhouse import ClickHouseStore
from dev_health_ops.storage.operational_ordering_guard import (
    OperationalOrderingContract,
)


class TestRepoIdFromUrl:
    def test_deterministic(self):
        url = "https://github.com/acme/app"
        assert _repo_id_from_url(url) == _repo_id_from_url(url)

    def test_different_urls_differ(self):
        assert _repo_id_from_url("https://a.com/x") != _repo_id_from_url(
            "https://b.com/y"
        )

    def test_returns_uuid(self):
        result = _repo_id_from_url("https://github.com/org/repo")
        assert isinstance(result, uuid.UUID)


class TestGetIngestSettings:
    def test_default_returns_async_settings(self, monkeypatch):
        monkeypatch.delenv("INGEST_ASYNC_INSERT", raising=False)
        settings = _get_ingest_settings()
        assert settings["async_insert"] == 1
        assert settings["wait_for_async_insert"] == 1
        assert "async_insert_busy_timeout_ms" in settings

    def test_disabled_returns_empty(self, monkeypatch):
        monkeypatch.setenv("INGEST_ASYNC_INSERT", "0")
        settings = _get_ingest_settings()
        assert settings == {}

    def test_explicit_enabled(self, monkeypatch):
        monkeypatch.setenv("INGEST_ASYNC_INSERT", "1")
        settings = _get_ingest_settings()
        assert settings["async_insert"] == 1


def _mock_store():
    store = MagicMock(spec=ClickHouseStore)
    store.insert_git_commit_data = AsyncMock()
    store.insert_git_pull_requests = AsyncMock()
    store.insert_work_items = AsyncMock()
    store.insert_deployments = AsyncMock()
    store.insert_operational_services = AsyncMock()
    store.insert_operational_incidents = AsyncMock()
    store.insert_operational_service_repository_mappings = AsyncMock()
    store.__aenter__ = AsyncMock(return_value=store)
    store.__aexit__ = AsyncMock(return_value=False)
    return store


@pytest.mark.asyncio
class TestPersistCommits:
    async def test_calls_store(self, monkeypatch):
        monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://localhost")
        store = _mock_store()
        with patch(
            "dev_health_ops.api.ingest.persist.ClickHouseStore",
            return_value=store,
        ):
            items = [
                {
                    "hash": "abc123",
                    "message": "fix",
                    "_repo_url": "https://github.com/org/repo",
                    "_org_id": "default",
                    "_ingestion_id": "ing-1",
                }
            ]
            count = await persist_items("commits", items)

        assert count == 1
        store.insert_git_commit_data.assert_awaited_once()
        call_args = store.insert_git_commit_data.call_args[0][0]
        assert "repo_id" in call_args[0]
        assert "_repo_url" not in call_args[0]
        assert "_org_id" not in call_args[0]


@pytest.mark.asyncio
class TestPersistPullRequests:
    async def test_calls_store(self, monkeypatch):
        monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://localhost")
        store = _mock_store()
        with patch(
            "dev_health_ops.api.ingest.persist.ClickHouseStore",
            return_value=store,
        ):
            items = [
                {
                    "number": 42,
                    "title": "feat",
                    "_repo_url": "https://github.com/org/repo",
                    "_org_id": "default",
                    "_ingestion_id": "ing-1",
                    "reviews": [{"author": "bob"}],
                }
            ]
            count = await persist_items("pull-requests", items)

        assert count == 1
        store.insert_git_pull_requests.assert_awaited_once()
        call_args = store.insert_git_pull_requests.call_args[0][0]
        assert "repo_id" in call_args[0]
        assert "reviews" not in call_args[0]


@pytest.mark.asyncio
class TestPersistWorkItems:
    async def test_calls_store(self, monkeypatch):
        monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://localhost")
        store = _mock_store()
        with patch(
            "dev_health_ops.api.ingest.persist.ClickHouseStore",
            return_value=store,
        ):
            items = [
                {
                    "work_item_id": "jira:PROJ-1",
                    "provider": "jira",
                    "_repo_url": "",
                    "_org_id": "default",
                    "_ingestion_id": "ing-1",
                }
            ]
            count = await persist_items("work-items", items)

        assert count == 1
        store.insert_work_items.assert_awaited_once()
        call_args = store.insert_work_items.call_args[0][0]
        assert "_repo_url" not in call_args[0]
        assert "_org_id" not in call_args[0]


@pytest.mark.asyncio
class TestPersistDeployments:
    async def test_calls_store(self, monkeypatch):
        monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://localhost")
        store = _mock_store()
        with patch(
            "dev_health_ops.api.ingest.persist.ClickHouseStore",
            return_value=store,
        ):
            items = [
                {
                    "deployment_id": "d-1",
                    "status": "success",
                    "_repo_url": "https://github.com/org/repo",
                    "_org_id": "default",
                    "_ingestion_id": "ing-1",
                }
            ]
            count = await persist_items("deployments", items)

        assert count == 1
        store.insert_deployments.assert_awaited_once()
        call_args = store.insert_deployments.call_args[0][0]
        assert "repo_id" in call_args[0]
        assert call_args[0]["release_ref"] == "d-1"
        assert call_args[0]["release_ref_confidence"] == pytest.approx(0.3)


@pytest.mark.asyncio
class TestPersistIncidents:
    async def test_calls_store(self, monkeypatch):
        monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://localhost")
        store = _mock_store()
        with patch(
            "dev_health_ops.api.ingest.persist.ClickHouseStore",
            return_value=store,
        ):
            items = [
                {
                    "incident_id": "inc-1",
                    "status": "resolved",
                    "started_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
                    "resolved_at": datetime(2026, 7, 1, 1, tzinfo=timezone.utc),
                    "_repo_url": "https://github.com/org/repo",
                    "_org_id": "default",
                    "_ingestion_id": "ing-1",
                }
            ]
            count = await persist_items("incidents", items)

        assert count == 1
        store.insert_operational_services.assert_awaited_once()
        store.insert_operational_service_repository_mappings.assert_awaited_once()
        store.insert_operational_incidents.assert_awaited_once()
        incident = store.insert_operational_incidents.await_args.args[0][0]
        assert incident.provider == "external"
        assert incident.provider_instance_id == "legacy-repository-ingest"
        assert incident.external_id == "inc-1"
        assert incident.normalized_status == "resolved"


@pytest.mark.asyncio
class TestPersistEdgeCases:
    async def test_unknown_entity_returns_zero(self, monkeypatch):
        monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://localhost")
        store = _mock_store()
        with patch(
            "dev_health_ops.api.ingest.persist.ClickHouseStore",
            return_value=store,
        ):
            count = await persist_items("unknown-type", [{"id": "1"}])
        assert count == 0

    async def test_no_clickhouse_uri_returns_zero(self, monkeypatch):
        monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
        monkeypatch.delenv("DATABASE_URI", raising=False)
        count = await persist_items("commits", [{"hash": "abc"}])
        assert count == 0


class TestClickHouseStoreSettings:
    def test_settings_passthrough(self):
        store = ClickHouseStore(
            "clickhouse://localhost",
            settings={"async_insert": 1},
        )
        assert store._settings == {"async_insert": 1}

    def test_default_settings_empty(self):
        store = ClickHouseStore("clickhouse://localhost")
        assert store._settings == {}

    def test_explicit_operational_ordering_contract(self):
        store = ClickHouseStore(
            "clickhouse://localhost",
            operational_ordering_contract=OperationalOrderingContract.CURRENT,
        )
        assert (
            store._operational_ordering_contract is OperationalOrderingContract.CURRENT
        )
        assert store._operational_ordering_contract_is_explicit

    def test_rejects_raw_operational_ordering_contract(self):
        with pytest.raises(TypeError, match="must be an OperationalOrderingContract"):
            ClickHouseStore(
                "clickhouse://localhost",
                operational_ordering_contract=cast(OperationalOrderingContract, 2),
            )

    @pytest.mark.asyncio
    async def test_insert_rows_passes_settings(self):
        settings = {"async_insert": 1, "wait_for_async_insert": 1}
        store = ClickHouseStore("clickhouse://localhost", settings=settings)
        mock_client = MagicMock()
        mock_client.insert = MagicMock()
        store.client = mock_client

        rows = [{"col_a": "val1", "col_b": "val2"}]
        with patch("asyncio.to_thread", new_callable=AsyncMock) as mock_thread:
            await store._insert_rows("test_table", ["col_a", "col_b"], rows)

        mock_thread.assert_awaited_once()
        call_kwargs = mock_thread.call_args
        assert call_kwargs.kwargs.get("settings") == settings
