"""Tests for CHAOS-639: org_id scoping in ClickHouseDataLoader."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader


@pytest.fixture()
def mock_query_dicts():
    """Patch _clickhouse_query_dicts to capture SQL and params."""
    with patch(
        "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
        new_callable=AsyncMock,
        return_value=[],
    ) as mock:
        yield mock


@pytest.mark.asyncio
async def test_load_git_rows_includes_org_filter(mock_query_dicts):
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_git_rows(start, end, repo_id=None)

    # Three queries: commits, PRs, reviews
    assert mock_query_dicts.call_count == 3
    for call in mock_query_dicts.call_args_list:
        sql = call.args[1]
        params = call.args[2]
        assert "org_id" in params, "org_id must be in query params"
        assert params["org_id"] == "acme-corp"
        assert "org_id" in sql, "SQL must contain org_id filter"


@pytest.mark.asyncio
async def test_load_git_rows_no_org_filter_when_empty(mock_query_dicts):
    loader = ClickHouseDataLoader(client=object(), org_id="")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_git_rows(start, end, repo_id=None)

    assert mock_query_dicts.call_count == 3
    for call in mock_query_dicts.call_args_list:
        params = call.args[2]
        assert "org_id" not in params, "org_id should not be injected when empty"


@pytest.mark.asyncio
async def test_load_work_items_includes_org_filter(mock_query_dicts):
    loader = ClickHouseDataLoader(client=object(), org_id="org-123")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_work_items(start, end, repo_id=None)

    assert mock_query_dicts.call_count == 2
    for call in mock_query_dicts.call_args_list:
        params = call.args[2]
        assert params["org_id"] == "org-123"


@pytest.mark.asyncio
async def test_load_cicd_data_includes_org_filter(mock_query_dicts):
    loader = ClickHouseDataLoader(client=object(), org_id="org-456")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_cicd_data(start, end, repo_id=None)

    assert mock_query_dicts.call_count == 2
    for call in mock_query_dicts.call_args_list:
        params = call.args[2]
        assert params["org_id"] == "org-456"


@pytest.mark.asyncio
async def test_load_incidents_includes_org_filter(mock_query_dicts):
    loader = ClickHouseDataLoader(client=object(), org_id="org-789")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_incidents(start, end, repo_id=None)

    assert mock_query_dicts.call_count == 1
    params = mock_query_dicts.call_args.args[2]
    assert params["org_id"] == "org-789"


@pytest.mark.asyncio
async def test_load_atlassian_schedules_includes_org_filter(mock_query_dicts):
    loader = ClickHouseDataLoader(client=object(), org_id="org-ops")

    await loader.load_atlassian_ops_schedules()

    assert mock_query_dicts.call_count == 1
    sql = mock_query_dicts.call_args.args[1]
    params = mock_query_dicts.call_args.args[2]
    assert params["org_id"] == "org-ops"
    assert "WHERE" in sql, "Must have WHERE clause when org_id is set"


@pytest.mark.asyncio
async def test_load_user_metrics_includes_org_filter(mock_query_dicts):
    loader = ClickHouseDataLoader(client=object(), org_id="org-metrics")

    await loader.load_user_metrics_rolling_30d(as_of=date(2026, 2, 27))

    assert mock_query_dicts.call_count == 1
    sql = mock_query_dicts.call_args.args[1]
    params = mock_query_dicts.call_args.args[2]
    assert params["org_id"] == "org-metrics"
    assert "org_id" in sql


@pytest.mark.asyncio
async def test_backward_compat_no_org_id(mock_query_dicts):
    """ClickHouseDataLoader(client) without org_id still works."""
    loader = ClickHouseDataLoader(client=object())
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_git_rows(start, end, repo_id=uuid.uuid4())

    assert mock_query_dicts.call_count == 3
    for call in mock_query_dicts.call_args_list:
        params = call.args[2]
        assert "org_id" not in params
