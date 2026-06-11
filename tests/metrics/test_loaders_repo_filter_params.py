"""Tests for CHAOS-2293: aliased repo filters must not corrupt param bindings.

``repo_filter.replace("repo_id", "p.repo_id")`` rewrote the ``{repo_id:UUID}``
parameter name to ``{p.repo_id:UUID}``; ClickHouse rejects dotted parameter
names with SYNTAX_ERROR (code 62), failing every per-repo daily metrics run.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader

# Matches any ClickHouse parameter binding whose name contains a dot,
# e.g. "{p.repo_id:UUID}" — always a syntax error server-side.
DOTTED_PARAM = re.compile(r"\{[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z0-9_.]+:")


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
async def test_testops_pipeline_repo_filter_keeps_param_name(mock_query_dicts):
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_testops_pipeline_data(start, end, repo_id=uuid.uuid4())

    # Two queries: pipeline runs, job runs (the job query aliases p).
    assert mock_query_dicts.call_count == 2
    for call in mock_query_dicts.call_args_list:
        sql = call.args[1]
        params = call.args[2]
        assert "{repo_id:UUID}" in sql
        assert "repo_id" in params
        assert not DOTTED_PARAM.search(sql), (
            f"dotted param binding leaked into SQL: {sql}"
        )


@pytest.mark.asyncio
async def test_testops_test_data_repo_filter_keeps_param_name(mock_query_dicts):
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_testops_test_data(start, end, repo_id=uuid.uuid4())

    # Two queries: suite results, case results (the case query aliases s).
    assert mock_query_dicts.call_count == 2
    for call in mock_query_dicts.call_args_list:
        sql = call.args[1]
        params = call.args[2]
        assert "{repo_id:UUID}" in sql
        assert "repo_id" in params
        assert not DOTTED_PARAM.search(sql), (
            f"dotted param binding leaked into SQL: {sql}"
        )


@pytest.mark.asyncio
async def test_testops_job_and_case_queries_alias_column(mock_query_dicts):
    """The join queries must still scope the aliased column when repo_id is set."""
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_testops_pipeline_data(start, end, repo_id=uuid.uuid4())
    job_sql = mock_query_dicts.call_args_list[1].args[1]
    assert "p.repo_id = {repo_id:UUID}" in job_sql

    mock_query_dicts.reset_mock()
    await loader.load_testops_test_data(start, end, repo_id=uuid.uuid4())
    case_sql = mock_query_dicts.call_args_list[1].args[1]
    assert "s.repo_id = {repo_id:UUID}" in case_sql
