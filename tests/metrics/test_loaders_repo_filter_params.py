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


@pytest.mark.asyncio
async def test_testops_join_predicates_scope_org_id(mock_query_dicts):
    """Joins must carry org_id equality so cross-org child rows never match."""
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_testops_pipeline_data(start, end, repo_id=uuid.uuid4())
    job_sql = mock_query_dicts.call_args_list[1].args[1]
    assert "(p.org_id = j.org_id)" in job_sql

    mock_query_dicts.reset_mock()
    await loader.load_testops_test_data(start, end, repo_id=uuid.uuid4())
    case_sql = mock_query_dicts.call_args_list[1].args[1]
    assert "(s.org_id = c.org_id)" in case_sql


@pytest.mark.asyncio
async def test_testops_test_data_org_scope_already_applied_without_repo_id(
    mock_query_dicts,
):
    """Audit for CHAOS-4350's stated cause: verified NOT reproducible.

    The ticket's code-argued cause was "case_query filters ONLY on the time
    window -- no org_id, no repo_id". That does not hold against this tree:
    both the suite and case queries carry ``AND org_id = {org_id:String}``
    (case query via the aliased ``s.org_id``) regardless of whether
    ``repo_id`` is passed -- this is the org-wide (repo_id=None) shape the
    daily job actually uses. This test pins that so a future edit cannot
    silently regress it; the real CHAOS-4350 defect (unbounded row
    materialization) is covered by the row-cap tests below.
    """
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_testops_test_data(start, end, repo_id=None)

    assert mock_query_dicts.call_count == 2
    suite_sql = mock_query_dicts.call_args_list[0].args[1]
    case_sql = mock_query_dicts.call_args_list[1].args[1]
    suite_params = mock_query_dicts.call_args_list[0].args[2]
    case_params = mock_query_dicts.call_args_list[1].args[2]
    assert "AND org_id = {org_id:String}" in suite_sql
    assert "AND s.org_id = {org_id:String}" in case_sql
    assert suite_params.get("org_id") == "acme-corp"
    assert case_params.get("org_id") == "acme-corp"


@pytest.mark.asyncio
async def test_testops_test_data_queries_are_row_capped(mock_query_dicts, monkeypatch):
    """CHAOS-4350: both testops queries carry a hard LIMIT.

    `query_dicts` materializes the full result with
    ``list(result.result_rows)`` -- no LIMIT, no streaming -- so without a
    cap here, a single org's rolling 30-day test-case history (the window
    `load_testops_test_data` is actually called with per backfilled day,
    org-wide since repo_id is frequently None) has no bound. This is what
    produced the observed MemoryError, independent of org scoping.
    """
    monkeypatch.setenv("DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS", "500")
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_testops_test_data(start, end, repo_id=None)

    assert mock_query_dicts.call_count == 2
    for call in mock_query_dicts.call_args_list:
        sql = call.args[1]
        params = call.args[2]
        assert "LIMIT {_row_cap:UInt64}" in sql
        assert params.get("_row_cap") == 501  # max_rows + 1, to detect truncation


@pytest.mark.asyncio
async def test_testops_test_data_truncates_oversized_result_loudly(monkeypatch):
    """Red on unmodified origin/main: no cap existed, so an oversized organic
    result (more rows than the configured cap) was returned/materialized in
    full rather than bounded. After the fix, the loader truncates to the cap
    and records a Prometheus counter + log line -- never silently.
    """
    from dev_health_ops.metrics.prometheus import (
        DEV_HEALTH_TESTOPS_LOADER_ROWS_TRUNCATED_TOTAL,
    )

    monkeypatch.setenv("DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS", "3")
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    oversized_suite_rows = [{"repo_id": "r", "org_id": "acme-corp"} for _ in range(4)]
    oversized_case_rows = [{"repo_id": "r", "org_id": "acme-corp"} for _ in range(10)]

    async def fake_query_dicts(client, query, params):
        if "test_case_results" in query and "INNER JOIN" in query:
            return list(oversized_case_rows)
        return list(oversized_suite_rows)

    suite_counter = DEV_HEALTH_TESTOPS_LOADER_ROWS_TRUNCATED_TOTAL.labels(
        table="test_suite_results"
    )
    case_counter = DEV_HEALTH_TESTOPS_LOADER_ROWS_TRUNCATED_TOTAL.labels(
        table="test_case_results"
    )
    suite_before = suite_counter._value.get()
    case_before = case_counter._value.get()

    with patch(
        "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
        fake_query_dicts,
    ):
        suites, cases = await loader.load_testops_test_data(start, end, repo_id=None)

    # Bounded to the cap (3), never the oversized organic result (4 / 10).
    assert len(suites) == 3
    assert len(cases) == 3
    assert suite_counter._value.get() == suite_before + 1
    assert case_counter._value.get() == case_before + 1
