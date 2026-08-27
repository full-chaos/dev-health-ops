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
async def test_testops_test_data_refuses_to_compute_on_oversized_result(monkeypatch):
    """Red on unmodified origin/main (no cap exists at all -- ImportError on
    the guard's counter): an oversized organic result must FAIL the read,
    never be silently truncated and computed on.

    chris's ruling (2026-08-26): a LIMIT that lets computation proceed on a
    truncated window produces WRONG testops metrics silently-ish -- not
    allowed. `test_suite_results`/`test_case_results` are ordered by
    `(repo_id, run_id, ...)`, not event time, so an unordered LIMIT could
    drop today's rows (or whole repos) while keeping stale ones (codex
    review of the first, truncating version of this fix). So exceeding the
    cap raises `TestopsRowCapExceeded` (a `MemoryError` subclass -- see its
    docstring for why) instead of returning a partial result.
    """
    from dev_health_ops.metrics.loaders.clickhouse import TestopsRowCapExceeded
    from dev_health_ops.metrics.prometheus import (
        DEV_HEALTH_TESTOPS_LOADER_ROW_CAP_EXCEEDED_TOTAL,
    )

    monkeypatch.setenv("DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS", "3")
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    oversized_suite_rows = [{"repo_id": "r", "org_id": "acme-corp"} for _ in range(2)]
    oversized_case_rows = [{"repo_id": "r", "org_id": "acme-corp"} for _ in range(10)]

    async def fake_query_dicts(client, query, params):
        if "test_case_results" in query and "INNER JOIN" in query:
            return list(oversized_case_rows)
        return list(oversized_suite_rows)

    case_counter = DEV_HEALTH_TESTOPS_LOADER_ROW_CAP_EXCEEDED_TOTAL.labels(
        table="test_case_results"
    )
    case_before = case_counter._value.get()

    with (
        patch(
            "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
            fake_query_dicts,
        ),
        pytest.raises(TestopsRowCapExceeded) as exc_info,
    ):
        await loader.load_testops_test_data(start, end, repo_id=None)

    # A MemoryError subclass -- worker_metrics_runner.py's bare `except
    # MemoryError` classifies this the same as an rlimit OOM
    # (EXIT_RESOURCE_EXHAUSTED), reusing the existing durable
    # failure_reason='resource_exhausted' persistence path.
    assert isinstance(exc_info.value, MemoryError)
    assert exc_info.value.table == "test_case_results"
    assert exc_info.value.org_id == "acme-corp"
    # CHAOS-4350 (team-lead ruling): a fixed, SigNoz-searchable token must
    # appear verbatim so a deliberately-tripped guard is distinguishable
    # from a real unbounded OOM despite sharing the MemoryError/
    # resource_exhausted classification upstream.
    assert "testops_row_cap_exceeded" in str(exc_info.value)
    assert case_counter._value.get() == case_before + 1


@pytest.mark.asyncio
async def test_testops_test_data_checks_suite_cap_before_issuing_case_query(
    monkeypatch,
):
    """CHAOS-4350 (codex round 3 P2): if the suite read alone already
    exceeds the cap, the case query must never even be issued -- with a cap
    sized near the runner's memory budget, materializing up to another
    `max_rows + 1` case rows before checking either result could itself
    trigger an ordinary OOM before this guard's classified exception fires.
    """
    from dev_health_ops.metrics.loaders.clickhouse import TestopsRowCapExceeded

    monkeypatch.setenv("DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS", "1")
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    oversized_suite_rows = [{"repo_id": "r", "org_id": "acme-corp"} for _ in range(5)]
    case_query_calls = 0

    async def fake_query_dicts(client, query, params):
        nonlocal case_query_calls
        if "test_case_results" in query and "INNER JOIN" in query:
            case_query_calls += 1
            return []
        return list(oversized_suite_rows)

    with (
        patch(
            "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
            fake_query_dicts,
        ),
        pytest.raises(TestopsRowCapExceeded) as exc_info,
    ):
        await loader.load_testops_test_data(start, end, repo_id=None)

    assert exc_info.value.table == "test_suite_results"
    assert case_query_calls == 0, (
        "the case query was issued even though the suite read alone "
        "already exceeded the cap"
    )


@pytest.mark.asyncio
async def test_historical_failed_case_names_query_shape(mock_query_dicts):
    """CHAOS-4350 PR 2: the historical aggregate must be FINAL on both
    joined tables, org+repo scoped, capped, and status='failed'-filtered --
    pushed into SQL instead of fetching raw historical case rows.
    """
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 31, tzinfo=timezone.utc)
    repo_id = uuid.uuid4()

    await loader.load_testops_historical_failed_case_names(start, end, repo_id=repo_id)

    assert mock_query_dicts.call_count == 1
    sql = mock_query_dicts.call_args.args[1]
    params = mock_query_dicts.call_args.args[2]
    assert "test_case_results AS c FINAL" in sql
    assert "test_suite_results AS s FINAL" in sql
    assert "c.status = 'failed'" in sql
    assert "GROUP BY s.repo_id, c.case_name" in sql
    assert "s.repo_id = {repo_id:UUID}" in sql
    assert "AND s.org_id = {org_id:String}" in sql
    assert "LIMIT {_row_cap:UInt64}" in sql
    assert params.get("repo_id") == str(repo_id)
    assert params.get("org_id") == "acme-corp"
    assert params.get("start") == start.replace(tzinfo=None)
    assert params.get("end") == end.replace(tzinfo=None)
    assert not DOTTED_PARAM.search(sql)


@pytest.mark.asyncio
async def test_historical_failed_case_names_groups_by_repo(mock_query_dicts):
    """Returns a dict[repo_id, set[case_name]] built from the aggregate rows,
    and org-wide (repo_id=None) calls still separate results per repo_id."""
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    mock_query_dicts.return_value = [
        {"repo_id": str(repo_a), "case_name": "test_flaky", "occurrences": 3},
        {"repo_id": str(repo_a), "case_name": "test_broken", "occurrences": 12},
        {"repo_id": str(repo_b), "case_name": "test_other", "occurrences": 1},
    ]

    result = await loader.load_testops_historical_failed_case_names(
        datetime.now(timezone.utc) - timedelta(days=29),
        datetime.now(timezone.utc),
        repo_id=None,
    )

    assert result == {
        repo_a: {"test_flaky", "test_broken"},
        repo_b: {"test_other"},
    }


@pytest.mark.asyncio
async def test_historical_failed_case_names_records_telemetry(monkeypatch):
    """rows_fetched (aggregate row count) vs rows_aggregated_from
    (sum(occurrences), the raw row volume the aggregate replaced) -- the gap
    between the two is the measured win (CHAOS-4350 PR 2, team-lead spec).
    """
    from dev_health_ops.metrics.prometheus import (
        DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_AGGREGATED_FROM,
        DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_FETCHED,
    )

    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    repo_id = uuid.uuid4()

    async def fake_query_dicts(client, query, params):
        return [
            {"repo_id": str(repo_id), "case_name": "test_a", "occurrences": 100_000},
            {"repo_id": str(repo_id), "case_name": "test_b", "occurrences": 250_000},
        ]

    fetched_sum_before = DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_FETCHED._sum.get()
    aggregated_sum_before = (
        DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_AGGREGATED_FROM._sum.get()
    )

    with patch(
        "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
        fake_query_dicts,
    ):
        result = await loader.load_testops_historical_failed_case_names(
            datetime.now(timezone.utc) - timedelta(days=29),
            datetime.now(timezone.utc),
            repo_id=repo_id,
        )

    assert result == {repo_id: {"test_a", "test_b"}}
    # 2 aggregate rows fetched (small) replaced 350,000 raw row occurrences
    # (the volume PR 1 alone would still have had to materialize for this
    # signal) -- that gap is the whole point of PR 2.
    assert DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_FETCHED._sum.get() == pytest.approx(
        fetched_sum_before + 2
    )
    assert (
        DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_AGGREGATED_FROM._sum.get()
        == pytest.approx(aggregated_sum_before + 350_000)
    )
