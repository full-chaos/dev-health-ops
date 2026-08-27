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
    # CHAOS-4350 PR2 (codex round 2 P2): the case query is no longer joined
    # to test_suite_results by suite_id/day -- it's scoped by run_id
    # membership via a semi-join subquery (see load_testops_test_data's
    # docstring), and the repo_id filter is applied directly on `c`.
    case_sql = mock_query_dicts.call_args_list[1].args[1]
    assert "c.repo_id = {repo_id:UUID}" in case_sql


@pytest.mark.asyncio
async def test_testops_join_predicates_scope_org_id(mock_query_dicts):
    """Joins (and the case query's run_id semi-join subquery) must carry
    org_id equality so cross-org child rows never match."""
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_testops_pipeline_data(start, end, repo_id=uuid.uuid4())
    job_sql = mock_query_dicts.call_args_list[1].args[1]
    assert "(p.org_id = j.org_id)" in job_sql

    mock_query_dicts.reset_mock()
    await loader.load_testops_test_data(start, end, repo_id=uuid.uuid4())
    # CHAOS-4350 PR2 (codex round 2 P2): org scoping now applies directly on
    # `c` (no join partner to compare against) AND inside the semi-join
    # subquery against test_suite_results (unaliased -- defense in depth,
    # so a cross-org run_id collision can't leak cases into the wrong org).
    case_sql = mock_query_dicts.call_args_list[1].args[1]
    assert "AND c.org_id = {org_id:String}" in case_sql
    assert "IN (" in case_sql and "AND org_id = {org_id:String}" in case_sql


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
    assert "AND c.org_id = {org_id:String}" in case_sql
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
        # CHAOS-4350 PR2 (codex round 2 P2): the case query no longer has an
        # INNER JOIN (it's a semi-join subquery instead) -- test_case_results
        # only ever appears in the case query, never the suite query, so
        # that alone still distinguishes them.
        if "test_case_results" in query:
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
        # CHAOS-4350 PR2 (codex round 2 P2): the case query no longer has an
        # INNER JOIN (it's a semi-join subquery instead) -- test_case_results
        # only ever appears in the case query, never the suite query, so
        # that alone still distinguishes them.
        if "test_case_results" in query:
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
    joined tables, org+repo scoped, capped, and filtered on the FULL
    failure-equivalent status vocabulary (matching
    compute_testops._normalize_test_status(), not just the literal
    'failed' string) -- pushed into SQL instead of fetching raw
    historical case rows.
    """
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 31, tzinfo=timezone.utc)
    current_day_end = end + timedelta(days=1)
    repo_id = uuid.uuid4()

    await loader.load_testops_historical_failed_case_names(
        start, end, repo_id=repo_id, current_day_end=current_day_end
    )

    # Two queries now: the GROUP BY aggregate, and (codex round 2) a second
    # unfiltered count() for the ROWS_AGGREGATED_FROM telemetry.
    assert mock_query_dicts.call_count == 2
    agg_call, count_call = mock_query_dicts.call_args_list
    sql = agg_call.args[1]
    params = agg_call.args[2]
    assert "test_case_results AS c FINAL" in sql
    assert "test_suite_results AS s FINAL" in sql
    assert "lower(trim(c.status)) IN {_failure_statuses:Array(String)}" in sql
    assert "GROUP BY s.repo_id, c.case_name" in sql
    assert "s.repo_id = {repo_id:UUID}" in sql
    assert "AND s.org_id = {org_id:String}" in sql
    assert "LIMIT {_row_cap:UInt64}" in sql
    # codex round 2 P2: today's run_ids (a day-boundary-straddling run's
    # cases) must be excluded from "historical" via a semi-join subquery,
    # not just time-sliced on `end`.
    assert "(s.repo_id, s.run_id) NOT IN" in sql
    assert "{current_day_end:DateTime}" in sql
    assert params.get("repo_id") == str(repo_id)
    assert params.get("org_id") == "acme-corp"
    assert params.get("start") == start.replace(tzinfo=None)
    assert params.get("end") == end.replace(tzinfo=None)
    assert params.get("current_day_end") == current_day_end.replace(tzinfo=None)
    assert set(params.get("_failure_statuses")) == {
        "failure",
        "failed",
        "error",
        "errors",
        "timeout",
        "timed_out",
    }
    assert not DOTTED_PARAM.search(sql)

    count_sql = count_call.args[1]
    assert "count() AS total" in count_sql
    assert "test_case_results AS c FINAL" in count_sql
    assert "test_suite_results AS s FINAL" in count_sql
    assert "(s.repo_id, s.run_id) NOT IN" in count_sql
    # Unfiltered by status (the whole point -- it measures the population the
    # failure-only aggregate replaced, not another failure-only count) and
    # unbounded by the row cap (a single scalar, never materializes rows).
    assert "c.status" not in count_sql
    assert "LIMIT" not in count_sql
    assert not DOTTED_PARAM.search(count_sql)


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

    now = datetime.now(timezone.utc)
    result = await loader.load_testops_historical_failed_case_names(
        now - timedelta(days=29),
        now,
        repo_id=None,
        current_day_end=now + timedelta(days=1),
    )

    assert result == {
        repo_a: {"test_flaky", "test_broken"},
        repo_b: {"test_other"},
    }


@pytest.mark.asyncio
async def test_historical_failed_case_names_records_telemetry(monkeypatch):
    """rows_fetched (aggregate row count) vs rows_aggregated_from (the FULL
    unfiltered joined-row population the aggregation replaced, from a
    separate count() query -- NOT sum(occurrences), which only reflects the
    failure-filtered subset and undercounts this metric per codex round 2)
    -- the gap between the two is the measured win (CHAOS-4350 PR 2,
    team-lead spec).
    """
    from dev_health_ops.metrics.prometheus import (
        DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_AGGREGATED_FROM,
        DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_FETCHED,
    )

    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    repo_id = uuid.uuid4()

    async def fake_query_dicts(client, query, params):
        if "GROUP BY" in query:
            return [
                {
                    "repo_id": str(repo_id),
                    "case_name": "test_a",
                    "occurrences": 100_000,
                },
                {
                    "repo_id": str(repo_id),
                    "case_name": "test_b",
                    "occurrences": 250_000,
                },
            ]
        # The unfiltered count() query -- deliberately a DIFFERENT number
        # than sum(occurrences)=350_000 above, so this test actually proves
        # the telemetry is sourced from the count() query, not a coincidence.
        assert "count() AS total" in query
        return [{"total": 1_100_000}]

    fetched_sum_before = DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_FETCHED._sum.get()
    aggregated_sum_before = (
        DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_AGGREGATED_FROM._sum.get()
    )

    now = datetime.now(timezone.utc)
    with patch(
        "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
        fake_query_dicts,
    ):
        result = await loader.load_testops_historical_failed_case_names(
            now - timedelta(days=29),
            now,
            repo_id=repo_id,
            current_day_end=now + timedelta(days=1),
        )

    assert result == {repo_id: {"test_a", "test_b"}}
    # 2 aggregate rows fetched (small) replaced the full 1.1M-row joined
    # population (the volume PR 1 alone would still have had to materialize
    # for this signal) -- that gap is the whole point of PR 2.
    assert DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_FETCHED._sum.get() == pytest.approx(
        fetched_sum_before + 2
    )
    assert (
        DEV_HEALTH_TESTOPS_HISTORICAL_ROWS_AGGREGATED_FROM._sum.get()
        == pytest.approx(aggregated_sum_before + 1_100_000)
    )


@pytest.mark.asyncio
async def test_load_work_items_excludes_description_and_bounds_block_size(
    mock_query_dicts,
):
    """CHAOS-4361: `description` (the one unbounded Nullable(String) column
    on `work_items` -- full issue/PR body text) must never leave ClickHouse
    for this read. Neither of load_work_items's two callers in job_daily.py
    reads `.description` -- one only collects `work_item_id` for dependency
    source-id lookups, the other feeds bug-MTTR/team-attribution/cycle-time
    computes, none of which touch it. `SELECT * EXCEPT (description)` (not a
    hand-maintained column list) plus a query-time `max_block_size` bound
    the peak size of any single `read_str_col` call -- the 2026-08-27
    incident's traceback -- for a long-lived repo's open-item backlog."""
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_work_items(start, end, repo_id=uuid.uuid4())

    assert mock_query_dicts.call_count == 2
    item_sql = mock_query_dicts.call_args_list[0].args[1]
    trans_sql = mock_query_dicts.call_args_list[1].args[1]
    assert "SELECT * EXCEPT (description)" in item_sql
    assert "description" not in trans_sql.replace("SELECT * EXCEPT (description)", "")
    for sql in (item_sql, trans_sql):
        assert "SETTINGS max_block_size = 8192" in sql
        assert not DOTTED_PARAM.search(sql), (
            f"dotted param binding leaked into SQL: {sql}"
        )


@pytest.mark.asyncio
async def test_load_work_items_row_without_description_key_builds_a_valid_work_item(
    mock_query_dicts,
):
    """A row dict that never carries a `description` key (exactly what the
    trimmed SELECT now returns) must still build a `WorkItem` -- the field
    has a `None` default, and `to_dataclass` only sets keys present in the
    row, so an absent key is not the same failure mode as an explicit
    `None` value and needs its own coverage."""
    mock_query_dicts.side_effect = [
        [
            {
                "repo_id": str(uuid.uuid4()),
                "work_item_id": "gh:owner/repo#1",
                "provider": "github",
                "title": "some title",
                "type": "bug",
                "status": "open",
                "status_raw": "open",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        ],
        [],  # transitions: unused by this test
    ]
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    items, _ = await loader.load_work_items(start, end, repo_id=uuid.uuid4())

    assert len(items) == 1
    assert items[0].description is None
    assert items[0].work_item_id == "gh:owner/repo#1"
