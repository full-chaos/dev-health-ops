"""CHAOS-4350 PR 2: parity between the SQL-side historical aggregate and a
from-raw-rows Python reference computation.

`load_testops_historical_failed_case_names` replaces materializing every
historical case row with a `GROUP BY case_name` aggregate. This test proves
that swap is loss-free for what `compute_test_metrics_daily` actually needs:
seeds real cross-day data (today's cases -- some flaky, some clean -- plus
historical failures on earlier days, including one case name that fails
both historically AND today, and one that recurs via a non-literal
failure-equivalent status ("error", per
`compute_testops._normalize_test_status`) rather than the literal
"failed" string), fetches the historical set via the SQL aggregate,
independently derives the same set from the raw historical rows in Python
(the pre-PR-2 algorithm, inlined here as the reference -- using the SAME
normalizer, since that is what the pre-PR-2 code actually did), and
asserts they're identical. Then runs `compute_test_metrics_daily` with each
and asserts `flake_rate` AND `failure_recurrence_score` match -- flake_rate
should be unaffected by the historical-fetch change (it's computed purely
from today's data) but is asserted anyway per team-lead's spec, since it
would silently break if today's fetch were accidentally scoped wrong
alongside the historical change.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

os.environ.setdefault("TZ", "UTC")
import time as _time  # noqa: E402

if hasattr(_time, "tzset"):
    _time.tzset()

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at a SCRATCH database "
        "(e.g. clickhouse://ch:ch@localhost:8123/ci_local_validate_xxx) "
        "-- never /default, which holds real dev data.",
    ),
]


def _resolves_to_default_database(uri: str) -> bool:
    from urllib.parse import unquote, urlsplit

    path = urlsplit(uri).path.lstrip("/")
    return unquote(path) in ("", "default")


if CLICKHOUSE_URI and _resolves_to_default_database(CLICKHOUSE_URI):
    pytest.skip(
        "refusing to run against CLICKHOUSE_URI's /default database -- "
        "point this at a scratch db (see ops-local-validate skill / "
        "ops/AGENTS.md 'Safety rule')",
        allow_module_level=True,
    )


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    s = ClickHouseMetricsSink(CLICKHOUSE_URI)
    s.ensure_schema(force=True)
    yield s
    s.close()


def _suite_cols() -> list[str]:
    return [
        "repo_id",
        "run_id",
        "suite_id",
        "suite_name",
        "framework",
        "environment",
        "total_count",
        "passed_count",
        "failed_count",
        "skipped_count",
        "error_count",
        "quarantined_count",
        "retried_count",
        "duration_seconds",
        "started_at",
        "finished_at",
        "team_id",
        "service_id",
        "org_id",
        "last_synced",
    ]


def _case_cols() -> list[str]:
    return [
        "repo_id",
        "run_id",
        "suite_id",
        "case_id",
        "case_name",
        "class_name",
        "status",
        "duration_seconds",
        "retry_attempt",
        "failure_message",
        "failure_type",
        "stack_trace",
        "is_quarantined",
        "org_id",
        "last_synced",
    ]


def _suite_row(
    *,
    repo_id: uuid.UUID,
    run_id: str,
    suite_id: str,
    org_id: str,
    when: datetime,
    total: int,
    passed: int,
    failed: int,
) -> list[object]:
    return [
        repo_id,
        run_id,
        suite_id,
        f"suite-{suite_id}",
        "pytest",
        "ci",
        total,
        passed,
        failed,
        0,
        0,
        0,
        0,
        1.0,
        when,
        when,
        None,
        None,
        org_id,
        when,
    ]


def _case_row(
    *,
    repo_id: uuid.UUID,
    run_id: str,
    suite_id: str,
    case_id: str,
    case_name: str,
    status: str,
    org_id: str,
    when: datetime,
    retry_attempt: int = 0,
) -> list[object]:
    return [
        repo_id,
        run_id,
        suite_id,
        case_id,
        case_name,
        None,
        status,
        0.1,
        retry_attempt,
        None,
        None,
        None,
        0,
        org_id,
        when,
    ]


async def _reference_historical_failed_names(
    sink, *, repo_id: uuid.UUID, org_id: str, start: datetime, end: datetime
) -> dict[uuid.UUID, set[str]]:
    """Pre-PR-2 algorithm, inlined: iterate every raw historical case row
    and collect failed case_names, using the SAME failure-equivalent
    vocabulary as `_normalize_test_status` (the pre-PR-2 code ran every
    historical row through that normalizer too -- a literal `status ==
    "failed"` check here would NOT be ground truth, it would just be the
    other place CHAOS-4350 PR 2's regression could hide). This is the
    ground truth PR 2's SQL aggregate must match.
    """
    from dev_health_ops.metrics.compute_testops import _normalize_test_status
    from dev_health_ops.metrics.loaders.clickhouse import _clickhouse_query_dicts

    query = """
    SELECT c.repo_id, c.case_name, c.status
    FROM test_case_results AS c FINAL
    INNER JOIN test_suite_results AS s FINAL
      ON s.repo_id = c.repo_id AND s.run_id = c.run_id AND s.suite_id = c.suite_id AND s.org_id = c.org_id
    WHERE s.repo_id = {repo_id:UUID} AND s.org_id = {org_id:String}
      AND coalesce(s.started_at, s.finished_at) >= {start:DateTime}
      AND coalesce(s.started_at, s.finished_at) < {end:DateTime}
    """
    rows = await _clickhouse_query_dicts(
        sink.client,
        query,
        {
            "repo_id": str(repo_id),
            "org_id": org_id,
            "start": start.replace(tzinfo=None),
            "end": end.replace(tzinfo=None),
        },
    )
    result: dict[uuid.UUID, set[str]] = {}
    for row in rows:
        if _normalize_test_status(row.get("status")) != "failed":
            continue
        rid = row["repo_id"]
        result.setdefault(rid, set()).add(str(row["case_name"]))
    return result


@pytest.mark.asyncio
async def test_historical_aggregate_matches_raw_row_reference(sink):
    from dev_health_ops.metrics.compute_testops import compute_test_metrics_daily
    from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader

    org_id = f"test-chaos-4350-parity-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    today = datetime(2026, 3, 15, tzinfo=timezone.utc)
    today_start = today
    today_end = today + timedelta(days=1)
    history_start = today - timedelta(days=29)
    five_days_ago = today - timedelta(days=5)

    rows_to_insert_suites = []
    rows_to_insert_cases = []

    # Historical day (5 days ago): "test_recurring" fails (will recur
    # today), "test_only_historical" also fails (won't recur),
    # "test_error_recurring" fails with the canonical "error" status (NOT
    # the literal "failed" string) -- CHAOS-4350 PR2 round-1 codex finding:
    # this must still count as a historical failure, per
    # _normalize_test_status's full failure-equivalent vocabulary.
    rows_to_insert_suites.append(
        _suite_row(
            repo_id=repo_id,
            run_id="run-hist",
            suite_id="suite-hist",
            org_id=org_id,
            when=five_days_ago,
            total=3,
            passed=0,
            failed=3,
        )
    )
    rows_to_insert_cases += [
        _case_row(
            repo_id=repo_id,
            run_id="run-hist",
            suite_id="suite-hist",
            case_id="h1",
            case_name="test_recurring",
            status="failed",
            org_id=org_id,
            when=five_days_ago,
        ),
        _case_row(
            repo_id=repo_id,
            run_id="run-hist",
            suite_id="suite-hist",
            case_id="h2",
            case_name="test_only_historical",
            status="failed",
            org_id=org_id,
            when=five_days_ago,
        ),
        _case_row(
            repo_id=repo_id,
            run_id="run-hist",
            suite_id="suite-hist",
            case_id="h3",
            case_name="test_error_recurring",
            status="error",
            org_id=org_id,
            when=five_days_ago,
        ),
    ]

    # Today: "test_flaky" fails then passes on retry (same-day flake, must
    # be detected regardless of history); "test_recurring" fails again
    # (recurrence); "test_clean" passes cleanly; "test_error_recurring"
    # fails again today (recurrence of the historical "error"-status case).
    rows_to_insert_suites.append(
        _suite_row(
            repo_id=repo_id,
            run_id="run-today",
            suite_id="suite-today",
            org_id=org_id,
            when=today_start + timedelta(hours=1),
            total=5,
            passed=2,
            failed=3,
        )
    )
    rows_to_insert_cases += [
        _case_row(
            repo_id=repo_id,
            run_id="run-today",
            suite_id="suite-today",
            case_id="t1",
            case_name="test_flaky",
            status="failed",
            org_id=org_id,
            when=today_start,
            retry_attempt=0,
        ),
        _case_row(
            repo_id=repo_id,
            run_id="run-today",
            suite_id="suite-today",
            case_id="t1r",
            case_name="test_flaky",
            status="passed",
            org_id=org_id,
            when=today_start,
            retry_attempt=1,
        ),
        _case_row(
            repo_id=repo_id,
            run_id="run-today",
            suite_id="suite-today",
            case_id="t2",
            case_name="test_recurring",
            status="failed",
            org_id=org_id,
            when=today_start,
        ),
        _case_row(
            repo_id=repo_id,
            run_id="run-today",
            suite_id="suite-today",
            case_id="t3",
            case_name="test_clean",
            status="passed",
            org_id=org_id,
            when=today_start,
        ),
        _case_row(
            repo_id=repo_id,
            run_id="run-today",
            suite_id="suite-today",
            case_id="t4",
            case_name="test_error_recurring",
            status="failed",
            org_id=org_id,
            when=today_start,
        ),
    ]

    try:
        sink.client.insert(
            "test_suite_results", rows_to_insert_suites, column_names=_suite_cols()
        )
        sink.client.insert(
            "test_case_results", rows_to_insert_cases, column_names=_case_cols()
        )

        loader = ClickHouseDataLoader(sink.client, org_id=org_id)

        # SQL-aggregated path (PR 2).
        aggregated_historical = await loader.load_testops_historical_failed_case_names(
            history_start, today_start, repo_id=repo_id, current_day_end=today_end
        )

        # Raw-row Python reference (pre-PR-2 algorithm, inlined).
        reference_historical = await _reference_historical_failed_names(
            sink,
            repo_id=repo_id,
            org_id=org_id,
            start=history_start,
            end=today_start,
        )

        assert (
            aggregated_historical
            == reference_historical
            == {
                repo_id: {
                    "test_recurring",
                    "test_only_historical",
                    "test_error_recurring",
                }
            }
        )

        suite_rows, case_rows = await loader.load_testops_test_data(
            today_start, today_end, repo_id=repo_id
        )

        records_aggregated = compute_test_metrics_daily(
            day=today.date(),
            suite_results=suite_rows,
            case_results=case_rows,
            computed_at=today,
            historical_failed_names_by_repo=aggregated_historical,
        )
        records_reference = compute_test_metrics_daily(
            day=today.date(),
            suite_results=suite_rows,
            case_results=case_rows,
            computed_at=today,
            historical_failed_names_by_repo=reference_historical,
        )

        rec_agg = next(r for r in records_aggregated if r.repo_id == repo_id)
        rec_ref = next(r for r in records_reference if r.repo_id == repo_id)

        # flake_rate: purely a function of today's data -- must match
        # (and must actually detect test_flaky) regardless of which
        # historical source was used.
        assert rec_agg.flake_rate == rec_ref.flake_rate
        assert rec_agg.flake_rate == pytest.approx(1 / 4)  # 1 of 4 distinct

        # failure_recurrence_score: the whole point of the aggregate --
        # must match between the SQL-aggregated and raw-row-reference paths.
        assert rec_agg.failure_recurrence_score == rec_ref.failure_recurrence_score
        # 2 of 3 today-failed names ("test_recurring", "test_error_recurring")
        # also failed historically; "test_flaky" failed today but not
        # historically. Pre-fix (literal status='failed' in SQL), the
        # historical set would have excluded "test_error_recurring" (its
        # historical status is "error"), giving a wrong 1/3 here instead.
        assert rec_agg.failure_recurrence_score == pytest.approx(2 / 3)
    finally:
        sink.client.command(
            "ALTER TABLE test_suite_results DELETE WHERE org_id = {org:String}",
            parameters={"org": org_id},
        )
        sink.client.command(
            "ALTER TABLE test_case_results DELETE WHERE org_id = {org:String}",
            parameters={"org": org_id},
        )


@pytest.mark.asyncio
async def test_run_straddling_midnight_not_split_between_today_and_historical(sink):
    """CHAOS-4350 PR2 (codex round 2 P2, team-lead ruling 2026-08-27): a
    single CI run whose suites straddle UTC midnight must not have its
    pre-midnight failure silently dropped from today's flake detection, nor
    double-counted as a "historical" failure by the separate historical
    query. Both would happen if cases were bucketed by their OWN suite's
    day instead of by RUN_ID membership -- which is the pre-PR-2 (and
    documented, "same run window", testops_schemas.py) semantics.

    One run_id ("run-boundary") has two suites: one just before midnight
    (day d-1) where "test_boundary" fails, and one just after midnight
    (day d, i.e. `today`) where "test_boundary" passes on retry -- a
    same-run flake that happens to straddle the UTC day boundary. JUnit
    `<testsuite>` elements can carry their own timestamp (preferred over
    the run-level fallback -- see `_build_rows_from_parsed`), so this is a
    real, reachable ingestion shape, not a contrived one.
    """
    from dev_health_ops.metrics.compute_testops import compute_test_metrics_daily
    from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader

    org_id = f"test-chaos-4350-boundary-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    today = datetime(2026, 3, 20, tzinfo=timezone.utc)
    today_start = today
    today_end = today + timedelta(days=1)
    history_start = today - timedelta(days=29)
    just_before_midnight = today - timedelta(minutes=10)  # day d-1, 23:50 UTC
    just_after_midnight = today + timedelta(minutes=10)  # day d, 00:10 UTC

    suites = [
        _suite_row(
            repo_id=repo_id,
            run_id="run-boundary",
            suite_id="suite-pre-midnight",
            org_id=org_id,
            when=just_before_midnight,
            total=1,
            passed=0,
            failed=1,
        ),
        _suite_row(
            repo_id=repo_id,
            run_id="run-boundary",
            suite_id="suite-post-midnight",
            org_id=org_id,
            when=just_after_midnight,
            total=1,
            passed=1,
            failed=0,
        ),
    ]
    cases = [
        _case_row(
            repo_id=repo_id,
            run_id="run-boundary",
            suite_id="suite-pre-midnight",
            case_id="pre1",
            case_name="test_boundary",
            status="failed",
            org_id=org_id,
            when=just_before_midnight,
            retry_attempt=0,
        ),
        _case_row(
            repo_id=repo_id,
            run_id="run-boundary",
            suite_id="suite-post-midnight",
            case_id="post1",
            case_name="test_boundary",
            status="passed",
            org_id=org_id,
            when=just_after_midnight,
            retry_attempt=1,
        ),
    ]

    try:
        sink.client.insert("test_suite_results", suites, column_names=_suite_cols())
        sink.client.insert("test_case_results", cases, column_names=_case_cols())

        loader = ClickHouseDataLoader(sink.client, org_id=org_id)

        historical = await loader.load_testops_historical_failed_case_names(
            history_start, today_start, repo_id=repo_id, current_day_end=today_end
        )
        # The pre-midnight failure belongs to TODAY's run (run-boundary has
        # a suite in today's window too) -- it must NOT appear as a
        # historical failure, even though its own suite's timestamp falls
        # inside the historical window on its own.
        assert "test_boundary" not in historical.get(repo_id, set())

        suite_rows, case_rows = await loader.load_testops_test_data(
            today_start, today_end, repo_id=repo_id
        )
        # Both the pre- and post-midnight case rows must come back for
        # "today" -- they share run_id "run-boundary", which has a suite in
        # today's window (suite-post-midnight).
        assert len(case_rows) == 2

        records = compute_test_metrics_daily(
            day=today.date(),
            suite_results=suite_rows,
            case_results=case_rows,
            computed_at=today,
            historical_failed_names_by_repo=historical,
        )
        rec = next(r for r in records if r.repo_id == repo_id)

        # flake_rate: the pair (failed pre-midnight, passed post-midnight,
        # same run_id) must be recognized as ONE flaky case.
        assert rec.flake_rate == pytest.approx(1.0)
        # failure_recurrence_score: "test_boundary" IS in current_failed_names
        # (it failed at least once today, via the pre-midnight row) but must
        # NOT be counted as a recurrence of a historical failure -- there is
        # no genuinely prior-day failure here, just this one straddling run.
        assert rec.failure_recurrence_score == pytest.approx(0.0)
    finally:
        sink.client.command(
            "ALTER TABLE test_suite_results DELETE WHERE org_id = {org:String}",
            parameters={"org": org_id},
        )
        sink.client.command(
            "ALTER TABLE test_case_results DELETE WHERE org_id = {org:String}",
            parameters={"org": org_id},
        )
