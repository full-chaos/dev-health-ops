"""CHAOS-4350: testops loader org scoping + unbounded-row-materialization.

The ticket's code-argued cause was "``load_testops_test_data``'s case_query
filters ONLY on the time window -- no org_id, no repo_id" (same defect class
as CHAOS-4324's ``load_git_rows`` unparenthesized-``OR`` leak). That does NOT
reproduce against this tree: both the suite and case queries already carry
``AND org_id = {org_id:String}`` regardless of whether ``repo_id`` is passed
(``git blame`` dates the case-query org filter to 2026-04-10, long before
this ticket and unrelated to CHAOS-4324). The first test below proves that
with real cross-tenant rows in a live ClickHouse, mirroring
``test_load_git_rows_pr_org_scope_live.py``'s pattern -- it is expected to
PASS on unmodified origin/main; it exists to pin the audit finding, not as
the fix's red-first proof.

The REAL, reproducible defect: ``query_dicts`` (``api/queries/client.py``)
materializes the full ClickHouse result with ``list(result.result_rows)`` --
no LIMIT, no streaming, no cap -- and ``load_testops_test_data`` was called
once per backfilled day with a rolling 30-day window, org-wide (``repo_id``
is frequently ``None``). That is genuinely unbounded and is what produced
the observed MemoryError in the compatibility-bridge runner, independent of
org scoping. The second test proves a real ClickHouse read now REFUSES
(raises ``TestopsRowCapExceeded``, a ``MemoryError`` subclass) once a
result exceeds the configured cap, recorded loudly via a Prometheus
counter -- rather than either materializing it in full (the original bug)
or silently truncating and computing wrong metrics from a partial,
arbitrarily-ordered slice (chris's ruling against the first version of
this fix; ``test_case_results`` is ordered by ``(repo_id, run_id, ...)``,
not event time).

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

# See test_load_git_rows_pr_org_scope_live.py for why TZ=UTC must be pinned
# before any naive-datetime ClickHouse param is bound.
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
    *, repo_id: uuid.UUID, run_id: str, suite_id: str, org_id: str, when: datetime
) -> list[object]:
    return [
        repo_id,
        run_id,
        suite_id,
        f"suite-{suite_id}",
        "pytest",
        "ci",
        1,
        1,
        0,
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
    org_id: str,
    when: datetime,
) -> list[object]:
    return [
        repo_id,
        run_id,
        suite_id,
        case_id,
        f"case-{case_id}",
        None,
        "passed",
        0.1,
        0,
        None,
        None,
        None,
        0,
        org_id,
        when,
    ]


@pytest.mark.asyncio
async def test_load_testops_test_data_does_not_leak_other_org_rows(sink):
    """Audit pin (expected PASS on unmodified origin/main too, see module
    docstring): org A must never see org B's suite/case rows, even from the
    same window and even when repo_id is omitted (org-wide, the shape the
    daily job actually calls this with).
    """
    from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader

    org_a = f"test-chaos-4350-a-{uuid.uuid4()}"
    org_b = f"test-chaos-4350-b-{uuid.uuid4()}"
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()

    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    inside_window = start + timedelta(hours=6)

    try:
        sink.client.insert(
            "test_suite_results",
            [
                _suite_row(
                    repo_id=repo_a,
                    run_id="run-a",
                    suite_id="suite-a",
                    org_id=org_a,
                    when=inside_window,
                ),
                _suite_row(
                    repo_id=repo_b,
                    run_id="run-b",
                    suite_id="suite-b",
                    org_id=org_b,
                    when=inside_window,
                ),
            ],
            column_names=_suite_cols(),
        )
        sink.client.insert(
            "test_case_results",
            [
                _case_row(
                    repo_id=repo_a,
                    run_id="run-a",
                    suite_id="suite-a",
                    case_id="case-a",
                    org_id=org_a,
                    when=inside_window,
                ),
                _case_row(
                    repo_id=repo_b,
                    run_id="run-b",
                    suite_id="suite-b",
                    case_id="case-b",
                    org_id=org_b,
                    when=inside_window,
                ),
            ],
            column_names=_case_cols(),
        )

        loader = ClickHouseDataLoader(sink.client, org_id=org_a)
        suites, cases = await loader.load_testops_test_data(start, end, repo_id=None)

        leaked_suites = [s for s in suites if s["org_id"] == org_b]
        leaked_cases = [c for c in cases if c["org_id"] == org_b]
        assert not leaked_suites, f"org {org_b!r} suite row leaked: {leaked_suites!r}"
        assert not leaked_cases, f"org {org_b!r} case row leaked: {leaked_cases!r}"

        own_suites = [s for s in suites if s["org_id"] == org_a]
        own_cases = [c for c in cases if c["org_id"] == org_a]
        assert len(own_suites) == 1, f"expected org A's own suite row, got: {suites!r}"
        assert len(own_cases) == 1, f"expected org A's own case row, got: {cases!r}"
    finally:
        sink.client.command(
            "ALTER TABLE test_suite_results DELETE WHERE org_id IN {orgs:Array(String)}",
            parameters={"orgs": [org_a, org_b]},
        )
        sink.client.command(
            "ALTER TABLE test_case_results DELETE WHERE org_id IN {orgs:Array(String)}",
            parameters={"orgs": [org_a, org_b]},
        )


@pytest.mark.asyncio
async def test_load_testops_test_data_refuses_oversized_case_results(sink, monkeypatch):
    """Real defect: an org's case-result volume for the window has no bound.

    Seeds MORE case rows than a small configured cap and proves a real
    ClickHouse read now REFUSES rather than silently computing on a partial
    result -- ``load_testops_test_data`` raises ``TestopsRowCapExceeded`` (a
    ``MemoryError`` subclass) and records
    ``DEV_HEALTH_TESTOPS_LOADER_ROW_CAP_EXCEEDED_TOTAL``. chris's ruling
    (2026-08-26): a LIMIT that lets computation proceed on a truncated
    window would produce wrong testops metrics -- not allowed, since
    ``test_case_results`` is ordered by ``(repo_id, run_id, ...)``, not
    event time, so an unordered LIMIT could arbitrarily keep stale rows over
    today's.
    """
    from dev_health_ops.metrics.loaders.clickhouse import (
        ClickHouseDataLoader,
        TestopsRowCapExceeded,
    )
    from dev_health_ops.metrics.prometheus import (
        DEV_HEALTH_TESTOPS_LOADER_ROW_CAP_EXCEEDED_TOTAL,
    )

    monkeypatch.setenv("DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS", "5")

    org_id = f"test-chaos-4350-cap-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    start = datetime(2026, 3, 5, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    inside_window = start + timedelta(hours=3)

    case_count = 12  # > the cap (5), well within reach of a live query
    case_rows = [
        _case_row(
            repo_id=repo_id,
            run_id="run-cap",
            suite_id="suite-cap",
            case_id=f"case-{i}",
            org_id=org_id,
            when=inside_window,
        )
        for i in range(case_count)
    ]

    counter = DEV_HEALTH_TESTOPS_LOADER_ROW_CAP_EXCEEDED_TOTAL.labels(
        table="test_case_results"
    )
    before = counter._value.get()

    try:
        sink.client.insert(
            "test_suite_results",
            [
                _suite_row(
                    repo_id=repo_id,
                    run_id="run-cap",
                    suite_id="suite-cap",
                    org_id=org_id,
                    when=inside_window,
                )
            ],
            column_names=_suite_cols(),
        )
        sink.client.insert("test_case_results", case_rows, column_names=_case_cols())

        loader = ClickHouseDataLoader(sink.client, org_id=org_id)
        with pytest.raises(TestopsRowCapExceeded) as exc_info:
            await loader.load_testops_test_data(start, end, repo_id=None)

        assert exc_info.value.table == "test_case_results"
        assert exc_info.value.org_id == org_id
        assert "testops_row_cap_exceeded" in str(exc_info.value)
        after = counter._value.get()
        assert after - before == 1, (
            "DEV_HEALTH_TESTOPS_LOADER_ROW_CAP_EXCEEDED_TOTAL did not "
            f"record the guard tripping (before={before}, after={after})"
        )
    finally:
        sink.client.command(
            "ALTER TABLE test_suite_results DELETE WHERE org_id = {org:String}",
            parameters={"org": org_id},
        )
        sink.client.command(
            "ALTER TABLE test_case_results DELETE WHERE org_id = {org:String}",
            parameters={"org": org_id},
        )
