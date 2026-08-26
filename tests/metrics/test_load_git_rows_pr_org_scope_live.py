"""CHAOS-4324: ``ClickHouseDataLoader.load_git_rows`` PR-query tenant leak.

The PR query in ``load_git_rows`` built its ``WHERE`` clause as::

    WHERE
      (created_at >= start AND created_at < end)
      OR (merged_at IS NOT NULL AND merged_at >= start AND merged_at < end)
      {repo_filter}
    {org_filter}

``{org_filter}`` renders as `` AND org_id = {org_id:String}`` (see
``OrgScopedQuery.filter``). SQL's ``AND`` binds tighter than ``OR``, so the
clause actually parsed as::

    (created_at in range) OR ((merged_at in range) AND org_id = X)

Any row whose ``created_at`` falls in the requested window matches the FIRST
disjunct regardless of org, so a same-window PR from ANY other tenant leaked
into this org's daily metrics compute. The existing unit tests in
``test_clickhouse_org_scope.py`` only mock ``_clickhouse_query_dicts`` and
assert ``"org_id" in sql`` / ``"org_id" in params`` -- they can't see the
precedence bug because the string genuinely contains an org_id clause, it's
just not applied to every branch. This test seeds real rows in a live
ClickHouse and proves actual cross-tenant leakage, and asserts the new
``CLICKHOUSE_ORG_SCOPE_ROWS_FILTERED_TOTAL`` leak-guard counter records
what org scoping excluded.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

# clickhouse-connect encodes naive `DateTime` query parameters as a Unix
# epoch computed in the PROCESS's local timezone (not UTC), while `INSERT`
# rows and `naive_utc()` treat naive datetimes as already-UTC wall-clock
# values. Outside a UTC process (e.g. a developer machine on PDT/PST) this
# silently shifts the query window by the local UTC offset and produces
# false negatives/positives unrelated to the code under test. Production
# and CI containers run with TZ=UTC; pin it here so this test is correct
# everywhere it runs.
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
    """True if *uri* would have the ClickHouse client connect to "default".

    Mirrors how the client itself resolves the database name from a DSN:
    the URL path component, percent-decoded, with an EMPTY path (no
    trailing "/xxx" at all, e.g. "clickhouse://ch:ch@localhost:8123")
    also meaning "default" -- a naive literal "/default" string match
    misses both that empty-path case and any percent-encoded form
    (e.g. "%64efault").
    """
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


def _pr_cols() -> list[str]:
    return [
        "repo_id",
        "number",
        "author_email",
        "author_name",
        "created_at",
        "last_synced",
        "org_id",
    ]


@pytest.mark.asyncio
async def test_load_git_rows_pr_query_does_not_leak_other_org_rows(sink):
    """Org A must never see Org B's PR rows, even though both PRs were
    created inside the exact same requested time window.
    """
    from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
    from dev_health_ops.metrics.prometheus import (
        CLICKHOUSE_ORG_SCOPE_ROWS_FILTERED_TOTAL,
    )

    counter = CLICKHOUSE_ORG_SCOPE_ROWS_FILTERED_TOTAL.labels(table="git_pull_requests")
    before = counter._value.get()

    org_a = f"test-chaos-4324-a-{uuid.uuid4()}"
    org_b = f"test-chaos-4324-b-{uuid.uuid4()}"

    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    inside_window = start + timedelta(hours=6)

    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()

    try:
        sink.client.insert(
            "git_pull_requests",
            [
                # Org A's own PR, created inside the window.
                [
                    repo_a,
                    1,
                    "alice@org-a.example",
                    "alice",
                    inside_window,
                    inside_window,
                    org_a,
                ],
                # Org B's PR, ALSO created inside the same window -- must
                # never appear in Org A's results.
                [
                    repo_b,
                    1,
                    "mallory@org-b.example",
                    "mallory",
                    inside_window,
                    inside_window,
                    org_b,
                ],
            ],
            column_names=_pr_cols(),
        )

        loader = ClickHouseDataLoader(sink.client, org_id=org_a)
        _commits, prs, _reviews = await loader.load_git_rows(start, end, repo_id=None)

        leaked = [p for p in prs if p["author_email"] == "mallory@org-b.example"]
        assert not leaked, (
            f"load_git_rows leaked {len(leaked)} PR row(s) from org {org_b!r} "
            f"into org {org_a!r}'s results: {leaked!r}"
        )
        own = [p for p in prs if p["author_email"] == "alice@org-a.example"]
        assert len(own) == 1, f"expected org A's own PR row, got: {prs!r}"

        # CHAOS-4324 telemetry: org scoping excluded exactly Org B's 1 PR
        # row (Org A's own row is not "filtered" -- it belongs to Org A).
        after = counter._value.get()
        assert after - before == 1, (
            "CLICKHOUSE_ORG_SCOPE_ROWS_FILTERED_TOTAL did not record the "
            f"1 row org scoping excluded (before={before}, after={after})"
        )
    finally:
        sink.client.command(
            "ALTER TABLE git_pull_requests DELETE WHERE org_id IN {orgs:Array(String)}",
            parameters={"orgs": [org_a, org_b]},
        )
