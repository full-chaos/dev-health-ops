"""Live-ClickHouse regression for the CHAOS-2492 Oracle NO-GO tenant leak.

``LATEST_WORK_UNIT_AUTHORS_CTE`` (api/queries/investment.py) resolves the
investment developer filter by deduping ``git_commits`` by (repo_id, hash) and
``git_pull_requests`` by (repo_id, number) via ``argMax(author_email, ...)``.
Those tables are keyed by (org_id, repo_id, hash/number) (migration 027), and
repo_id+hash / repo_id+number values CAN collide across tenants -- this exact
collision risk is documented at work_graph/builder.py:1643 for the equivalent
git_commits join. Before the fix, the inner dedupe subqueries grouped by
(repo_id, hash) / (repo_id, number) only -- WITHOUT org_id -- so a collision
let ``argMax`` resolve ANOTHER org's ``author_email`` into this org's
investment developer filter: a tenant-isolation leak.

This test seeds TWO orgs sharing the SAME repo_id + commit hash (and the same
repo_id + PR number) but with DIFFERENT author_email, and gives the OTHER
org's row a newer ``last_synced`` (the argMax ordering column) than the
querying org's own row -- exactly the shape that would make the pre-fix,
unscoped ``argMax`` pick the wrong tenant's email. It asserts that each org's
``work_unit_authors.author_emails`` resolves ONLY that org's own
``author_email`` -- no cross-tenant bleed.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

import pytest

import dev_health_ops.api.queries.investment as investment_queries

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
    ),
]


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    s = ClickHouseMetricsSink(CLICKHOUSE_URI)
    s.ensure_schema(force=True)
    yield s
    s.close()


def _git_commit_cols() -> list[str]:
    return ["repo_id", "hash", "author_email", "last_synced", "org_id"]


def _git_pr_cols() -> list[str]:
    return ["repo_id", "number", "author_email", "created_at", "last_synced", "org_id"]


def _work_unit_investment_cols() -> list[str]:
    return [
        "work_unit_id",
        "from_ts",
        "to_ts",
        "structural_evidence_json",
        "computed_at",
        "org_id",
    ]


async def _resolve_author_emails(sink, org_id: str) -> dict[str, set[str]]:
    """Run the REAL org-scoped CTEs and return {work_unit_id: {author_email, ...}}."""
    query = f"""
        WITH {investment_queries.LATEST_WORK_UNIT_INVESTMENTS_CTE},
             {investment_queries.LATEST_WORK_UNIT_AUTHORS_CTE}
        SELECT work_unit_id, author_emails
        FROM work_unit_authors
        ORDER BY work_unit_id
    """
    rows = await investment_queries.query_dicts(sink, query, {"org_id": org_id})
    return {row["work_unit_id"]: set(row["author_emails"]) for row in rows}


@pytest.mark.asyncio
async def test_authors_cte_scopes_dedup_by_org_no_cross_tenant_bleed(sink):
    """TWO orgs share the exact same repo_id+hash commit ref AND repo_id+number
    PR ref (a collision that CAN happen in practice -- see
    work_graph/builder.py:1643) but have DIFFERENT author_email. Org B's rows
    carry a NEWER last_synced (the argMax ordering column) than Org A's --
    the exact shape that made the pre-fix, unscoped argMax(author_email, ...)
    pick Org B's email for Org A's query too. Asserts each org's investment
    developer filter resolves ONLY its own author_email.
    """
    org_a = f"test-chaos-2492-a-{uuid.uuid4()}"
    org_b = f"test-chaos-2492-b-{uuid.uuid4()}"
    repo_id = str(uuid.uuid4())  # SAME repo_id reused by both tenants
    commit_hash = uuid.uuid4().hex
    pr_number = 42

    commit_ref = f"{repo_id}@{commit_hash}"
    pr_ref = f"{repo_id}#pr{pr_number}"

    older = datetime(2026, 1, 1, tzinfo=timezone.utc)
    newer = datetime(2026, 6, 1, tzinfo=timezone.utc)  # Org B "wins" a global argMax

    try:
        sink.client.insert(
            "git_commits",
            [
                [repo_id, commit_hash, "alice@org-a.example", older, org_a],
                [repo_id, commit_hash, "mallory@org-b.example", newer, org_b],
            ],
            column_names=_git_commit_cols(),
        )
        sink.client.insert(
            "git_pull_requests",
            [
                [repo_id, pr_number, "alice@org-a.example", older, older, org_a],
                [repo_id, pr_number, "mallory@org-b.example", newer, newer, org_b],
            ],
            column_names=_git_pr_cols(),
        )

        evidence_json = json.dumps({"commits": [commit_ref], "prs": [pr_ref]})
        sink.client.insert(
            "work_unit_investments",
            [
                ["wu-org-a", older, newer, evidence_json, older, org_a],
                ["wu-org-b", older, newer, evidence_json, older, org_b],
            ],
            column_names=_work_unit_investment_cols(),
        )

        emails_a = await _resolve_author_emails(sink, org_a)
        emails_b = await _resolve_author_emails(sink, org_b)

        assert emails_a == {"wu-org-a": {"alice@org-a.example"}}, (
            "Org A's investment developer filter leaked Org B's author_email "
            f"across the repo_id+hash/number collision: {emails_a!r}"
        )
        assert emails_b == {"wu-org-b": {"mallory@org-b.example"}}, (
            "Org B's investment developer filter leaked Org A's author_email "
            f"across the repo_id+hash/number collision: {emails_b!r}"
        )
    finally:
        for table in ("git_commits", "git_pull_requests", "work_unit_investments"):
            for org_id in (org_a, org_b):
                sink.client.command(
                    f"ALTER TABLE {table} DELETE WHERE org_id = {{o:String}} "
                    "SETTINGS mutations_sync=2",
                    parameters={"o": org_id},
                )
