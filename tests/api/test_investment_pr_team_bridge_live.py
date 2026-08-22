"""Live-ClickHouse behavioral proof for CHAOS-2416 (Investment PR team bridge).

A work unit reaches a `TEAM` node only through the work-item refs recorded in
`work_unit_investments.structural_evidence_json`. Until this change the ONLY
bridge the `unit_team` CTE consulted was the `issues` array, so a unit whose
PR work item ALREADY carried a resolver-computed team still collapsed into the
false `TEAM:unassigned` node whenever the unit happened to carry no issue ref
of its own. The 2026-08-22 prod probe sized that at 375,821 loc -- 49.6% of the
whole unassigned bucket -- with work unit `9626aea0...` (`issues: []`, PR
`ghpr:full-chaos/dev-health-ops#1726` attributed to Fullchaos via
`linked_issue`) as the smoking gun.

This file seeds exactly that shape against REAL ClickHouse and proves:

* RED CONTROL (`test_pr_only_unit_is_unassigned_without_the_repos_bridge`):
  the pre-fix join key -- issues array only -- still leaves the fixture
  unassigned, so the passing tests below cannot be explained by the fixture
  merely carrying a team somewhere reachable.
* the REST Sankey/coverage fetchers resolve the PR's attributed team and
  report `missing_team == 0`;
* the GraphQL Sankey compiler resolves the SAME label (REST/GraphQL parity --
  the CTE is rendered from one shared builder, and this proves both renders);
* NEGATIVE CONTROL: a unit whose PR work item has NO primary attribution row
  stays `unassigned`. The bridge reuses teams the CHAOS-2600 resolver already
  computed; it must never fabricate one.
* NEGATIVE CONTROL: a `prs` ref whose repo UUID is absent from `repos` cannot
  be rewritten into the work-item id space and stays `unassigned`.
* provider coverage (AGENTS.md provider x entity contract -- never
  Linear-only): the GitHub `ghpr:{repo}#{n}` and GitLab `gitlab:{repo}!{n}`
  merge-request id namespaces are BOTH bridged, driven off `repos.provider`.

Every id is minted by the real producers -- `work_graph.ids.generate_pr_id`
for the evidence ref and `external_ingest.ids.derive_work_item_id` for the
work-item id -- so a future change to either format breaks this test instead
of silently un-bridging the join.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``. Provision an
ISOLATED scratch DB first, e.g.::

    docker exec dev-health-clickhouse-1 clickhouse-client --query \\
        "CREATE DATABASE IF NOT EXISTS ci_live_2416"
    CLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/ci_live_2416 \\
        .venv/bin/python -m pytest tests/api/test_investment_pr_team_bridge_live.py -m clickhouse
    docker exec dev-health-clickhouse-1 clickhouse-client --query \\
        "DROP DATABASE ci_live_2416"
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urlparse

import pytest

import dev_health_ops.api.queries.investment as investment_queries
from dev_health_ops.external_ingest.ids import derive_work_item_id
from dev_health_ops.metrics.schemas import (
    WorkItemTeamAttributionRecord,
    WorkUnitInvestmentRecord,
)
from dev_health_ops.work_graph.ids import generate_pr_id

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason=(
            "Requires CLICKHOUSE_URI pointed at an ISOLATED scratch DB, e.g. "
            "clickhouse://ch:ch@localhost:8123/ci_live_2416"
        ),
    ),
]

FROM_TS = datetime(2026, 1, 5, tzinfo=timezone.utc)
TO_TS = datetime(2026, 1, 6, tzinfo=timezone.utc)
COMPUTED_AT = datetime(2026, 1, 7, tzinfo=timezone.utc)
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 2, 1, tzinfo=timezone.utc)
START_DATE = date(2026, 1, 1)
END_DATE = date(2026, 2, 1)

GITHUB_REPO_SLUG = "full-chaos/dev-health-ops"
GITHUB_PR_NUMBER = 1726
GITLAB_REPO_SLUG = "full.chaos/dev-health-ops"
GITLAB_MR_NUMBER = 42
EFFORT = 100.0


def _scratch_db() -> str:
    assert CLICKHOUSE_URI is not None
    return (urlparse(CLICKHOUSE_URI).path or "").lstrip("/")


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    # Safety rule (repo policy): ``ensure_schema(force=True)`` rebuilds tables,
    # so this live test must NEVER touch the real local ``default`` database.
    db = _scratch_db()
    if db in ("", "default"):
        pytest.skip(
            "refusing to run against the 'default' database; point CLICKHOUSE_URI "
            "at an isolated scratch DB (e.g. .../ci_live_2416)"
        )
    s = ClickHouseMetricsSink(CLICKHOUSE_URI)
    s.ensure_schema(force=True)
    yield s
    s.close()


def _cleanup(sink: Any, org_id: str, repo_ids: list[uuid.UUID]) -> None:
    for table in ("work_unit_investments", "work_item_team_attributions"):
        sink.client.command(
            f"ALTER TABLE {table} DELETE WHERE org_id = {{o:String}} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )
    for repo_id in repo_ids:
        sink.client.command(
            "ALTER TABLE repos DELETE WHERE id = {r:UUID} SETTINGS mutations_sync=2",
            parameters={"r": str(repo_id)},
        )


def _seed_repo(
    sink: Any,
    org: str,
    *,
    repo_id: uuid.UUID,
    slug: str,
    provider: str,
    last_synced: datetime | None = None,
):
    """`repos` is a raw git-sync table with no metrics-sink writer; the columns
    written here are exactly those the bridge reads (id -> repo, provider)."""
    sink.client.insert(
        "repos",
        [[repo_id, slug, COMPUTED_AT, last_synced or COMPUTED_AT, provider, org]],
        column_names=["id", "repo", "created_at", "last_synced", "provider", "org_id"],
    )


def _seed_pr_only_work_unit(
    sink: Any,
    org: str,
    *,
    work_unit_id: str,
    repo_id: uuid.UUID,
    pr_number: int,
) -> None:
    """A work unit with an EMPTY `issues` array and one `prs` ref -- the exact
    prod shape (unit 9626aea0..., 220,373 loc) this ticket is about."""
    evidence = {
        "issues": [],
        "prs": [generate_pr_id(repo_id, pr_number)],
        "commits": [],
        "edges": [],
    }
    sink.write_work_unit_investments(
        [
            WorkUnitInvestmentRecord(
                work_unit_id=work_unit_id,
                work_unit_type="pr",
                work_unit_name=f"PR #{pr_number}",
                from_ts=FROM_TS,
                to_ts=TO_TS,
                repo_id=repo_id,
                provider="github",
                effort_metric="fte_days",
                effort_value=EFFORT,
                theme_distribution_json={"Feature Delivery": 1.0},
                subcategory_distribution_json={"Feature Delivery.product": 1.0},
                structural_evidence_json=json.dumps(evidence),
                evidence_quality=0.9,
                evidence_quality_band="high",
                categorization_status="ok",
                categorization_errors_json="[]",
                categorization_model_version="test",
                categorization_input_hash="hash",
                categorization_run_id="run",
                computed_at=COMPUTED_AT,
                org_id=org,
            )
        ]
    )


def _seed_pr_attribution(
    sink: Any,
    org: str,
    *,
    work_item_id: str,
    team_id: str,
    team_name: str,
    provider: str,
) -> None:
    """The team the CHAOS-2600 resolver already computed for the PR work item.
    `linked_issue` is the source the prod smoking gun carried: the PR inherited
    Fullchaos from `linear:CHAOS-3498` through `work_item_dependencies`."""
    sink.write_work_item_team_attributions(
        [
            WorkItemTeamAttributionRecord(
                work_item_id=work_item_id,
                provider=provider,
                source="linked_issue",
                is_primary=1,
                confidence="high",
                evidence="linked issue donor",
                computed_at=COMPUTED_AT,
                repo_id=None,
                team_id=team_id,
                team_name=team_name,
                org_id=org,
            )
        ]
    )


async def _team_edges(sink: Any, org: str) -> dict[str, float]:
    rows = await investment_queries.fetch_investment_team_edges(
        sink,
        start_ts=START,
        end_ts=END,
        scope_filter="",
        scope_params={},
        org_id=org,
    )
    return {str(row["target"]): float(row["value"]) for row in rows}


@pytest.mark.asyncio
async def test_pr_only_unit_is_unassigned_without_the_repos_bridge(sink):
    """RED CONTROL.

    Runs the PRE-FIX join key against the post-fix fixture: resolve teams
    through `structural_evidence_json.issues` alone. The fixture's `issues`
    array is empty, so the unit MUST come back unassigned -- which is exactly
    what the product showed before this change. If this test ever goes green
    the fixture has stopped reproducing the bug and every assertion below
    becomes vacuous.
    """
    org = f"test-chaos-2416-red-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG, provider="github")
        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-2416-red",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
        )
        _seed_pr_attribution(
            sink,
            org,
            work_item_id=work_item_id,
            team_id="team-fullchaos",
            team_name="Fullchaos",
            provider="github",
        )

        pre_fix_sql = f"""
            WITH {investment_queries.LATEST_WORK_UNIT_INVESTMENTS_CTE},
            unit_team AS (
                SELECT
                    work_unit_id,
                    argMax(team, cnt) AS team
                FROM (
                    SELECT
                        work_unit_investments.work_unit_id AS work_unit_id,
                        ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) AS team,
                        countIf(
                            ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, ''))
                            IS NOT NULL
                        ) AS cnt
                    FROM latest_work_unit_investments AS work_unit_investments
                    ARRAY JOIN arrayDistinct(arrayConcat(
                        JSONExtract(structural_evidence_json, 'issues', 'Array(String)'),
                        [work_unit_investments.work_unit_id]
                    )) AS issue_id
                    LEFT JOIN
                        {investment_queries.PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE}
                        AS t ON t.work_item_id = issue_id
                    WHERE work_unit_investments.from_ts < %(end_ts)s
                      AND work_unit_investments.to_ts >= %(start_ts)s
                      AND work_unit_investments.org_id = %(org_id)s
                    GROUP BY work_unit_id, team
                )
                GROUP BY work_unit_id
            )
            SELECT ifNull(nullIf(unit_team.team, ''), 'unassigned') AS team
            FROM latest_work_unit_investments AS work_unit_investments
            LEFT JOIN unit_team
                ON unit_team.work_unit_id = work_unit_investments.work_unit_id
            WHERE work_unit_investments.from_ts < %(end_ts)s
              AND work_unit_investments.to_ts >= %(start_ts)s
              AND work_unit_investments.org_id = %(org_id)s
        """
        from dev_health_ops.api.queries.client import query_dicts

        rows = await query_dicts(
            sink,
            pre_fix_sql,
            {"start_ts": START, "end_ts": END, "org_id": org},
        )
        assert rows, "fixture did not land in the query window"
        assert all(str(row["team"]) == "unassigned" for row in rows), rows

        # ... and the SAME fixture is teamed once the PR bridge is in play.
        assert await _team_edges(sink, org) == {"Fullchaos": pytest.approx(EFFORT)}
    finally:
        _cleanup(sink, org, [repo_id])


@pytest.mark.asyncio
async def test_rest_fetchers_resolve_team_through_the_pr_bridge(sink):
    org = f"test-chaos-2416-rest-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG, provider="github")
        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-2416-rest",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
        )
        _seed_pr_attribution(
            sink,
            org,
            work_item_id=work_item_id,
            team_id="team-fullchaos",
            team_name="Fullchaos",
            provider="github",
        )

        by_team = await _team_edges(sink, org)
        assert by_team.get("Fullchaos") == pytest.approx(EFFORT), by_team
        assert "unassigned" not in by_team, by_team

        for fetcher in (
            investment_queries.fetch_investment_repo_team_edges,
            investment_queries.fetch_investment_team_category_repo_edges,
            investment_queries.fetch_investment_team_subcategory_repo_edges,
        ):
            rows = await fetcher(
                sink,
                start_ts=START,
                end_ts=END,
                scope_filter="",
                scope_params={},
                org_id=org,
            )
            assert rows, fetcher.__name__
            assert all(str(row["team"]) == "Fullchaos" for row in rows), (
                fetcher.__name__,
                rows,
            )

        counts = await investment_queries.fetch_investment_unassigned_counts(
            sink,
            start_ts=START,
            end_ts=END,
            scope_filter="",
            scope_params={},
            org_id=org,
        )
        assert counts["missing_team"] == 0, counts
    finally:
        _cleanup(sink, org, [repo_id])


@pytest.mark.asyncio
async def test_gitlab_merge_request_evidence_is_bridged_too(sink):
    """Provider coverage: the same `{repo_uuid}#pr{n}` evidence ref resolves
    into the GitLab merge-request namespace (`gitlab:{repo}!{n}`) when
    `repos.provider` says gitlab -- never a GitHub-only bridge."""
    org = f"test-chaos-2416-gitlab-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = derive_work_item_id(
        system="gitlab",
        instance=GITLAB_REPO_SLUG,
        external_key=str(GITLAB_MR_NUMBER),
        work_item_type="merge_request",
    )
    assert work_item_id == f"gitlab:{GITLAB_REPO_SLUG}!{GITLAB_MR_NUMBER}"
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITLAB_REPO_SLUG, provider="gitlab")
        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-2416-gitlab",
            repo_id=repo_id,
            pr_number=GITLAB_MR_NUMBER,
        )
        _seed_pr_attribution(
            sink,
            org,
            work_item_id=work_item_id,
            team_id="team-platform",
            team_name="Platform",
            provider="gitlab",
        )

        by_team = await _team_edges(sink, org)
        assert by_team.get("Platform") == pytest.approx(EFFORT), by_team
        assert "unassigned" not in by_team, by_team
    finally:
        _cleanup(sink, org, [repo_id])


@pytest.mark.asyncio
async def test_pr_without_attribution_stays_unassigned(sink):
    """NEGATIVE CONTROL: the bridge reuses a team the resolver already
    computed. A PR work item with no primary attribution row must leave the
    unit unassigned -- no fabricated teams."""
    org = f"test-chaos-2416-negative-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG, provider="github")
        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-2416-negative",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
        )
        # deliberately NO write_work_item_team_attributions call

        by_team = await _team_edges(sink, org)
        assert by_team == {"unassigned": pytest.approx(EFFORT)}, by_team

        counts = await investment_queries.fetch_investment_unassigned_counts(
            sink,
            start_ts=START,
            end_ts=END,
            scope_filter="",
            scope_params={},
            org_id=org,
        )
        assert counts["missing_team"] == 1, counts
    finally:
        _cleanup(sink, org, [repo_id])


@pytest.mark.asyncio
async def test_unknown_repo_uuid_stays_unassigned(sink):
    """NEGATIVE CONTROL / input symmetry: a `prs` ref whose repo UUID is not in
    `repos` cannot be rewritten into the work-item id space. It must fall
    through as unassigned rather than matching an attribution row by accident
    (e.g. via the raw `{uuid}#pr{n}` string)."""
    org = f"test-chaos-2416-unknown-repo-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    try:
        # NO _seed_repo call -- the repos lookup misses.
        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-2416-unknown-repo",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
        )
        # An attribution row keyed on the RAW evidence ref must not be picked
        # up: the bridge resolves into the work_items id space, not this.
        _seed_pr_attribution(
            sink,
            org,
            work_item_id=generate_pr_id(repo_id, GITHUB_PR_NUMBER),
            team_id="team-should-not-win",
            team_name="Should Not Win",
            provider="github",
        )

        by_team = await _team_edges(sink, org)
        assert by_team == {"unassigned": pytest.approx(EFFORT)}, by_team
    finally:
        _cleanup(sink, org, [repo_id])


@pytest.mark.asyncio
async def test_graphql_sankey_compiler_resolves_the_same_pr_team(sink):
    """REST/GraphQL parity: the compiler renders the shared `unit_team` builder
    too, so it must reach the SAME team from the SAME fixture. A partial fix
    that touched only `api/queries/investment.py` fails here."""
    from dev_health_ops.api.graphql.sql.compiler import SankeyRequest, compile_sankey
    from dev_health_ops.api.queries.client import query_dicts

    org = f"test-chaos-2416-graphql-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG, provider="github")
        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-2416-graphql",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
        )
        _seed_pr_attribution(
            sink,
            org,
            work_item_id=work_item_id,
            team_id="team-fullchaos",
            team_name="Fullchaos",
            provider="github",
        )

        nodes_qs, _edges_qs = compile_sankey(
            SankeyRequest(
                path=["theme", "team"],
                measure="count",
                start_date=START_DATE,
                end_date=END_DATE,
                use_investment=True,
            ),
            org_id=org,
        )

        team_nodes: set[str] = set()
        for sql, params in nodes_qs:
            for row in await query_dicts(sink, sql, params):
                if str(row["dimension"]) == "TEAM":
                    team_nodes.add(str(row["node_id"]))

        assert "Fullchaos" in team_nodes, team_nodes
        assert "unassigned" not in team_nodes, team_nodes
    finally:
        _cleanup(sink, org, [repo_id])


def _team_scope_of(investment: Any) -> tuple[str, str]:
    for entry in investment.evidence.contextual:
        if entry.get("type") == "team_scope":
            return (
                str(entry.get("team_ids", ["?"])[0]),
                str(entry.get("team_names", ["?"])[0]),
            )
    raise AssertionError(f"no team_scope evidence: {investment.evidence.contextual}")


@pytest.mark.asyncio
async def test_work_unit_drilldown_agrees_with_the_sankey(sink):
    """The work-unit drilldown resolves a unit's team in PYTHON
    (`api/services/work_units.py`), a hand-written mirror of the `unit_team`
    SQL CTE. Fixing only the SQL would make the Sankey say "Fullchaos" while
    clicking into the very same unit still said "unassigned" -- the dual-path
    trap. This pins the two together on one fixture.
    """
    from dev_health_ops.api.models.filters import MetricFilter, TimeFilter
    from dev_health_ops.api.services.work_units import build_work_unit_investments

    org = f"test-chaos-2416-drilldown-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG, provider="github")
        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-2416-drilldown",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
        )
        _seed_pr_attribution(
            sink,
            org,
            work_item_id=work_item_id,
            team_id="team-fullchaos",
            team_name="Fullchaos",
            provider="github",
        )

        filters = MetricFilter(
            time=TimeFilter(start_date=START_DATE, end_date=END_DATE)
        )
        investments = await build_work_unit_investments(
            db_url=str(CLICKHOUSE_URI),
            filters=filters,
            org_id=org,
            include_text=False,
            work_unit_id="wu-2416-drilldown",
        )
        assert investments, "drilldown returned no rows for the seeded unit"
        team_id, team_name = _team_scope_of(investments[0])
        assert (team_id, team_name) == ("team-fullchaos", "Fullchaos"), (
            team_id,
            team_name,
        )

        # ... and the Sankey agrees, from the same fixture.
        assert await _team_edges(sink, org) == {"Fullchaos": pytest.approx(EFFORT)}
    finally:
        _cleanup(sink, org, [repo_id])


@pytest.mark.asyncio
async def test_work_unit_drilldown_does_not_fabricate_a_team(sink):
    """NEGATIVE CONTROL for the drilldown mirror: no attribution row for the
    PR work item means the drilldown stays unassigned, exactly as the Sankey
    does."""
    from dev_health_ops.api.models.filters import MetricFilter, TimeFilter
    from dev_health_ops.api.services.work_units import build_work_unit_investments

    org = f"test-chaos-2416-drilldown-negative-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG, provider="github")
        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-2416-drilldown-negative",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
        )

        investments = await build_work_unit_investments(
            db_url=str(CLICKHOUSE_URI),
            filters=MetricFilter(
                time=TimeFilter(start_date=START_DATE, end_date=END_DATE)
            ),
            org_id=org,
            include_text=False,
            work_unit_id="wu-2416-drilldown-negative",
        )
        assert investments, "drilldown returned no rows for the seeded unit"
        assert _team_scope_of(investments[0]) == ("unassigned", "Unassigned")
        assert await _team_edges(sink, org) == {"unassigned": pytest.approx(EFFORT)}
    finally:
        _cleanup(sink, org, [repo_id])


@pytest.mark.asyncio
async def test_shared_team_subquery_is_tenant_isolated(sink):
    """Cross-tenant collision guard.

    `build_unit_team_subquery` groups by `work_unit_id` alone and the GraphQL
    compiler / analytics coverage call it with no WHERE of their own, which
    reads like a tenant leak. It is not: every call site's `source` is
    `latest_work_unit_investments` (or the repo-allocated source built from
    it), and that CTE applies `WHERE org_id = %(org_id)s` and groups by
    `(org_id, work_unit_id)` BEFORE the builder ever sees a row -- so the
    relation handed to the subquery is already single-tenant.

    That is an invariant of the call sites, not of the builder, so it deserves
    an executable guard rather than a comment. Two orgs share one
    `work_unit_id` with conflicting PR evidence pointing at different teams;
    each org must see only its own.
    """
    from dev_health_ops.api.graphql.sql.compiler import SankeyRequest, compile_sankey
    from dev_health_ops.api.queries.client import query_dicts

    suffix = uuid.uuid4()
    org_a = f"test-chaos-2416-tenant-a-{suffix}"
    org_b = f"test-chaos-2416-tenant-b-{suffix}"
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    shared_work_unit_id = f"wu-collision-{suffix}"
    slug_b = "full-chaos/dev-health-web"

    item_a = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    item_b = derive_work_item_id(
        system="github",
        instance=slug_b,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        _seed_repo(
            sink, org_a, repo_id=repo_a, slug=GITHUB_REPO_SLUG, provider="github"
        )
        _seed_repo(sink, org_b, repo_id=repo_b, slug=slug_b, provider="github")
        # SAME work_unit_id in both orgs, different PR evidence.
        _seed_pr_only_work_unit(
            sink,
            org_a,
            work_unit_id=shared_work_unit_id,
            repo_id=repo_a,
            pr_number=GITHUB_PR_NUMBER,
        )
        _seed_pr_only_work_unit(
            sink,
            org_b,
            work_unit_id=shared_work_unit_id,
            repo_id=repo_b,
            pr_number=GITHUB_PR_NUMBER,
        )
        _seed_pr_attribution(
            sink,
            org_a,
            work_item_id=item_a,
            team_id="team-a",
            team_name="Tenant A Team",
            provider="github",
        )
        _seed_pr_attribution(
            sink,
            org_b,
            work_item_id=item_b,
            team_id="team-b",
            team_name="Tenant B Team",
            provider="github",
        )

        # REST fetchers: each tenant sees exactly its own team, once.
        assert await _team_edges(sink, org_a) == {
            "Tenant A Team": pytest.approx(EFFORT)
        }
        assert await _team_edges(sink, org_b) == {
            "Tenant B Team": pytest.approx(EFFORT)
        }

        # GraphQL compiler -- the call site that passes no WHERE of its own.
        for org, expected, forbidden in (
            (org_a, "Tenant A Team", "Tenant B Team"),
            (org_b, "Tenant B Team", "Tenant A Team"),
        ):
            nodes_qs, _ = compile_sankey(
                SankeyRequest(
                    path=["theme", "team"],
                    measure="count",
                    start_date=START_DATE,
                    end_date=END_DATE,
                    use_investment=True,
                ),
                org_id=org,
            )
            team_nodes: set[str] = set()
            for sql, params in nodes_qs:
                for row in await query_dicts(sink, sql, params):
                    if str(row["dimension"]) == "TEAM":
                        team_nodes.add(str(row["node_id"]))
            assert expected in team_nodes, (org, team_nodes)
            assert forbidden not in team_nodes, (org, team_nodes)
    finally:
        _cleanup(sink, org_a, [repo_a])
        _cleanup(sink, org_b, [repo_b])


@pytest.mark.asyncio
async def test_repo_uuid_shared_across_orgs_resolves_per_tenant(sink):
    """Cross-tenant repo-identity guard.

    `repos.id` is derived from the repo SLUG alone
    (`external_ingest/ids.py:derive_repo_uuid` -> `get_repo_uuid_from_repo`,
    no org in the seed), so two orgs syncing the same repo mint the SAME UUID.
    `repos` is a ReplacingMergeTree keyed on `(org_id, id)` (migration 027 --
    NOT the `(id)` that migration 000 still declares), so both rows survive,
    one per tenant.

    A bridge that resolved a UUID without an org predicate would argMax across
    tenants and hand one org the other's slug. Here org B holds a DIFFERENT
    slug for the same UUID and is the NEWER writer, so a global argMax picks
    B's slug for everyone: org A would then build
    `ghpr:full-chaos/dev-health-web#N`, match no attribution row, and report
    `unassigned`. Both tenants must instead resolve their own row.
    """
    from dev_health_ops.api.models.filters import MetricFilter, TimeFilter
    from dev_health_ops.api.services.work_units import build_work_unit_investments

    suffix = uuid.uuid4()
    org_a = f"test-chaos-2416-shared-repo-a-{suffix}"
    org_b = f"test-chaos-2416-shared-repo-b-{suffix}"
    shared_repo_id = uuid.uuid4()  # same UUID in both orgs, as the seed implies
    slug_b = "full-chaos/dev-health-web"

    item_a = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    item_b = derive_work_item_id(
        system="github",
        instance=slug_b,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        _seed_repo(
            sink,
            org_a,
            repo_id=shared_repo_id,
            slug=GITHUB_REPO_SLUG,
            provider="github",
            last_synced=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        # Org B writes LATER: a cross-tenant argMax would elect this row.
        _seed_repo(
            sink,
            org_b,
            repo_id=shared_repo_id,
            slug=slug_b,
            provider="github",
            last_synced=datetime(2026, 1, 9, tzinfo=timezone.utc),
        )
        rows = sink.client.query(
            "SELECT count() FROM repos FINAL WHERE id = {r:UUID}",
            parameters={"r": str(shared_repo_id)},
        ).result_rows[0][0]
        assert rows == 2, (
            f"fixture premise: (org_id, id) keeps one row per tenant, saw {rows}"
        )

        for org, pr_number, item, team_id, team_name in (
            (org_a, GITHUB_PR_NUMBER, item_a, "team-a", "Tenant A Team"),
            (org_b, GITHUB_PR_NUMBER, item_b, "team-b", "Tenant B Team"),
        ):
            _seed_pr_only_work_unit(
                sink,
                org,
                work_unit_id=f"wu-2416-shared-{org[-8:]}",
                repo_id=shared_repo_id,
                pr_number=pr_number,
            )
            _seed_pr_attribution(
                sink,
                org,
                work_item_id=item,
                team_id=team_id,
                team_name=team_name,
                provider="github",
            )

        # Each tenant resolves through ITS OWN repo row, not the newer one.
        assert await _team_edges(sink, org_a) == {
            "Tenant A Team": pytest.approx(EFFORT)
        }
        assert await _team_edges(sink, org_b) == {
            "Tenant B Team": pytest.approx(EFFORT)
        }

        # ... and the drilldown agrees, for the tenant that is NOT the newest
        # writer -- the one a cross-tenant argMax would have broken.
        investments = await build_work_unit_investments(
            db_url=str(CLICKHOUSE_URI),
            filters=MetricFilter(
                time=TimeFilter(start_date=START_DATE, end_date=END_DATE)
            ),
            org_id=org_a,
            include_text=False,
            work_unit_id=f"wu-2416-shared-{org_a[-8:]}",
        )
        assert investments, "drilldown returned no rows"
        assert _team_scope_of(investments[0]) == ("team-a", "Tenant A Team")
    finally:
        _cleanup(sink, org_a, [])
        _cleanup(sink, org_b, [shared_repo_id])


@pytest.mark.asyncio
async def test_unmerged_repo_versions_resolve_deterministically(sink):
    """`repos` is a ReplacingMergeTree, so several versions of one id coexist
    until a background merge. Both bridges must pick the newest by
    `last_synced` -- and pick the SAME one -- rather than depending on row
    order. A stale slug rewrites the evidence ref to a work-item id nobody
    minted, which reads as `unassigned`.

    The older version deliberately carries a DIFFERENT slug, so resolving the
    wrong version yields no attribution match and the test fails loudly --
    whenever the pre-merge state is still observable.
    """
    from dev_health_ops.api.models.filters import MetricFilter, TimeFilter
    from dev_health_ops.api.services.work_units import build_work_unit_investments

    org = f"test-chaos-2416-unmerged-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        # Two SEPARATE inserts -- rows sharing a sorting key inside one block
        # are collapsed on the spot, so a single insert cannot reproduce the
        # pre-merge state this test is about. The stale version is written
        # SECOND, so the newer row does not simply happen to come first.
        columns = ["id", "repo", "created_at", "last_synced", "provider", "org_id"]
        sink.client.insert(
            "repos",
            [
                [
                    repo_id,
                    GITHUB_REPO_SLUG,
                    COMPUTED_AT,
                    datetime(2026, 1, 9, tzinfo=timezone.utc),
                    "github",
                    org,
                ]
            ],
            column_names=columns,
        )
        sink.client.insert(
            "repos",
            [
                [
                    repo_id,
                    "full-chaos/renamed-away",
                    COMPUTED_AT,
                    datetime(2026, 1, 2, tzinfo=timezone.utc),
                    "github",
                    org,
                ]
            ],
            column_names=columns,
        )
        # No assertion on the number of surviving parts: a background merge
        # may collapse them at any moment, and a test whose PREMISE depends on
        # merge timing is a flake. The behaviour asserted below holds either
        # way -- pre-merge the argMax picks the newest version, post-merge the
        # ReplacingMergeTree has already kept exactly that row. The mechanism
        # itself (argMax by last_synced, grouped by the dedup key) is pinned
        # deterministically in
        # tests/api/test_investment_team_attribution_sql.py.

        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-2416-unmerged",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
        )
        _seed_pr_attribution(
            sink,
            org,
            work_item_id=work_item_id,
            team_id="team-fullchaos",
            team_name="Fullchaos",
            provider="github",
        )

        # SQL bridge picks the newest version...
        assert await _team_edges(sink, org) == {"Fullchaos": pytest.approx(EFFORT)}

        # ... and the drilldown picks the same one.
        investments = await build_work_unit_investments(
            db_url=str(CLICKHOUSE_URI),
            filters=MetricFilter(
                time=TimeFilter(start_date=START_DATE, end_date=END_DATE)
            ),
            org_id=org,
            include_text=False,
            work_unit_id="wu-2416-unmerged",
        )
        assert investments, "drilldown returned no rows"
        assert _team_scope_of(investments[0]) == ("team-fullchaos", "Fullchaos")
    finally:
        _cleanup(sink, org, [repo_id])


def _seed_issue_and_pr_work_unit(
    sink: Any,
    org: str,
    *,
    work_unit_id: str,
    repo_id: uuid.UUID,
    pr_number: int,
    issue_id: str,
) -> None:
    """A unit reaching one team through an ISSUE and another through a PR --
    the equal-count tie the PR bridge makes ordinary."""
    evidence = {
        "issues": [issue_id],
        "prs": [generate_pr_id(repo_id, pr_number)],
        "commits": [],
        "edges": [],
    }
    sink.write_work_unit_investments(
        [
            WorkUnitInvestmentRecord(
                work_unit_id=work_unit_id,
                work_unit_type="pr",
                work_unit_name=f"PR #{pr_number}",
                from_ts=FROM_TS,
                to_ts=TO_TS,
                repo_id=repo_id,
                provider="github",
                effort_metric="fte_days",
                effort_value=EFFORT,
                theme_distribution_json={"Feature Delivery": 1.0},
                subcategory_distribution_json={"Feature Delivery.product": 1.0},
                structural_evidence_json=json.dumps(evidence),
                evidence_quality=0.9,
                evidence_quality_band="high",
                categorization_status="ok",
                categorization_errors_json="[]",
                categorization_model_version="test",
                categorization_input_hash="hash",
                categorization_run_id="run",
                computed_at=COMPUTED_AT,
                org_id=org,
            )
        ]
    )


@pytest.mark.asyncio
async def test_tied_issue_and_pr_teams_break_identically_everywhere(sink):
    """Tie-break parity across the SQL bridge and the Python drilldown.

    The PR bridge makes a two-team tie ordinary: one attributed issue for team
    A, one attributed PR for team B, one vote each. `argMax(team, cnt)` alone
    is order-dependent there, so SQL and the drilldown could name different
    teams for the SAME unit. Both sides now break the tie on (count, LABEL),
    so the winner is 'Zulu Team' over 'Alpha Team' deterministically --
    whichever path you ask.
    """
    from dev_health_ops.api.models.filters import MetricFilter, TimeFilter
    from dev_health_ops.api.services.work_units import build_work_unit_investments

    org = f"test-chaos-2416-tie-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    issue_id = "linear:CHAOS-3498"
    pr_item = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG, provider="github")
        _seed_issue_and_pr_work_unit(
            sink,
            org,
            work_unit_id="wu-2416-tie",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
            issue_id=issue_id,
        )
        _seed_pr_attribution(
            sink,
            org,
            work_item_id=issue_id,
            team_id="team-alpha",
            team_name="Alpha Team",
            provider="linear",
        )
        _seed_pr_attribution(
            sink,
            org,
            work_item_id=pr_item,
            team_id="team-zulu",
            team_name="Zulu Team",
            provider="github",
        )

        by_team = await _team_edges(sink, org)
        assert by_team == {"Zulu Team": pytest.approx(EFFORT)}, by_team

        for fetcher in (
            investment_queries.fetch_investment_repo_team_edges,
            investment_queries.fetch_investment_team_category_repo_edges,
            investment_queries.fetch_investment_team_subcategory_repo_edges,
        ):
            rows = await fetcher(
                sink,
                start_ts=START,
                end_ts=END,
                scope_filter="",
                scope_params={},
                org_id=org,
            )
            assert all(str(row["team"]) == "Zulu Team" for row in rows), (
                fetcher.__name__,
                rows,
            )

        investments = await build_work_unit_investments(
            db_url=str(CLICKHOUSE_URI),
            filters=MetricFilter(
                time=TimeFilter(start_date=START_DATE, end_date=END_DATE)
            ),
            org_id=org,
            include_text=False,
            work_unit_id="wu-2416-tie",
        )
        assert investments, "drilldown returned no rows"
        assert _team_scope_of(investments[0]) == ("team-zulu", "Zulu Team")
    finally:
        _cleanup(sink, org, [repo_id])


@pytest.mark.asyncio
async def test_one_team_id_with_conflicting_names_agrees_across_paths(sink):
    """Parity when one team_id is spelled with two different names.

    Attribution rows are per work item, so the same `team_id` can carry
    different `team_name` values on two refs of one unit. If the SQL counted
    votes per rendered LABEL it would see two one-vote candidates and pick the
    lexicographically larger, while the drilldown -- counting per ID -- would
    see one two-vote team and pick its first-seen name. Same unit, two
    different answers.

    Both sides now count per team_id and derive the label as max(label), so
    this unit is a single two-vote team on both paths.
    """
    from dev_health_ops.api.models.filters import MetricFilter, TimeFilter
    from dev_health_ops.api.services.work_units import build_work_unit_investments

    org = f"test-chaos-2416-name-drift-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    issue_id = "linear:CHAOS-3498"
    pr_item = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG, provider="github")
        _seed_issue_and_pr_work_unit(
            sink,
            org,
            work_unit_id="wu-2416-name-drift",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
            issue_id=issue_id,
        )
        # ONE team_id, two spellings of its name.
        _seed_pr_attribution(
            sink,
            org,
            work_item_id=issue_id,
            team_id="team-shared",
            team_name="Alpha Spelling",
            provider="linear",
        )
        _seed_pr_attribution(
            sink,
            org,
            work_item_id=pr_item,
            team_id="team-shared",
            team_name="Zulu Spelling",
            provider="github",
        )

        by_team = await _team_edges(sink, org)
        assert by_team == {"Zulu Spelling": pytest.approx(EFFORT)}, by_team

        investments = await build_work_unit_investments(
            db_url=str(CLICKHOUSE_URI),
            filters=MetricFilter(
                time=TimeFilter(start_date=START_DATE, end_date=END_DATE)
            ),
            org_id=org,
            include_text=False,
            work_unit_id="wu-2416-name-drift",
        )
        assert investments, "drilldown returned no rows"
        drill_id, drill_label = _team_scope_of(investments[0])
        assert drill_id == "team-shared", drill_id
        # The label the drilldown shows is the one the Sankey grouped under.
        assert drill_label == next(iter(by_team)), (drill_label, by_team)
    finally:
        _cleanup(sink, org, [repo_id])


@pytest.mark.asyncio
async def test_duplicate_evidence_ref_votes_once_on_both_paths(sink):
    """`arrayDistinct` parity.

    The SQL de-duplicates the combined issues+prs ref array before voting. A
    unit whose persisted evidence lists the SAME work item twice -- here the
    PR appears both as a raw `issues` entry and as a `prs` ref that translates
    to the identical id -- must therefore cast ONE vote, not two. If the
    drilldown counted it twice it could outvote a genuinely two-ref team and
    disagree with the Sankey.

    The unit reaches 'Duplicated Team' through the duplicated PR (1 vote after
    dedup) and 'Distinct Team' through two separate issues (2 votes), so the
    latter must win on BOTH paths. Without dedup the duplicated team ties at 2
    and its lexicographically larger id would steal the unit in the drilldown.
    """
    from dev_health_ops.api.models.filters import MetricFilter, TimeFilter
    from dev_health_ops.api.services.work_units import build_work_unit_investments

    org = f"test-chaos-2416-dupe-ref-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    pr_item = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    issue_a, issue_b = "linear:CHAOS-1001", "linear:CHAOS-1002"
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG, provider="github")
        # The PR is listed BOTH as a resolved issues entry and as a prs ref
        # that translates to the same work-item id.
        evidence = {
            "issues": [issue_a, issue_b, pr_item],
            "prs": [generate_pr_id(repo_id, GITHUB_PR_NUMBER)],
            "commits": [],
            "edges": [],
        }
        sink.write_work_unit_investments(
            [
                WorkUnitInvestmentRecord(
                    work_unit_id="wu-2416-dupe-ref",
                    work_unit_type="pr",
                    work_unit_name="dupe",
                    from_ts=FROM_TS,
                    to_ts=TO_TS,
                    repo_id=repo_id,
                    provider="github",
                    effort_metric="fte_days",
                    effort_value=EFFORT,
                    theme_distribution_json={"Feature Delivery": 1.0},
                    subcategory_distribution_json={"Feature Delivery.product": 1.0},
                    structural_evidence_json=json.dumps(evidence),
                    evidence_quality=0.9,
                    evidence_quality_band="high",
                    categorization_status="ok",
                    categorization_errors_json="[]",
                    categorization_model_version="test",
                    categorization_input_hash="hash",
                    categorization_run_id="run",
                    computed_at=COMPUTED_AT,
                    org_id=org,
                )
            ]
        )
        # 'zz-duplicated' sorts ABOVE 'aa-distinct', so a double-counted PR
        # would win the (count, team_id) tie-break.
        _seed_pr_attribution(
            sink,
            org,
            work_item_id=pr_item,
            team_id="zz-duplicated",
            team_name="Duplicated Team",
            provider="github",
        )
        for issue in (issue_a, issue_b):
            _seed_pr_attribution(
                sink,
                org,
                work_item_id=issue,
                team_id="aa-distinct",
                team_name="Distinct Team",
                provider="linear",
            )

        by_team = await _team_edges(sink, org)
        assert by_team == {"Distinct Team": pytest.approx(EFFORT)}, by_team

        investments = await build_work_unit_investments(
            db_url=str(CLICKHOUSE_URI),
            filters=MetricFilter(
                time=TimeFilter(start_date=START_DATE, end_date=END_DATE)
            ),
            org_id=org,
            include_text=False,
            work_unit_id="wu-2416-dupe-ref",
        )
        assert investments, "drilldown returned no rows"
        assert _team_scope_of(investments[0]) == ("aa-distinct", "Distinct Team")
    finally:
        _cleanup(sink, org, [repo_id])
