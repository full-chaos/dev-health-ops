"""Live-ClickHouse behavioral proof for CHAOS-4244.

18.47% of last-14d work units (87/471) were unassigned locally, all pure
GitHub-PR-only evidence against this project's own dogfooded repos, no linked
issue. `work_item_team_attributions` had ZERO `provider='github'` rows for all
time.

Measured cause (see `tests/metrics/test_pr_author_team_attribution.py` for the
producer-level proof): GitHub PRs ARE already modeled as `WorkItem`s
(`ghpr:{repo}#{n}`, `providers/github/normalize.py:541`) and DO flow into
`compute_work_item_team_attributions` (`compute_work_items.py:1189`)
unfiltered -- the CHAOS-2416 bridge (`build_unit_team_subquery`,
`api/queries/investment.py:399`) has always been correct. The gap was that
`resolve_team_attribution` only ever built a membership candidate from
`item.assignees` -- GitHub's "assignee" field, distinct from and far less
commonly set than the PR's author (`item.reporter`). A PR opened by a team
member with no assignee, no repo_patterns row, and no linked issue -- exactly
the shape of all 87 sampled units -- resolved `unassigned`.

This file proves the fix through the PRODUCTION READ PATH, not just the
producer function in isolation: run the real `compute_work_item_team_attributions`
against a PR-shaped `WorkItem` (author known, no assignee), persist through the
real sink, and assert `fetch_investment_team_edges` (which compiles
`build_unit_team_subquery`) resolves the team -- the same query the UI and the
ticket's own measurement used.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``. Provision an
ISOLATED scratch DB first, e.g.::

    docker exec dev-health-clickhouse-1 clickhouse-client --query \\
        "CREATE DATABASE IF NOT EXISTS ci_live_4244"
    CLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/ci_live_4244 \\
        .venv/bin/python -m pytest tests/api/test_pr_author_team_attribution_live.py -m clickhouse
    docker exec dev-health-clickhouse-1 clickhouse-client --query \\
        "DROP DATABASE ci_live_4244"
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import pytest

import dev_health_ops.api.queries.investment as investment_queries
from dev_health_ops.external_ingest.ids import derive_work_item_id
from dev_health_ops.metrics.compute_work_items import (
    TeamAttributionCandidate,
    TeamAttributionContext,
    compute_work_item_team_attributions,
)
from dev_health_ops.metrics.schemas import WorkUnitInvestmentRecord
from dev_health_ops.models.work_items import WorkItem
from dev_health_ops.providers.teams import TeamResolver
from dev_health_ops.work_graph.ids import generate_pr_id

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason=(
            "Requires CLICKHOUSE_URI pointed at an ISOLATED scratch DB, e.g. "
            "clickhouse://ch:ch@localhost:8123/ci_live_4244"
        ),
    ),
]

FROM_TS = datetime(2026, 1, 5, tzinfo=timezone.utc)
TO_TS = datetime(2026, 1, 6, tzinfo=timezone.utc)
COMPUTED_AT = datetime(2026, 1, 7, tzinfo=timezone.utc)
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 2, 1, tzinfo=timezone.utc)

GITHUB_REPO_SLUG = "full-chaos/dev-health-ops"
GITHUB_PR_NUMBER = 4244
EFFORT = 100.0


def _scratch_db() -> str:
    assert CLICKHOUSE_URI is not None
    return (urlparse(CLICKHOUSE_URI).path or "").lstrip("/")


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    db = _scratch_db()
    if db in ("", "default"):
        pytest.skip(
            "refusing to run against the 'default' database; point CLICKHOUSE_URI "
            "at an isolated scratch DB (e.g. .../ci_live_4244)"
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


def _seed_repo(sink: Any, org: str, *, repo_id: uuid.UUID, slug: str) -> None:
    sink.client.insert(
        "repos",
        [[repo_id, slug, COMPUTED_AT, COMPUTED_AT, "github", org]],
        column_names=["id", "repo", "created_at", "last_synced", "provider", "org_id"],
    )


def _seed_pr_only_work_unit(
    sink: Any, org: str, *, work_unit_id: str, repo_id: uuid.UUID, pr_number: int
) -> None:
    """The EXACT shape of all 87 sampled unassigned units in the ticket:
    empty `issues`, one `prs` ref, against this project's own dogfooded repo,
    no linked Linear/GitLab issue."""
    evidence = {"issues": [], "prs": [generate_pr_id(repo_id, pr_number)]}
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


async def _team_edges(sink: Any, org: str) -> dict[str, float]:
    rows = await investment_queries.fetch_investment_team_edges(
        sink, start_ts=START, end_ts=END, scope_filter="", scope_params={}, org_id=org
    )
    return {str(row["target"]): float(row["value"]) for row in rows}


@pytest.mark.asyncio
async def test_pr_only_unit_is_unassigned_without_the_author_candidate_red_control(
    sink,
):
    """RED CONTROL: run the pre-fix producer shape (no reporter passed through
    at all -- simulating item.reporter being ignored) against the exact
    fixture. Must stay unassigned, proving the fixture reproduces the bug and
    the passing test below is not vacuous."""
    org = f"test-chaos-4244-red-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG)
        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-4244-red",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
        )

        pr_item = WorkItem(
            work_item_id=work_item_id,
            provider="github",
            title="Fix attribution gap",
            type="pr",
            status="done",
            status_raw="merged",
            reporter=None,  # pre-fix shape: author never reaches the resolver
            assignees=[],
            created_at=COMPUTED_AT,
            updated_at=COMPUTED_AT,
            org_id=org,
        )
        records = compute_work_item_team_attributions(
            work_items=[pr_item],
            computed_at=COMPUTED_AT,
            team_resolver=TeamResolver(
                member_to_team={"alice": ("team-ops", "Ops Team")}
            ),
        )
        sink.write_work_item_team_attributions(records)

        by_team = await _team_edges(sink, org)
        assert by_team == {"unassigned": pytest.approx(EFFORT)}, by_team
    finally:
        _cleanup(sink, org, [repo_id])


@pytest.mark.asyncio
async def test_pr_author_resolves_through_the_real_producer_and_the_production_query(
    sink,
):
    """GREEN: the real producer function (compute_work_item_team_attributions)
    given the PR's author (item.reporter), persisted through the real sink,
    read back through the SAME query the Sankey/Allocation UI and the
    ticket's own measurement use (fetch_investment_team_edges ->
    build_unit_team_subquery)."""
    org = f"test-chaos-4244-green-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG)
        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-4244-green",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
        )

        pr_item = WorkItem(
            work_item_id=work_item_id,
            provider="github",
            title="Fix attribution gap",
            type="pr",
            status="done",
            status_raw="merged",
            reporter="alice",
            assignees=[],
            created_at=COMPUTED_AT,
            updated_at=COMPUTED_AT,
            org_id=org,
        )
        records = compute_work_item_team_attributions(
            work_items=[pr_item],
            computed_at=COMPUTED_AT,
            team_resolver=TeamResolver(
                member_to_team={"alice": ("team-ops", "Ops Team")}
            ),
        )
        assert any(r.is_primary and r.team_id == "team-ops" for r in records), records
        sink.write_work_item_team_attributions(records)

        by_team = await _team_edges(sink, org)
        assert by_team == {"Ops Team": pytest.approx(EFFORT)}, by_team

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
async def test_pr_with_no_author_match_falls_back_to_repo_ownership(sink):
    """SECOND red case (team-lead's ask): a PR whose repo has a repo_patterns
    /team_repo_ownership row resolves through repo_ownership -- unchanged,
    pre-existing logic this ticket's fix does not touch. Author is unknown
    (no membership match) so this proves the fallback tier fires on its own,
    not as a side effect of the author candidate."""
    org = f"test-chaos-4244-repo-fallback-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG)
        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-4244-repo-fallback",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
        )

        pr_item = WorkItem(
            work_item_id=work_item_id,
            provider="github",
            title="Fix attribution gap",
            type="pr",
            status="done",
            status_raw="merged",
            reporter="external-contributor",  # not a member of any team
            assignees=[],
            project_id=GITHUB_REPO_SLUG,
            created_at=COMPUTED_AT,
            updated_at=COMPUTED_AT,
            org_id=org,
        )
        context = TeamAttributionContext(
            repo_by_name={
                ("github", GITHUB_REPO_SLUG): [
                    TeamAttributionCandidate(
                        source="repo_ownership",
                        team_id="team-ops",
                        team_name="Ops Team",
                        confidence="medium",
                        evidence=f"repo_pattern={GITHUB_REPO_SLUG}",
                        is_primary=1,
                        specificity=30,
                    )
                ]
            }
        )
        records = compute_work_item_team_attributions(
            work_items=[pr_item],
            computed_at=COMPUTED_AT,
            team_resolver=TeamResolver(member_to_team={}),
            attribution_context=context,
        )
        primary = [r for r in records if r.is_primary]
        assert len(primary) == 1
        assert primary[0].source == "repo_ownership"
        assert primary[0].team_id == "team-ops"
        sink.write_work_item_team_attributions(records)

        by_team = await _team_edges(sink, org)
        assert by_team == {"Ops Team": pytest.approx(EFFORT)}, by_team
    finally:
        _cleanup(sink, org, [repo_id])


@pytest.mark.asyncio
async def test_backfill_next_run_attributes_a_previously_unassigned_unit(sink):
    """CHAOS-4244 deliverable #3: an existing unassigned unit must get
    attributed on the NEXT producer run without a manual step -- no unit
    re-materialization, no new work_unit_investments row. This simulates a
    'day 1' run (pre-fix shape, unassigned) followed by a 'day 2' run
    (post-fix code, same WorkItem re-derived) and asserts the SAME work unit
    flips from unassigned to attributed purely from the new attribution
    snapshot -- exactly what a normal recompute does, since
    work_item_team_attributions is a ReplacingMergeTree keyed to always read
    the latest computed_at per work_item_id (PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE)."""
    org = f"test-chaos-4244-backfill-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    work_item_id = derive_work_item_id(
        system="github",
        instance=GITHUB_REPO_SLUG,
        external_key=str(GITHUB_PR_NUMBER),
        work_item_type="pr",
    )
    try:
        _seed_repo(sink, org, repo_id=repo_id, slug=GITHUB_REPO_SLUG)
        _seed_pr_only_work_unit(
            sink,
            org,
            work_unit_id="wu-4244-backfill",
            repo_id=repo_id,
            pr_number=GITHUB_PR_NUMBER,
        )

        # "Day 1": the run BEFORE this fix -- resolver never sees the author.
        stale_item = WorkItem(
            work_item_id=work_item_id,
            provider="github",
            title="Fix attribution gap",
            type="pr",
            status="done",
            status_raw="merged",
            reporter=None,
            assignees=[],
            created_at=COMPUTED_AT,
            updated_at=COMPUTED_AT,
            org_id=org,
        )
        stale_records = compute_work_item_team_attributions(
            work_items=[stale_item],
            computed_at=COMPUTED_AT,
            team_resolver=TeamResolver(
                member_to_team={"alice": ("team-ops", "Ops Team")}
            ),
        )
        sink.write_work_item_team_attributions(stale_records)
        assert await _team_edges(sink, org) == {"unassigned": pytest.approx(EFFORT)}

        # "Day 2": next scheduled producer run, same WorkItem, fixed code --
        # no manual backfill step, no re-sync, just the normal recompute
        # picking up the SAME provider-fetched item (reporter is a normalized
        # field on the row, sourced from the PR's real author every sync).
        next_run_at = datetime(2026, 1, 8, tzinfo=timezone.utc)
        fresh_item = WorkItem(
            work_item_id=work_item_id,
            provider="github",
            title="Fix attribution gap",
            type="pr",
            status="done",
            status_raw="merged",
            reporter="alice",
            assignees=[],
            created_at=COMPUTED_AT,
            updated_at=COMPUTED_AT,
            org_id=org,
        )
        fresh_records = compute_work_item_team_attributions(
            work_items=[fresh_item],
            computed_at=next_run_at,
            team_resolver=TeamResolver(
                member_to_team={"alice": ("team-ops", "Ops Team")}
            ),
        )
        sink.write_work_item_team_attributions(fresh_records)

        # Same work unit, no re-materialization -- now attributed.
        assert await _team_edges(sink, org) == {"Ops Team": pytest.approx(EFFORT)}
    finally:
        _cleanup(sink, org, [repo_id])
