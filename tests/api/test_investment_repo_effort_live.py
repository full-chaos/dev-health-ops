"""Live-ClickHouse proof for CHAOS-2777.

PR #1106 added ``work_unit_repo_effort`` (per-(work_unit, repo) effort fan-out)
so multi-repo work units -- whose scalar ``work_unit_investments.repo_id`` is
NULL -- can map effort to their real repos. The Allocation-tab Sankey fetchers
in ``api/queries/investment.py`` originally read only the scalar ``repo_id``, so
ANY multi-repo unit collapsed onto 'Unassigned repo'. These tests seed
controlled fixtures and assert, against REAL ClickHouse:

* the per-repo effort split + SUM INVARIANT (each unit contributes exactly its
  original effort, so the grand total is unchanged versus the scalar path),
* the scalar fallback (a unit with no allocation row keeps its scalar repo +
  full effort and is never dropped),
* ``missing_repo`` counts only units with a NULL scalar repo AND no allocation,
* theme filtering still interacts correctly with the fan-out,
* generation scoping -- a later, SMALLER allocation generation for the same
  work_unit_id supersedes the older one, so stale repos are not fanned and the
  sum invariant survives churn/skip rewrites (CHAOS-2777 round 2 / HIGH 1),
* scope-filter symmetry -- a repo scope resolves the team for a multi-repo unit
  via its in-scope allocated repo instead of collapsing to 'unassigned'
  (CHAOS-2777 round 2 / HIGH 2).

``ensure_schema(force=True)`` (re)creates the full schema, so this file must run
against an ISOLATED scratch DB, never the real local ``default``. Provision one
by hand, e.g.::

    docker exec dev-health-clickhouse-1 clickhouse-client --query \\
        "CREATE DATABASE IF NOT EXISTS ci_live_2777"
    CLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/ci_live_2777 \\
        .venv/bin/python -m pytest tests/api/test_investment_repo_effort_live.py -m clickhouse
    docker exec dev-health-clickhouse-1 clickhouse-client --query \\
        "DROP DATABASE ci_live_2777"

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import pytest

import dev_health_ops.api.queries.investment as investment_queries
from dev_health_ops.metrics.schemas import WorkItemTeamAttributionRecord

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason=(
            "Requires CLICKHOUSE_URI pointed at an ISOLATED scratch DB, e.g. "
            "clickhouse://ch:ch@localhost:8123/ci_live_2777"
        ),
    ),
]

FROM_TS = datetime(2026, 1, 5, tzinfo=timezone.utc)
TO_TS = datetime(2026, 1, 6, tzinfo=timezone.utc)
COMPUTED_AT = datetime(2026, 1, 7, tzinfo=timezone.utc)
# Two allocation generations for the stale-generation test: GEN2 is newer.
GEN1_AT = datetime(2026, 1, 7, tzinfo=timezone.utc)
GEN2_AT = datetime(2026, 1, 9, tzinfo=timezone.utc)
# Query window strictly containing [FROM_TS, TO_TS).
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 2, 1, tzinfo=timezone.utc)


def _scratch_db() -> str:
    assert CLICKHOUSE_URI is not None
    return (urlparse(CLICKHOUSE_URI).path or "").lstrip("/")


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    # Safety rule (repo policy): ``ensure_schema(force=True)`` rebuilds tables, so
    # this live test must NEVER touch the real local ``default`` database.
    db = _scratch_db()
    if db in ("", "default"):
        pytest.skip(
            "refusing to run against the 'default' database; point CLICKHOUSE_URI "
            "at an isolated scratch DB (e.g. .../ci_live_2777)"
        )
    s = ClickHouseMetricsSink(CLICKHOUSE_URI)
    s.ensure_schema(force=True)
    yield s
    s.close()


def _wui_cols() -> list[str]:
    return [
        "work_unit_id",
        "from_ts",
        "to_ts",
        "repo_id",
        "effort_metric",
        "effort_value",
        "subcategory_distribution_json",
        "structural_evidence_json",
        "computed_at",
        "org_id",
    ]


def _wure_cols() -> list[str]:
    return [
        "work_unit_id",
        "repo_id",
        "effort_metric",
        "effort_value",
        "allocation_weight",
        "allocation_source",
        "computed_at",
        "org_id",
    ]


def _repo_cols() -> list[str]:
    return ["id", "repo", "created_at", "last_synced", "org_id"]


async def _rows(
    sink: Any,
    org_id: str,
    *,
    scope_filter: str = "",
    scope_params: dict | None = None,
    **kwargs,
) -> list[dict]:
    return await investment_queries.fetch_investment_team_subcategory_repo_edges(
        sink,
        start_ts=START,
        end_ts=END,
        scope_filter=scope_filter,
        scope_params=scope_params or {},
        org_id=org_id,
        **kwargs,
    )


async def _edges(sink: Any, org_id: str, **kwargs) -> dict[str, float]:
    by_repo: dict[str, float] = {}
    for row in await _rows(sink, org_id, **kwargs):
        # Mirror build_investment_flow_response: an empty repo label (a NULL /
        # unmatched repo_id) is surfaced as the 'unassigned' repo bucket.
        label = str(row["repo"]) or "unassigned"
        by_repo[label] = by_repo.get(label, 0.0) + float(row["value"])
    return by_repo


def _cleanup(sink: Any, org_id: str) -> None:
    for table in (
        "work_unit_investments",
        "work_unit_repo_effort",
        "repos",
        "work_item_cycle_times",
        "work_item_team_attributions",
    ):
        sink.client.command(
            f"ALTER TABLE {table} DELETE WHERE org_id = {{o:String}} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )


@pytest.mark.asyncio
async def test_allocation_sankey_reads_repo_effort(sink):
    org = f"test-chaos-2777-{uuid.uuid4()}"
    repo1, repo2, repo3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

    feature = {"Feature Delivery.product": 1.0}
    ktlo = {"Keeping the Lights On.ops": 1.0}

    try:
        # Give the three repo_ids resolvable names; the repos LEFT JOIN keys on
        # id only, so random UUIDs avoid collisions with existing rows.
        sink.client.insert(
            "repos",
            [
                [repo1, "repo-one", COMPUTED_AT, COMPUTED_AT, org],
                [repo2, "repo-two", COMPUTED_AT, COMPUTED_AT, org],
                [repo3, "repo-three", COMPUTED_AT, COMPUTED_AT, org],
            ],
            column_names=_repo_cols(),
        )
        sink.client.insert(
            "work_unit_investments",
            [
                # Multi-repo unit: NULL scalar repo, effort split 70/30 across repo1/repo2.
                [
                    "wu-multi",
                    FROM_TS,
                    TO_TS,
                    None,
                    "fte_days",
                    100.0,
                    feature,
                    "{}",
                    COMPUTED_AT,
                    org,
                ],
                # Single-repo unit: scalar repo set, NO allocation row -> scalar fallback.
                [
                    "wu-scalar",
                    FROM_TS,
                    TO_TS,
                    repo3,
                    "fte_days",
                    40.0,
                    feature,
                    "{}",
                    COMPUTED_AT,
                    org,
                ],
                # Genuinely repo-less unit: NULL scalar repo AND no allocation row.
                [
                    "wu-norepo",
                    FROM_TS,
                    TO_TS,
                    None,
                    "fte_days",
                    25.0,
                    ktlo,
                    "{}",
                    COMPUTED_AT,
                    org,
                ],
            ],
            column_names=_wui_cols(),
        )
        sink.client.insert(
            "work_unit_repo_effort",
            [
                [
                    "wu-multi",
                    repo1,
                    "fte_days",
                    70.0,
                    0.7,
                    "structural",
                    COMPUTED_AT,
                    org,
                ],
                [
                    "wu-multi",
                    repo2,
                    "fte_days",
                    30.0,
                    0.3,
                    "structural",
                    COMPUTED_AT,
                    org,
                ],
            ],
            column_names=_wure_cols(),
        )

        # --- Fan-out + sum invariant (no filters) ---
        by_repo = await _edges(sink, org)
        assert by_repo.get("repo-one") == pytest.approx(70.0), by_repo
        assert by_repo.get("repo-two") == pytest.approx(30.0), by_repo
        # Scalar fallback: unit with no allocation row keeps scalar repo + full effort.
        assert by_repo.get("repo-three") == pytest.approx(40.0), by_repo
        # Repo-less unit still lands on 'unassigned' with its full effort.
        assert by_repo.get("unassigned") == pytest.approx(25.0), by_repo
        # SUM INVARIANT: total is unchanged versus the scalar path (100 + 40 + 25).
        assert sum(by_repo.values()) == pytest.approx(165.0), by_repo
        # The multi-repo unit's per-repo effort sums back to its unit total.
        assert by_repo["repo-one"] + by_repo["repo-two"] == pytest.approx(100.0)

        # --- Unassigned counts: only the NULL-scalar + no-allocation unit ---
        counts = await investment_queries.fetch_investment_unassigned_counts(
            sink,
            start_ts=START,
            end_ts=END,
            scope_filter="",
            scope_params={},
            org_id=org,
        )
        assert counts["missing_repo"] == 1, counts

        # --- Theme filter interaction: only Feature Delivery units survive ---
        by_repo_ft = await _edges(sink, org, themes=["Feature Delivery"])
        assert by_repo_ft.get("repo-one") == pytest.approx(70.0)
        assert by_repo_ft.get("repo-two") == pytest.approx(30.0)
        assert by_repo_ft.get("repo-three") == pytest.approx(40.0)
        assert "unassigned" not in by_repo_ft  # KTLO no-repo unit filtered out
        assert sum(by_repo_ft.values()) == pytest.approx(140.0)

        counts_ft = await investment_queries.fetch_investment_unassigned_counts(
            sink,
            start_ts=START,
            end_ts=END,
            scope_filter="",
            scope_params={},
            org_id=org,
            themes=["Feature Delivery"],
        )
        assert counts_ft["missing_repo"] == 0, counts_ft
    finally:
        _cleanup(sink, org)


@pytest.mark.asyncio
async def test_repo_effort_scopes_to_latest_generation(sink):
    """CHAOS-2777 round 2 / HIGH 1: work_unit_repo_effort is deduped per
    (org, work_unit_id, repo_id), so when a later materialize generation emits a
    SMALLER repo set for the same work_unit_id (a repo's churn share drops to
    zero), the stale (unit, dropped-repo) row survives under its own older
    computed_at. Without generation scoping the fan-out would add that stale
    repo's effort and break the sum invariant.

    Two generations for one unit: GEN1 splits 40/60 across repo1/repo2; GEN2
    (newer) allocates the whole 100 to repo1 only. The unit's investments row is
    left at GEN1's clock (older than GEN2) to mirror the categorization-skipped
    path, which rewrites allocation newer than the investments row -- proving the
    generation clock is the allocation table's own per-unit max, not the
    investments computed_at. Only GEN2 must be fanned out.
    """
    org = f"test-chaos-2777-gen-{uuid.uuid4()}"
    repo1, repo2 = uuid.uuid4(), uuid.uuid4()
    feature = {"Feature Delivery.product": 1.0}

    try:
        sink.client.insert(
            "repos",
            [
                [repo1, "repo-one", COMPUTED_AT, COMPUTED_AT, org],
                [repo2, "repo-two", COMPUTED_AT, COMPUTED_AT, org],
            ],
            column_names=_repo_cols(),
        )
        # Investments row stamped at the OLDER generation clock (skipped path).
        sink.client.insert(
            "work_unit_investments",
            [
                [
                    "wu-gen",
                    FROM_TS,
                    TO_TS,
                    None,
                    "fte_days",
                    100.0,
                    feature,
                    "{}",
                    GEN1_AT,
                    org,
                ]
            ],
            column_names=_wui_cols(),
        )
        sink.client.insert(
            "work_unit_repo_effort",
            [
                # GEN1: 40/60 split.
                ["wu-gen", repo1, "fte_days", 40.0, 0.4, "structural", GEN1_AT, org],
                ["wu-gen", repo2, "fte_days", 60.0, 0.6, "structural", GEN1_AT, org],
                # GEN2 (newer): repo2 dropped, all 100 on repo1.
                ["wu-gen", repo1, "fte_days", 100.0, 1.0, "structural", GEN2_AT, org],
            ],
            column_names=_wure_cols(),
        )

        by_repo = await _edges(sink, org)
        # Only the latest generation is fanned: repo1 = 100, repo2 gone.
        assert by_repo.get("repo-one") == pytest.approx(100.0), by_repo
        assert "repo-two" not in by_repo, by_repo
        # SUM INVARIANT survives the shrink: total == unit effort, NOT 160.
        assert sum(by_repo.values()) == pytest.approx(100.0), by_repo
    finally:
        _cleanup(sink, org)


@pytest.mark.asyncio
async def test_scope_filter_resolves_team_for_multi_repo_unit(sink):
    """CHAOS-2777 round 2 / HIGH 2: under a repo scope_filter the unit_team CTE
    must read the SAME repo-allocated source as the outer flow, so the filter
    applies to the fanned repo_id. A multi-repo unit with a NULL scalar repo but
    an in-scope allocated repo must resolve its team, not collapse to
    'unassigned' (which happened while unit_team still filtered scalar repo_id).
    """
    org = f"test-chaos-2777-scope-{uuid.uuid4()}"
    repo_in, repo_out = uuid.uuid4(), uuid.uuid4()
    feature = {"Feature Delivery.product": 1.0}
    evidence = '{"issues": ["ISSUE-SCOPE-1"]}'

    try:
        sink.client.insert(
            "repos",
            [
                [repo_in, "repo-in", COMPUTED_AT, COMPUTED_AT, org],
                [repo_out, "repo-out", COMPUTED_AT, COMPUTED_AT, org],
            ],
            column_names=_repo_cols(),
        )
        sink.client.insert(
            "work_unit_investments",
            [
                [
                    "wu-scope",
                    FROM_TS,
                    TO_TS,
                    None,
                    "fte_days",
                    100.0,
                    feature,
                    evidence,
                    COMPUTED_AT,
                    org,
                ]
            ],
            column_names=_wui_cols(),
        )
        sink.client.insert(
            "work_unit_repo_effort",
            [
                [
                    "wu-scope",
                    repo_in,
                    "fte_days",
                    70.0,
                    0.7,
                    "structural",
                    COMPUTED_AT,
                    org,
                ],
                [
                    "wu-scope",
                    repo_out,
                    "fte_days",
                    30.0,
                    0.3,
                    "structural",
                    COMPUTED_AT,
                    org,
                ],
            ],
            column_names=_wure_cols(),
        )
        # CHAOS-2833: the unit's issue is owned by a team via the primary
        # ClickHouse attribution row (work_item_cycle_times is no longer read
        # for Sankey/repo-team team resolution -- proving this fixture still
        # resolves 'Team Scope' pins the migration for the repo-scope-filter
        # interaction, not just the plain team join).
        sink.write_work_item_team_attributions(
            [
                WorkItemTeamAttributionRecord(
                    work_item_id="ISSUE-SCOPE-1",
                    provider="linear",
                    source="native_team",
                    is_primary=1,
                    confidence="high",
                    evidence="native_team_key=team-scope",
                    computed_at=COMPUTED_AT,
                    repo_id=repo_in,
                    team_id="team-scope",
                    team_name="Team Scope",
                    org_id=org,
                )
            ]
        )

        # Scope to the in-scope repo only (mirrors build_scope_filter_multi).
        rows = await _rows(
            sink,
            org,
            scope_filter=" AND repo_id IN %(scope_ids)s",
            scope_params={"scope_ids": [str(repo_in)]},
        )
        by_repo = {str(r["repo"]): r for r in rows}
        # Out-of-scope repo is filtered out; only the in-scope allocated repo shows.
        assert "repo-out" not in by_repo, by_repo
        assert "repo-in" in by_repo, by_repo
        assert float(by_repo["repo-in"]["value"]) == pytest.approx(70.0)
        # The team resolves through the fanned unit_team -- NOT 'unassigned'.
        assert by_repo["repo-in"]["team"] == "Team Scope", by_repo
    finally:
        _cleanup(sink, org)
