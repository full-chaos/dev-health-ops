"""Live-ClickHouse proof for CHAOS-2777.

PR #1106 added ``work_unit_repo_effort`` (per-(work_unit, repo) effort fan-out)
so multi-repo work units -- whose scalar ``work_unit_investments.repo_id`` is
NULL -- can map effort to their real repos. The Allocation-tab Sankey fetchers
in ``api/queries/investment.py`` originally read only the scalar ``repo_id``, so
ANY multi-repo unit collapsed onto 'Unassigned repo'. This test seeds a
multi-repo unit (NULL scalar repo, 3 allocation rows), a single-repo unit (scalar
repo, no allocation rows -> scalar fallback) and a genuinely repo-less unit
(NULL scalar, no allocation) and asserts, against REAL ClickHouse:

* the per-repo effort split (multi-repo unit fans out across its 3 repos),
* the SUM INVARIANT -- each unit contributes exactly its original effort, so the
  grand total is unchanged versus the scalar path,
* the scalar fallback (a unit with no allocation row keeps its scalar repo +
  full effort and is never dropped),
* ``missing_repo`` counts only units with a NULL scalar repo AND no allocation,
* theme filtering still interacts correctly with the fan-out.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

import dev_health_ops.api.queries.investment as investment_queries
from dev_health_ops.metrics.sinks.base import BaseMetricsSink

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
    ),
]

FROM_TS = datetime(2026, 1, 5, tzinfo=timezone.utc)
TO_TS = datetime(2026, 1, 6, tzinfo=timezone.utc)
COMPUTED_AT = datetime(2026, 1, 7, tzinfo=timezone.utc)
# Query window strictly containing [FROM_TS, TO_TS).
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 2, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
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


async def _edges(sink: BaseMetricsSink, org_id: str, **kwargs) -> dict[str, float]:
    rows = await investment_queries.fetch_investment_team_subcategory_repo_edges(
        sink,
        start_ts=START,
        end_ts=END,
        scope_filter="",
        scope_params={},
        org_id=org_id,
        **kwargs,
    )
    by_repo: dict[str, float] = {}
    for row in rows:
        # Mirror build_investment_flow_response: an empty repo label (a NULL /
        # unmatched repo_id) is surfaced as the 'unassigned' repo bucket.
        label = str(row["repo"]) or "unassigned"
        by_repo[label] = by_repo.get(label, 0.0) + float(row["value"])
    return by_repo


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
        for table in ("work_unit_investments", "work_unit_repo_effort", "repos"):
            sink.client.command(
                f"ALTER TABLE {table} DELETE WHERE org_id = {{o:String}} "
                "SETTINGS mutations_sync=2",
                parameters={"o": org},
            )
