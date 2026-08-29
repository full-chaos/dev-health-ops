"""Real ClickHouse proof that ``hotspots`` row order is deterministic at a
``risk_score`` tie (CHAOS-4472 / CHAOS-4369).

Plants several rows that share the same ``risk_score`` (a genuine tie on
the resolver's primary sort key) with a small ``limit`` sitting exactly at
the tie boundary, and asserts the returned row order -- and row SET, since a
tie at the LIMIT boundary can otherwise drop a different subset of tied rows
each run -- is byte-for-byte identical across repeated calls against the
same real ClickHouse server.

Without the CHAOS-4472 fix (``ORDER BY risk_score DESC NULLS LAST`` with no
tie-break), ClickHouse does not guarantee a stable order among rows with
equal ``risk_score``: parallel block processing has no ordering guarantee
beyond the declared sort key, so a repeated identical query can legitimately
return a different row order/set. This test proves the FIX (the resolver's
ORDER BY now carries the ``repo_id, file_path`` tie-break, which is a total
order over the deduplicated row set) makes the result the same on every
call, not merely "usually the same". Same shape as CHAOS-4421's
``test_review_edges_tie_order_live.py``.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.complexity import resolve_hotspots
from dev_health_ops.api.graphql.types.complexity import HotspotsInput
from dev_health_ops.fixtures.world import _require_scratch_database
from dev_health_ops.metrics.schemas import FileHotspotDaily
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at an isolated scratch database",
    ),
]


@pytest.mark.asyncio
async def test_tied_rows_return_stable_order_and_set_across_repeated_calls() -> None:
    assert CLICKHOUSE_URI is not None
    # codex review precedent (CHAOS-4421, P1): reject a shared/default
    # database BEFORE ever constructing the sink or calling
    # ensure_schema/insert/delete -- this test issues destructive-capable
    # ClickHouse calls (schema DDL, insert, ALTER ... DELETE), which must
    # never reach the real dev `default` database (AGENTS.md's ops pre-push
    # gate safety rule).
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4472-tie-order-{uuid.uuid4()}"
    day = date(2026, 8, 15)
    computed_at = datetime(2026, 8, 16, 0, 0, 0, tzinfo=timezone.utc)

    # Five distinct (repo_id, file_path) rows, all sharing risk_score=42.0
    # -- a genuine tie on the primary sort key. limit=3 sits exactly at the
    # tie boundary, so which 3 of the 5 tied rows survive the LIMIT (and in
    # what order) is exactly what an absent tie-break leaves undefined.
    # file_path is held constant so the tie-break is exercised purely
    # through repo_id ordering.
    repo_ids = [uuid.uuid4() for _ in range(5)]
    hotspots = [
        FileHotspotDaily(
            repo_id=repo_id,
            day=day,
            file_path="src/main.go",
            churn_loc_30d=100,
            churn_commits_30d=5,
            cyclomatic_total=10,
            cyclomatic_avg=2.0,
            blame_concentration=0.5,
            risk_score=42.0,
            computed_at=computed_at,
            org_id=org_id,
        )
        for repo_id in repo_ids
    ]

    try:
        sink.write_file_hotspot_daily(hotspots)

        async def _fetch_order() -> list[str]:
            ctx = GraphQLContext(
                org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client
            )
            result = await resolve_hotspots(
                ctx,
                HotspotsInput(
                    org_id=org_id,
                    since_utc=datetime(2026, 8, 15, 0, 0, 0, tzinfo=timezone.utc),
                    until_utc=datetime(2026, 8, 15, 23, 59, 59, tzinfo=timezone.utc),
                    repo_ids=None,
                    team_ids=None,
                    limit=3,
                ),
            )
            assert len(result.rows) == 3, (
                f"expected exactly 3 of the 5 tied rows under limit=3, got "
                f"{len(result.rows)}"
            )
            return [row.repo_id for row in result.rows]

        # Repeat the identical query several times. With the CHAOS-4472
        # fix, the tie-break makes both the row SET (which 3 of 5 survive
        # LIMIT) and the row ORDER identical every time -- the
        # deterministic tie-break sorts by repo_id ascending among equal
        # risk_score, so the 3 lexicographically-smallest repo_ids must win
        # every run.
        orders = [await _fetch_order() for _ in range(8)]

        expected = sorted(str(r) for r in repo_ids)[:3]
        for i, order in enumerate(orders):
            assert order == expected, (
                f"run {i}: tied-row order/set was not deterministic: "
                f"got {order}, expected {expected} (CHAOS-4472 regression -- "
                f"ORDER BY risk_score DESC NULLS LAST alone does not "
                f"guarantee a stable result among tied rows)"
            )
    finally:
        sink.client.command(
            "ALTER TABLE file_hotspot_daily DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()
