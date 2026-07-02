"""Live-ClickHouse integration for CHAOS-2776: the no-LLM membership projection
re-arms the read-path stale-generation guard.

Reproduces the CHAOS-2776 incident end-to-end against a real ClickHouse:

  1. An OLD org-wide membership marker exists (completed at ``old_ts``).
  2. A NEWER ``work_unit_investments`` row (computed at ``mid_ts`` > ``old_ts``)
     makes the read-path scope state fall back to ``unscoped_fallback`` —
     investments are newer than the latest marker, so scoped reads disarm and
     stale work-unit generations flood the Investment charts (the ~18x effort
     inflation observed in the incident).
  3. Running ``backfill_memberships`` (the full-coverage projection the finalizer
     now runs after a WINDOWED org-wide post-sync materialize — the fix) publishes
     a FRESH org-wide marker stamped with the COMPLETION time (``now()`` >>
     ``mid_ts``), so the scope state returns to ``scoped``.

The projection's coverage is independent of any materialize window, which is why
running it after a windowed org-wide materialize is safe and correct — the exact
property the CHAOS-2776 gating change relies on.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

from dev_health_ops.api.queries.investment_membership_scope import (
    fetch_investment_membership_scope_state,
)
from dev_health_ops.work_graph.investment.backfill import (
    MembershipBackfillConfig,
    backfill_memberships,
)

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


def _edge_cols() -> list[str]:
    return [
        "org_id",
        "edge_id",
        "source_type",
        "source_id",
        "target_type",
        "target_id",
        "edge_type",
        "repo_id",
        "provider",
        "provenance",
        "confidence",
        "evidence",
        "discovered_at",
        "last_synced",
        "event_ts",
        "day",
    ]


def _membership_run_cols() -> list[str]:
    return ["org_id", "run_id", "completed_at"]


def _investment_cols() -> list[str]:
    return ["org_id", "work_unit_id", "computed_at"]


@pytest.mark.asyncio
async def test_projection_after_windowed_materialize_restores_scoped_state(sink):
    """After the projection runs, the scope state flips unscoped_fallback ->
    scoped (CHAOS-2776)."""
    org_id = f"test-chaos-2776-{uuid.uuid4()}"
    node_id = f"ISSUE-{uuid.uuid4()}"
    pr_id = f"PR-{uuid.uuid4()}"
    edge_id = f"edge-{uuid.uuid4()}"
    repo_uuid = str(uuid.uuid4())

    old_marker_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    # Investment newer than the OLD marker -> read path falls back to unscoped.
    investment_ts = datetime(2026, 2, 1, tzinfo=timezone.utc)
    old_run_id = uuid.uuid4().hex

    # One edge => one current work-graph component => the projection has something
    # to iterate and will publish an org-wide completion marker.
    edge_rows = [
        [
            org_id,
            edge_id,
            "issue",
            node_id,
            "pr",
            pr_id,
            "implements",
            repo_uuid,
            "github",
            "native",
            1.0,
            "",
            investment_ts,
            investment_ts,
            investment_ts,
            investment_ts.date(),
        ]
    ]
    # A prior COMPLETE org-wide marker at old_marker_ts.
    run_marker_rows = [[org_id, old_run_id, old_marker_ts]]
    # A newer investments row -> latest_investment_computed_at > latest marker.
    # Only org_id + computed_at feed the scope-state clock; the rest default.
    investment_rows = [[org_id, f"U-{uuid.uuid4()}", investment_ts]]

    sink.client.insert("work_graph_edges", edge_rows, column_names=_edge_cols())
    sink.client.insert(
        "work_unit_membership_runs",
        run_marker_rows,
        column_names=_membership_run_cols(),
    )
    sink.client.insert(
        "work_unit_investments", investment_rows, column_names=_investment_cols()
    )

    # BEFORE the projection: investments are newer than the marker -> fallback.
    before = await fetch_investment_membership_scope_state(sink, org_id=org_id)
    assert before.scope_mode == "unscoped_fallback"
    assert before.lag_seconds > 0

    # Run the real no-LLM projection (org-wide: repo_ids=None). It publishes a
    # fresh marker stamped at COMPLETION time (now() >> investment_ts).
    assert CLICKHOUSE_URI is not None  # narrowed by pytestmark.skipif
    backfill_memberships(
        MembershipBackfillConfig(dsn=CLICKHOUSE_URI, org_id=org_id, repo_ids=None)
    )

    # AFTER the projection: the newest marker is >= the investment clock -> scoped.
    after = await fetch_investment_membership_scope_state(sink, org_id=org_id)
    assert after.scope_mode == "scoped"
