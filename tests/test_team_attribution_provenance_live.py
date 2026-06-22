"""Live-ClickHouse regression for team-attribution provenance staleness (CHAOS-2605).

A re-org appends a NEW candidate set (new ``computed_at``) without deleting prior
candidates, and the RMT key includes ``team_id``/``source`` so FINAL cannot retire
the old ``(team_id, source)`` rows. The provenance resolver must therefore return
only each work item's LATEST compute snapshot — never a stale second
``is_primary`` row or a higher-precedence source that no longer applies.

Opt-in (filtered from unit/CI): ``pytest -m clickhouse`` with ``CLICKHOUSE_URI``
pointing at a SCRATCH db (never the dev ``default``).
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/ci_local_validate)",
    ),
]


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    s = ClickHouseMetricsSink(CLICKHOUSE_URI)
    s.ensure_schema(force=True)
    yield s
    s.close()


@pytest.mark.asyncio
async def test_latest_compute_snapshot_drops_stale_candidates(sink):
    from dev_health_ops.api.graphql.context import GraphQLContext
    from dev_health_ops.api.graphql.models.outputs import TeamAttributionSource
    from dev_health_ops.api.graphql.resolvers.team_attribution import (
        resolve_work_item_team_attributions,
    )
    from dev_health_ops.metrics.schemas import WorkItemTeamAttributionRecord

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    org_id = f"test-2605-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    wid = "linear:CHAOS-REORG-1"
    day1 = datetime(2026, 6, 1, tzinfo=timezone.utc)
    day2 = day1 + timedelta(days=1)

    # Day 1: native_team -> team-old (primary) + an assignee side candidate.
    # Day 2 (re-org): native_team -> team-new (primary). The day-1 rows linger in
    # the RMT because their (team_id, source) key differs from the day-2 row.
    sink.write_work_item_team_attributions(
        [
            WorkItemTeamAttributionRecord(
                work_item_id=wid,
                provider="linear",
                source="native_team",
                is_primary=1,
                confidence="high",
                evidence="native_team_key=OLD",
                computed_at=day1,
                repo_id=repo_id,
                team_id="team-old",
                team_name="Old",
                org_id=org_id,
            ),
            WorkItemTeamAttributionRecord(
                work_item_id=wid,
                provider="linear",
                source="assignee_membership",
                is_primary=0,
                confidence="medium",
                evidence="assignee=x",
                computed_at=day1,
                repo_id=repo_id,
                team_id="team-side",
                team_name="Side",
                org_id=org_id,
            ),
            WorkItemTeamAttributionRecord(
                work_item_id=wid,
                provider="linear",
                source="native_team",
                is_primary=1,
                confidence="high",
                evidence="native_team_key=NEW",
                computed_at=day2,
                repo_id=repo_id,
                team_id="team-new",
                team_name="New",
                org_id=org_id,
            ),
        ]
    )

    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client)
    result = await resolve_work_item_team_attributions(context, work_item_ids=[wid])

    # Only the day-2 snapshot survives: exactly one candidate, the new team, one
    # primary — the stale day-1 team-old / team-side rows are gone.
    assert [r.team_id for r in result] == ["team-new"]
    assert [r.is_primary for r in result].count(True) == 1
    assert result[0].source == TeamAttributionSource.NATIVE_TEAM
    assert all(r.team_id != "team-old" for r in result)
