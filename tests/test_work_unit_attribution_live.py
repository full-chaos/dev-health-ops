"""Live-ClickHouse regression for work-UNIT team attribution (CHAOS-2600 CS7).

Proves the cross-id-space collapse the per-unit resolver does in SQL:
``work_unit_membership.node_id`` (provider key) JOIN
``work_item_team_attributions.work_item_id`` → one owning team per unit by source
precedence. Uses DISTINCT id shapes (unit = 64-char hash, member = provider key)
so the bug that shipped green — passing a ``work_unit_id`` to a ``work_item_id``-
keyed query — is reproduced and pinned, not hidden by a matching-id mock.

Opt-in (filtered from unit/CI): ``pytest -m clickhouse`` with ``CLICKHOUSE_URI``
pointing at a SCRATCH db (never the dev ``default``).
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/ci_local_validate)",
    ),
]

# Unit ids are 64-char content hashes; member ids are provider keys. Keeping them
# structurally distinct is the whole point of this regression.
_UNIT_SOURCE_WINS = "a" * 64  # stronger source must beat a higher member count
_UNIT_COUNT_TIEBREAK = "b" * 64  # same source → most member items wins
_DAY = datetime(2026, 6, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    s = ClickHouseMetricsSink(CLICKHOUSE_URI)
    s.ensure_schema(force=True)
    yield s
    s.close()


def _membership(org_id, unit_id, node_id, run_id, *, node_type="issue"):
    from dev_health_ops.metrics.schemas import WorkUnitMembershipRecord

    return WorkUnitMembershipRecord(
        org_id=org_id,
        node_type=node_type,
        node_id=node_id,
        work_unit_id=unit_id,
        category_kind="theme",
        category="feature_delivery",
        weight=1.0,
        is_dominant=1,
        categorization_status="complete",
        computed_at=_DAY,
        run_id=run_id,
    )


def _attr(org_id, repo_id, work_item_id, team_id, team_name, source):
    from dev_health_ops.metrics.schemas import WorkItemTeamAttributionRecord

    return WorkItemTeamAttributionRecord(
        work_item_id=work_item_id,
        provider="linear",
        source=source,
        is_primary=1,
        confidence="high",
        evidence=f"{source}={team_id}",
        computed_at=_DAY,
        repo_id=repo_id,
        team_id=team_id,
        team_name=team_name,
        org_id=org_id,
    )


@pytest.mark.asyncio
async def test_unit_attribution_collapses_members_by_precedence(sink):
    from dev_health_ops.api.graphql.context import GraphQLContext
    from dev_health_ops.api.graphql.models.outputs import TeamAttributionSource
    from dev_health_ops.api.graphql.resolvers.team_attribution import (
        resolve_work_item_team_attributions,
        resolve_work_unit_team_attributions,
    )
    from dev_health_ops.metrics.schemas import WorkUnitMembershipRunRecord

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    org_id = f"test-cs7-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    run_id = "run-complete"

    # Unit A — strongest source wins over member count:
    #   CHAOS via native_team (rank 0, 1 member) beats PLATFORM via
    #   assignee_membership (rank 4, 2 members). One member is a cross-provider
    #   extkey: node with NO attribution row (must be ignored, not crash).
    # Unit B — same source, most members wins: PLATFORM (2) over CHAOS (1).
    sink.write_work_unit_memberships(
        [
            _membership(org_id, _UNIT_SOURCE_WINS, "linear:CHAOS-10", run_id),
            _membership(org_id, _UNIT_SOURCE_WINS, "linear:CHAOS-11", run_id),
            _membership(org_id, _UNIT_SOURCE_WINS, "linear:CHAOS-12", run_id),
            _membership(org_id, _UNIT_SOURCE_WINS, "extkey:ACTIONS-3", run_id),
            _membership(org_id, _UNIT_COUNT_TIEBREAK, "linear:CHAOS-20", run_id),
            _membership(org_id, _UNIT_COUNT_TIEBREAK, "linear:CHAOS-21", run_id),
            _membership(org_id, _UNIT_COUNT_TIEBREAK, "linear:CHAOS-22", run_id),
            # Incomplete run (no completion marker) — must be invisible. If the run
            # guard regressed, this row would inject team GHOST into unit A.
            _membership(org_id, _UNIT_SOURCE_WINS, "linear:CHAOS-99", "run-incomplete"),
        ]
    )
    sink.write_membership_run(
        WorkUnitMembershipRunRecord(org_id=org_id, run_id=run_id, completed_at=_DAY)
    )
    sink.write_work_item_team_attributions(
        [
            _attr(
                org_id, repo_id, "linear:CHAOS-10", "CHAOS", "Fullchaos", "native_team"
            ),
            _attr(
                org_id,
                repo_id,
                "linear:CHAOS-11",
                "PLATFORM",
                "Platform",
                "assignee_membership",
            ),
            _attr(
                org_id,
                repo_id,
                "linear:CHAOS-12",
                "PLATFORM",
                "Platform",
                "assignee_membership",
            ),
            _attr(
                org_id, repo_id, "linear:CHAOS-20", "CHAOS", "Fullchaos", "native_team"
            ),
            _attr(
                org_id,
                repo_id,
                "linear:CHAOS-21",
                "PLATFORM",
                "Platform",
                "native_team",
            ),
            _attr(
                org_id,
                repo_id,
                "linear:CHAOS-22",
                "PLATFORM",
                "Platform",
                "native_team",
            ),
            _attr(org_id, repo_id, "linear:CHAOS-99", "GHOST", "Ghost", "native_team"),
        ]
    )

    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client)
    result = await resolve_work_unit_team_attributions(
        context, work_unit_ids=[_UNIT_SOURCE_WINS, _UNIT_COUNT_TIEBREAK]
    )
    by_unit = {r.work_unit_id: r for r in result}

    # Unit A: stronger source (native_team) wins despite fewer members.
    a = by_unit[_UNIT_SOURCE_WINS]
    assert a.team_id == "CHAOS"
    assert a.source == TeamAttributionSource.NATIVE_TEAM
    assert a.member_count == 1  # only CHAOS-10 backs CHAOS
    assert a.is_primary is True
    # Incomplete-run row never leaked in.
    assert a.team_id != "GHOST"

    # Unit B: same source for both teams → most member items wins.
    b = by_unit[_UNIT_COUNT_TIEBREAK]
    assert b.team_id == "PLATFORM"
    assert b.member_count == 2

    # PIN THE BUG (CHAOS-2608): the original code passed the unit's 64-char hash to
    # the work_item-keyed resolver. That id is NOT a work_item_id, so it resolves
    # NOTHING — which is exactly why the badge never rendered. The unit resolver
    # above is the fix.
    wrong = await resolve_work_item_team_attributions(
        context, work_item_ids=[_UNIT_SOURCE_WINS]
    )
    assert wrong == []


@pytest.mark.asyncio
async def test_team_filter_selects_units_for_that_team(sink):
    from dev_health_ops.api.graphql.context import GraphQLContext
    from dev_health_ops.api.graphql.resolvers.team_attribution import (
        resolve_work_unit_team_attributions,
    )

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    org_id = f"test-cs7-filter-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    run_id = "run-complete"
    unit_chaos = "c" * 64
    unit_platform = "d" * 64

    from dev_health_ops.metrics.schemas import WorkUnitMembershipRunRecord

    sink.write_work_unit_memberships(
        [
            _membership(org_id, unit_chaos, "linear:CHAOS-30", run_id),
            _membership(org_id, unit_platform, "linear:CHAOS-31", run_id),
        ]
    )
    sink.write_membership_run(
        WorkUnitMembershipRunRecord(org_id=org_id, run_id=run_id, completed_at=_DAY)
    )
    sink.write_work_item_team_attributions(
        [
            _attr(
                org_id, repo_id, "linear:CHAOS-30", "CHAOS", "Fullchaos", "native_team"
            ),
            _attr(
                org_id,
                repo_id,
                "linear:CHAOS-31",
                "PLATFORM",
                "Platform",
                "native_team",
            ),
        ]
    )

    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client)
    only_chaos = await resolve_work_unit_team_attributions(context, team_id="CHAOS")
    assert {r.work_unit_id for r in only_chaos} == {unit_chaos}
    assert all(r.team_id == "CHAOS" for r in only_chaos)


@pytest.mark.asyncio
async def test_legacy_marker_keeps_migrated_membership_readable(sink):
    """Migrated tenants (CHAOS-2608 codex HIGH): pre-migration membership rows carry
    run_id='' and migration 048 seeds a '__legacy__' marker. The resolver must map
    the legacy marker to each node's latest run_id='' row — a plain
    run_id=argMax(...) would filter to run_id='__legacy__' and match NOTHING,
    silently dropping the unit's badge.
    """
    from dev_health_ops.api.graphql.context import GraphQLContext
    from dev_health_ops.api.graphql.resolvers.team_attribution import (
        resolve_work_unit_team_attributions,
    )
    from dev_health_ops.metrics.schemas import WorkUnitMembershipRunRecord

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    org_id = f"test-cs7-legacy-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    unit_legacy = "e" * 64

    # Pre-migration rows: run_id='' (the default migration 047 backfilled).
    sink.write_work_unit_memberships(
        [
            _membership(org_id, unit_legacy, "linear:CHAOS-40", ""),
            _membership(org_id, unit_legacy, "linear:CHAOS-41", ""),
        ]
    )
    # Migration 048's seeded legacy marker: run_id='__legacy__'.
    sink.write_membership_run(
        WorkUnitMembershipRunRecord(
            org_id=org_id, run_id="__legacy__", completed_at=_DAY
        )
    )
    sink.write_work_item_team_attributions(
        [
            _attr(
                org_id, repo_id, "linear:CHAOS-40", "CHAOS", "Fullchaos", "native_team"
            ),
            _attr(
                org_id, repo_id, "linear:CHAOS-41", "CHAOS", "Fullchaos", "native_team"
            ),
        ]
    )

    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client)
    result = await resolve_work_unit_team_attributions(
        context, work_unit_ids=[unit_legacy]
    )
    assert len(result) == 1
    assert result[0].work_unit_id == unit_legacy
    assert result[0].team_id == "CHAOS"
    assert result[0].member_count == 2


@pytest.mark.asyncio
async def test_no_complete_run_resolves_to_no_membership(sink):
    """No completion marker (in-flight or pre-048 with no real run) → the org has no
    complete run, so the empty-string guard must yield NOTHING rather than
    over-matching empty-run_id rows.
    """
    from dev_health_ops.api.graphql.context import GraphQLContext
    from dev_health_ops.api.graphql.resolvers.team_attribution import (
        resolve_work_unit_team_attributions,
    )

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    org_id = f"test-cs7-norun-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    unit_orphan = "f" * 64

    # Membership rows exist (run_id='') but NO marker was ever written.
    sink.write_work_unit_memberships(
        [_membership(org_id, unit_orphan, "linear:CHAOS-50", "")]
    )
    sink.write_work_item_team_attributions(
        [_attr(org_id, repo_id, "linear:CHAOS-50", "CHAOS", "Fullchaos", "native_team")]
    )

    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client)
    result = await resolve_work_unit_team_attributions(context)
    assert result == []
