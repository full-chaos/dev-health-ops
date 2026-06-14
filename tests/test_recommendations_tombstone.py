"""Tombstone / resolution regression tests for recommendations (CHAOS-2373).

The scheduled recommendations job previously persisted *only* fired
recommendations. The GraphQL readers ``argMax(fired, computed_at)`` per
``(org_id, team_id, rule_id, window_end)`` and keep ``HAVING latest_fired =
true``. Because ``window_end`` is part of the read key and each run writes
``window_end = today``, a rule that fired yesterday but no longer fires today
left a stale ``fired=true`` row at yesterday's ``window_end`` that kept
surfacing resolved guidance until it aged out.

Two layers of fix are exercised here:

* ``RuleEngine.evaluate_state`` returns the *full* rule state — fired rows AND
  explicit ``fired=False`` tombstones — so a recovered signal is written, not
  silently omitted (pure unit test, no DB).
* The reader SQL collapses to the *latest* ``window_end`` per
  ``(org, team, rule)`` so a newer no-fire as-of supersedes an older fired row
  (live-ClickHouse test, opt-in via ``pytest -m clickhouse``).
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone

import pytest

# Import connectors first to break the providers._base <-> connectors circular
# import that otherwise ERRORs collection of isolated processor/engine imports
# (matches the CHAOS-2370 guard).
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.recommendations.snapshot import (
    MetricsSnapshot,
    RecommendationRecord,
)

# ---------------------------------------------------------------------------
# Unit: evaluate_state writes a tombstone for every non-firing rule
# ---------------------------------------------------------------------------


class _StubLoader:
    """MetricsLoader stub returning an all-``None``/empty snapshot (no rule fires)."""

    def __init__(self, snapshot: MetricsSnapshot) -> None:
        self._snapshot = snapshot

    def load_team_metrics_window(
        self,
        team_id: str,
        org_id: str,
        window_start: date,
        window_end: date,
    ) -> MetricsSnapshot:
        return self._snapshot


def _empty_snapshot(team_id: str, org_id: str, ws: date, we: date) -> MetricsSnapshot:
    return MetricsSnapshot(
        team_id=team_id,
        org_id=org_id,
        window_start=ws,
        window_end=we,
        wip_by_day=[],
        throughput_by_cycle=[],
        review_latency_p75_hours=None,
        reviewer_gini=None,
        rework_churn_ratio=None,
        after_hours_ratio=None,
        cycle_time_by_day=[],
        hotspot_complexity_delta=None,
        hotspot_churn_overlap=None,
    )


def test_evaluate_state_emits_tombstone_for_every_rule_when_none_fire():
    """No rule fires -> one fired=False record per registered rule (no gaps)."""
    from dev_health_ops.recommendations import registry
    from dev_health_ops.recommendations.engine import RuleEngine
    from dev_health_ops.recommendations.rules import RULE_EVALUATORS

    now = datetime(2026, 4, 8, 2, 0, 0, tzinfo=timezone.utc)
    ws = date(2026, 3, 25)
    snapshot = _empty_snapshot("team-1", "org-1", ws, now.date())
    engine = RuleEngine(registry=registry, loader=_StubLoader(snapshot), now=now)

    records = engine.evaluate_state(team_id="team-1", window=14, org_id="org-1")

    # Exactly one record per registered rule, all non-fired tombstones.
    assert {r.rule_id for r in records} == set(RULE_EVALUATORS)
    assert len(records) == len(RULE_EVALUATORS)
    assert all(r.fired is False for r in records)
    # Tombstones share the run's computed_at + window_end (one consistent batch).
    assert all(r.computed_at == now for r in records)
    assert all(r.window_end == now.date() for r in records)
    # Registry metadata is carried so the row is self-describing.
    sat = next(r for r in records if r.rule_id == "saturation")
    assert sat.title == registry.get_rule("saturation").title
    assert sat.evidence_json == "[]"


def test_evaluate_state_mixes_fired_and_tombstones():
    """A firing rule yields fired=True; the rest yield fired=False, no gaps."""
    from dev_health_ops.recommendations import registry
    from dev_health_ops.recommendations.engine import RuleEngine
    from dev_health_ops.recommendations.rules import RULE_EVALUATORS
    from dev_health_ops.recommendations.schema import Recommendation

    now = datetime(2026, 4, 8, 2, 0, 0, tzinfo=timezone.utc)
    ws = date(2026, 3, 25)
    snapshot = _empty_snapshot("team-1", "org-1", ws, now.date())

    fired_rec = Recommendation(
        rule_id="saturation",
        team_id="team-1",
        org_id="org-1",
        computed_at=now,
        window_start=ws,
        window_end=now.date(),
        severity="warning",
        title="WIP saturation rising",
        rationale="WIP up.",
        success_criterion="WIP < 3",
        evidence=(),
    )
    # Force only 'saturation' to fire; all other evaluators return None.
    evaluators = {
        rid: (lambda s, n: fired_rec) if rid == "saturation" else (lambda s, n: None)
        for rid in RULE_EVALUATORS
    }
    engine = RuleEngine(
        registry=registry, loader=_StubLoader(snapshot), now=now, evaluators=evaluators
    )

    records = engine.evaluate_state(team_id="team-1", window=14, org_id="org-1")

    by_rule = {r.rule_id: r for r in records}
    assert set(by_rule) == set(RULE_EVALUATORS)
    assert by_rule["saturation"].fired is True
    assert by_rule["saturation"].title == "WIP saturation rising"
    assert all(r.fired is False for rid, r in by_rule.items() if rid != "saturation")


# ---------------------------------------------------------------------------
# Live ClickHouse: a newer no-fire as-of supersedes an older fired row
# ---------------------------------------------------------------------------

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

_live = pytest.mark.skipif(
    not CLICKHOUSE_URI,
    reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
)


def _record(
    *,
    team_id: str,
    org_id: str,
    rule_id: str,
    window_end: date,
    fired: bool,
    computed_at: datetime,
) -> RecommendationRecord:
    return RecommendationRecord(
        team_id=team_id,
        org_id=org_id,
        rule_id=rule_id,
        rule_version="1.0.0",
        window_start=window_end,
        window_end=window_end,
        fired=fired,
        severity="warning",
        title="Saturation" if fired else "",
        rationale="fired" if fired else "",
        success_criterion="resolve",
        evidence_json="[]",
        computed_at=computed_at,
    )


@pytest.mark.clickhouse
@_live
def test_newer_nofire_supersedes_older_fired_row():
    """A fired row on day1 is cleared once day2 writes a fired=False tombstone.

    Drives the *actual* resolver SQL against live ClickHouse — no mocking of
    the read path — so the two-stage (latest-window_end) collapse is proven.
    """
    from dev_health_ops.api.graphql.resolvers.recommendations import (
        _RECOMMENDATIONS_SQL,
    )
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    org_id = f"test-chaos-2373-{uuid.uuid4()}"
    team_id = "team-tomb"
    rule_id = "saturation"
    day1 = date(2026, 4, 7)
    day2 = date(2026, 4, 8)

    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)
    try:
        # Day 1: rule fired (older as-of).
        sink.write_recommendations(
            [
                _record(
                    team_id=team_id,
                    org_id=org_id,
                    rule_id=rule_id,
                    window_end=day1,
                    fired=True,
                    computed_at=datetime(2026, 4, 7, 2, 0, tzinfo=timezone.utc),
                )
            ]
        )

        def _fired_rules(window_end: date) -> set[str]:
            res = sink.client.query(
                _RECOMMENDATIONS_SQL,
                parameters={
                    "team_id": team_id,
                    "org_id": org_id,
                    "window_start": day1.isoformat(),
                    "window_end": window_end.isoformat(),
                },
            )
            return {row[2] for row in (res.result_rows or [])}  # rule_id col

        # After day 1 the rule is visible (positive control).
        assert _fired_rules(day1) == {rule_id}

        # Day 2: full-state write -> rule no longer fires (tombstone, newer as-of).
        sink.write_recommendations(
            [
                _record(
                    team_id=team_id,
                    org_id=org_id,
                    rule_id=rule_id,
                    window_end=day2,
                    fired=False,
                    computed_at=datetime(2026, 4, 8, 2, 0, tzinfo=timezone.utc),
                )
            ]
        )

        # The stale day-1 fired row must be superseded by the day-2 tombstone:
        # the reader collapses to the latest window_end per (org, team, rule).
        assert _fired_rules(day2) == set(), (
            "recovered signal still visible: latest-window_end collapse failed"
        )
    finally:
        sink.client.command(
            "ALTER TABLE recommendations_daily DELETE WHERE org_id = {o:String} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )
        sink.close()
