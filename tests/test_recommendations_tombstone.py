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
from datetime import date, datetime, timedelta, timezone

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


@pytest.mark.clickhouse
@_live
def test_writer_window_end_visible_same_day_via_resolver_cap(monkeypatch):
    """Writer's ``window_end == today + 1`` is read same-day by recommendations().

    Regression for the CONVENTION MISMATCH (CHAOS-2373): the scheduled writer
    anchors ``now = as_of_day + 1`` and persists rows at ``window_end ==
    today + 1`` (so the loader's exclusive ``day < window_end`` still reads the
    just-finalized ``today`` partition). The GraphQL ``recommendations()``
    resolver previously capped its read at ``window_end <= today`` and so
    EXCLUDED those freshest rows — the surface was one finalize-day stale.

    This drives the ACTUAL resolver bounds (``_window_to_dates``, patched so
    "today" is deterministic) against live ClickHouse and asserts:

    * a rule that fired this finalize cycle (``window_end = today + 1``) APPEARS
      same-day, and
    * a recovered rule (fired yesterday, tombstoned this cycle) is CLEARED
      same-day — not lingering for an extra finalize-day.
    """
    from dev_health_ops.api.graphql.models.recommendations import (
        WindowInput,
        WindowUnit,
    )
    from dev_health_ops.api.graphql.resolvers import recommendations as resolver_mod
    from dev_health_ops.api.graphql.resolvers.recommendations import (
        _RECOMMENDATIONS_SQL,
        _window_to_dates,
    )
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None

    # Deterministic "today" so the writer's today+1 and the resolver cap line up.
    today = date(2026, 4, 8)
    # Writer convention: window_end == as_of_day + 1 (as_of_day == today).
    writer_window_end = today + timedelta(days=1)
    assert writer_window_end == date(2026, 4, 9)  # today + 1

    monkeypatch.setattr(resolver_mod, "utc_today", lambda: today)

    # Sanity: the resolver now caps the read at today + 1 (inclusive), matching
    # the writer's persisted key. If this regresses to `today`, the freshest
    # finalized rows fall outside the cap and the asserts below fail.
    window = WindowInput(value=2, unit=WindowUnit.WEEK)
    res_start, res_end = _window_to_dates(window)
    assert res_end == writer_window_end, (
        "resolver read cap must include the writer's today+1 window_end"
    )

    org_id = f"test-chaos-2373-cap-{uuid.uuid4()}"
    team_id = "team-cap"
    fired_rule = "saturation"
    recovered_rule = "thrash"
    computed_at = datetime(2026, 4, 9, 2, 0, tzinfo=timezone.utc)

    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)
    try:
        # Yesterday's finalize: recovered_rule fired at window_end = today
        # (yesterday's as_of + 1). The writer convention always bumps by one.
        sink.write_recommendations(
            [
                _record(
                    team_id=team_id,
                    org_id=org_id,
                    rule_id=recovered_rule,
                    window_end=today,  # yesterday's as_of + 1
                    fired=True,
                    computed_at=datetime(2026, 4, 8, 2, 0, tzinfo=timezone.utc),
                )
            ]
        )

        # Today's finalize (as_of = today): full state at window_end = today + 1.
        #   * fired_rule fires (new signal)
        #   * recovered_rule tombstoned (no longer fires)
        sink.write_recommendations(
            [
                _record(
                    team_id=team_id,
                    org_id=org_id,
                    rule_id=fired_rule,
                    window_end=writer_window_end,
                    fired=True,
                    computed_at=computed_at,
                ),
                _record(
                    team_id=team_id,
                    org_id=org_id,
                    rule_id=recovered_rule,
                    window_end=writer_window_end,
                    fired=False,
                    computed_at=computed_at,
                ),
            ]
        )

        # Read through the resolver's OWN derived bounds (the cap under test).
        res = sink.client.query(
            _RECOMMENDATIONS_SQL,
            parameters={
                "team_id": team_id,
                "org_id": org_id,
                "window_start": res_start.isoformat(),
                "window_end": res_end.isoformat(),
            },
        )
        fired_now = {row[2] for row in (res.result_rows or [])}  # rule_id col

        # Same-day visibility: today's finalized fired rule APPEARS immediately.
        assert fired_rule in fired_now, (
            "today's finalized recommendation excluded by the resolver read cap"
        )
        # Same-day clearance: the recovered rule is gone, not stale for a day.
        assert recovered_rule not in fired_now, (
            "recovered signal still visible same-day: read cap/convention mismatch"
        )
    finally:
        sink.client.command(
            "ALTER TABLE recommendations_daily DELETE WHERE org_id = {o:String} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )
        sink.close()


# ---------------------------------------------------------------------------
# CHAOS-2398: a same-as_of re-run clears a recovered rule because the worker
# re-stamps computed_at with the wall-clock write time (NOT the engine `now`).
# ---------------------------------------------------------------------------


@pytest.mark.clickhouse
@_live
def test_same_as_of_rerun_clears_recovered_rule_via_wallclock_restamp(monkeypatch):
    """Two worker runs for the SAME as_of day: run 1 fires, run 2 recovers.

    Approach (focused e2e through the real worker write path, not the read SQL
    of a hand-crafted row): we drive ``_compute_recommendations_for_org`` twice
    with an *identical* ``now``/``as_of_day`` so the engine derives an identical
    ``window_end`` AND an identical engine ``computed_at`` on both runs. The
    only thing that can differ between the two runs is the worker's wall-clock
    re-stamp (``write_ts = datetime.now(UTC)`` just before
    ``sink.write_recommendations``, CHAOS-2398).

    We force fire-vs-no-fire by injecting evaluators into the ``RuleEngine``
    (run 1: the target rule fires; run 2: nothing fires -> tombstone), and stub
    team discovery so no ``work_item_metrics_daily`` seeding is required. The
    real ``ClickHouseMetricsLoader`` still runs, but the injected evaluators
    ignore its snapshot.

    Asserts:

    * the two runs land DISTINCT ``computed_at`` at the SAME
      ``(org, team, rule, window_end)`` (proving the re-stamp is monotonic and
      not the constant engine ``now``), and
    * the resolver — whose inner stage does ``argMax(fired, computed_at)`` to
      collapse re-runs of the same ``window_end`` — shows the rule CLEARED after
      run 2. If both runs shared the engine ``computed_at``, the inner argMax
      tie could not deterministically pick the run-2 tombstone.
    """
    import time as _time

    from dev_health_ops.api.graphql.resolvers.recommendations import (
        _RECOMMENDATIONS_SQL,
    )
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
    from dev_health_ops.recommendations.engine import RuleEngine as _RealRuleEngine
    from dev_health_ops.recommendations.rules import RULE_EVALUATORS
    from dev_health_ops.recommendations.schema import Recommendation
    from dev_health_ops.workers import recommendations_tasks as tasks_mod

    assert CLICKHOUSE_URI is not None

    org_id = f"test-chaos-2398-{uuid.uuid4()}"
    team_id = f"team-{uuid.uuid4()}"
    fired_rule = "saturation"

    # Single finalized partition; BOTH runs evaluate this same as_of day, so the
    # engine `now` (and thus window_end + the engine-derived computed_at) is a
    # constant across runs. window_end == as_of_day + 1 by the worker convention.
    as_of_day = date(2026, 4, 8)
    now = datetime(2026, 4, 9, 0, 0, 0, tzinfo=timezone.utc)
    window_end = as_of_day + timedelta(days=1)
    assert window_end == now.date()

    # Stub team discovery so we don't depend on work_item_metrics_daily rows.
    monkeypatch.setattr(tasks_mod, "_discover_team_ids", lambda client, oid: [team_id])
    # Gate is a no-op for a synthetic org; force-ready to be explicit.
    monkeypatch.setattr(tasks_mod, "_daily_metrics_ready", lambda oid, day: True)

    def _make_fired_rec(computed_at: datetime) -> Recommendation:
        return Recommendation(
            rule_id=fired_rule,
            team_id=team_id,
            org_id=org_id,
            computed_at=computed_at,
            window_start=window_end - timedelta(days=14),
            window_end=window_end,
            severity="warning",
            title="WIP saturation rising",
            rationale="WIP up.",
            success_criterion="WIP < 3",
            evidence=(),
        )

    # `should_fire` toggles which run we are in; both runs share `now`.
    state = {"should_fire": True}

    def _evaluator_factory(rid: str):
        def _fn(snapshot, engine_now):
            if rid == fired_rule and state["should_fire"]:
                return _make_fired_rec(engine_now)
            return None

        return _fn

    injected = {rid: _evaluator_factory(rid) for rid in RULE_EVALUATORS}

    class _InjectingRuleEngine(_RealRuleEngine):
        """Injects deterministic evaluators and bypasses the live loader.

        ``evaluate_state`` (the worker entrypoint) calls ``evaluate`` which
        normally hits ``loader.load_team_metrics_window``. We override
        ``evaluate`` to feed the injected evaluators a ``None`` snapshot so the
        test never touches the metrics tables — the evaluators ignore the
        snapshot anyway. The full tombstone state (fired + non-fired) is still
        built by the unmodified ``evaluate_state``.
        """

        def __init__(self, *args, **kwargs):
            kwargs["evaluators"] = injected
            super().__init__(*args, **kwargs)

        def evaluate(self, team_id, org_id, window_start, window_end):
            results = []
            for fn in self._evaluators.values():
                rec = fn(None, self._now)
                if rec is not None:
                    results.append(rec)
            return results

    monkeypatch.setattr(
        "dev_health_ops.recommendations.engine.RuleEngine", _InjectingRuleEngine
    )

    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)
    try:

        def _read_rows() -> list[tuple]:
            res = sink.client.query(
                "SELECT rule_id, fired, computed_at "
                "FROM recommendations_daily "
                "WHERE org_id = {o:String} AND team_id = {t:String} "
                "  AND window_end = {we:Date} "
                "ORDER BY computed_at",
                parameters={
                    "o": org_id,
                    "t": team_id,
                    "we": window_end.isoformat(),
                },
            )
            return list(res.result_rows or [])

        def _resolver_fired() -> set[str]:
            res = sink.client.query(
                _RECOMMENDATIONS_SQL,
                parameters={
                    "team_id": team_id,
                    "org_id": org_id,
                    "window_start": (window_end - timedelta(days=14)).isoformat(),
                    "window_end": window_end.isoformat(),
                },
            )
            return {row[2] for row in (res.result_rows or [])}  # rule_id col

        # --- Run 1: target rule fires for this as_of day. ---
        state["should_fire"] = True
        tasks_mod._compute_recommendations_for_org(
            org_id=org_id,
            db_url=CLICKHOUSE_URI,
            window=14,
            now=now,
            as_of_day=as_of_day,
            team_id=team_id,
        )
        assert _resolver_fired() == {fired_rule}, (
            "run-1 fired rule should be visible (positive control)"
        )

        # Guarantee the wall-clock re-stamp is strictly newer on run 2 even on
        # fast hardware; computed_at is second-granularity in ClickHouse.
        _time.sleep(1.1)

        # --- Run 2: SAME as_of (same `now`) but the rule has recovered. ---
        state["should_fire"] = False
        tasks_mod._compute_recommendations_for_org(
            org_id=org_id,
            db_url=CLICKHOUSE_URI,
            window=14,
            now=now,  # identical -> engine now/window_end/computed_at unchanged
            as_of_day=as_of_day,
            team_id=team_id,
        )

        # The target rule's two rows live at the SAME (org, team, rule,
        # window_end) but must carry DISTINCT computed_at — proving the
        # wall-clock re-stamp moved, not the (constant) engine `now`.
        rule_rows = [r for r in _read_rows() if r[0] == fired_rule]
        assert len(rule_rows) == 2, f"expected 2 rows for {fired_rule}, got {rule_rows}"
        run1_row, run2_row = rule_rows
        assert run1_row[1] is True or run1_row[1] == 1  # run 1 fired
        assert run2_row[1] is False or run2_row[1] == 0  # run 2 tombstone
        assert run2_row[2] > run1_row[2], (
            "run-2 computed_at must be strictly newer than run-1: the worker's "
            "wall-clock re-stamp is what breaks the same-as_of tie (CHAOS-2398)"
        )

        # Resolver collapse: argMax(fired, computed_at) picks the newer
        # tombstone, so the recovered rule is CLEARED after run 2.
        assert _resolver_fired() == set(), (
            "recovered rule still visible after same-as_of re-run: "
            "argMax(fired, computed_at) could not pick run-2 (re-stamp missing?)"
        )
    finally:
        sink.client.command(
            "ALTER TABLE recommendations_daily DELETE WHERE org_id = {o:String} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )
        sink.close()


# ---------------------------------------------------------------------------
# CHAOS-2398 (DB-free): the worker re-stamps computed_at with a fresh
# wall-clock per run while window_end stays a pure function of as_of.
# ---------------------------------------------------------------------------


class _CapturingSink:
    """Fake ClickHouseMetricsSink: captures write_recommendations batches.

    Mirrors only the surface ``_compute_recommendations_for_org`` touches —
    ``.client`` (handed to the loader, never queried here because the engine is
    stubbed), ``write_recommendations`` and ``close``.
    """

    def __init__(self, *args, **kwargs) -> None:
        self.client = object()
        self.batches: list[list[RecommendationRecord]] = []

    def write_recommendations(self, records):
        # Snapshot the list so later mutations can't alias an earlier capture.
        self.batches.append(list(records))

    def close(self) -> None:
        pass


def test_compute_restamps_distinct_computed_at_per_run_same_as_of(monkeypatch):
    """Two _compute writes for identical (org, team, rule, window_end) carry
    DISTINCT, monotonically-increasing computed_at — proving the CHAOS-2398
    wall-clock re-stamp, not the (constant) engine ``now``, differentiates runs.

    Pure unit test (no live DB): a capturing fake sink records the exact batches
    handed to ``write_recommendations``. Both runs pass an *identical* ``now``
    so the engine-derived ``window_end`` and engine ``computed_at`` are constant;
    the only moving part is the worker's ``write_ts = datetime.now(UTC)``
    re-stamp. We freeze ``datetime.now`` in the worker module to two explicit,
    increasing instants so the assertion is deterministic (no sleep / flake).
    """
    from dev_health_ops.recommendations.engine import RuleEngine as _RealRuleEngine
    from dev_health_ops.recommendations.rules import RULE_EVALUATORS
    from dev_health_ops.recommendations.schema import Recommendation
    from dev_health_ops.workers import recommendations_tasks as tasks_mod

    org_id = f"org-{uuid.uuid4()}"
    team_id = f"team-{uuid.uuid4()}"
    fired_rule = "saturation"

    as_of_day = date(2026, 4, 8)
    now = datetime(2026, 4, 9, 0, 0, 0, tzinfo=timezone.utc)
    expected_window_end = now.date()  # worker/engine convention: as_of_day + 1

    monkeypatch.setattr(tasks_mod, "_discover_team_ids", lambda client, oid: [team_id])
    monkeypatch.setattr(tasks_mod, "_daily_metrics_ready", lambda oid, day: True)

    capturing = _CapturingSink()
    monkeypatch.setattr(
        "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink",
        lambda *a, **k: capturing,
    )

    state = {"should_fire": True}

    def _fire_fn(snapshot, engine_now):
        if not state["should_fire"]:
            return None
        return Recommendation(
            rule_id=fired_rule,
            team_id=team_id,
            org_id=org_id,
            computed_at=engine_now,  # engine derives this from the constant `now`
            window_start=engine_now.date() - timedelta(days=14),
            window_end=engine_now.date(),
            severity="warning",
            title="WIP saturation rising",
            rationale="WIP up.",
            success_criterion="WIP < 3",
            evidence=(),
        )

    injected = {
        rid: (_fire_fn if rid == fired_rule else (lambda s, n: None))
        for rid in RULE_EVALUATORS
    }

    class _InjectingRuleEngine(_RealRuleEngine):
        def __init__(self, *args, **kwargs):
            kwargs["evaluators"] = injected
            super().__init__(*args, **kwargs)

        def evaluate(self, team_id, org_id, window_start, window_end):
            return [
                rec
                for rec in (fn(None, self._now) for fn in self._evaluators.values())
                if rec is not None
            ]

    monkeypatch.setattr(
        "dev_health_ops.recommendations.engine.RuleEngine", _InjectingRuleEngine
    )

    # Freeze the worker's wall-clock re-stamp to two explicit, increasing values.
    write_stamps = iter(
        [
            datetime(2026, 6, 14, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 6, 14, 12, 0, 5, tzinfo=timezone.utc),
        ]
    )

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: D401 - match datetime.now signature
            return next(write_stamps)

    monkeypatch.setattr(tasks_mod, "datetime", _FrozenDatetime)

    # --- Run 1: rule fires. ---
    state["should_fire"] = True
    tasks_mod._compute_recommendations_for_org(
        org_id=org_id,
        db_url="clickhouse://unused",
        window=14,
        now=now,
        as_of_day=as_of_day,
        team_id=team_id,
    )

    # --- Run 2: SAME as_of (same `now`), rule has recovered. ---
    state["should_fire"] = False
    tasks_mod._compute_recommendations_for_org(
        org_id=org_id,
        db_url="clickhouse://unused",
        window=14,
        now=now,
        as_of_day=as_of_day,
        team_id=team_id,
    )

    assert len(capturing.batches) == 2, "expected one write per run"
    run1, run2 = capturing.batches

    r1 = next(r for r in run1 if r.rule_id == fired_rule)
    r2 = next(r for r in run2 if r.rule_id == fired_rule)

    # Same (org, team, rule, window_end) across both runs — the read key.
    assert (r1.org_id, r1.team_id, r1.rule_id, r1.window_end) == (
        r2.org_id,
        r2.team_id,
        r2.rule_id,
        r2.window_end,
    )
    assert r1.window_end == expected_window_end
    # State flipped: fired on run 1, tombstone on run 2.
    assert r1.fired is True
    assert r2.fired is False
    # The load-bearing CHAOS-2398 invariant: distinct, strictly-increasing
    # computed_at even though the engine `now` (and thus window_end) is identical.
    assert r1.computed_at != r2.computed_at, (
        "re-runs of the same as_of must carry distinct computed_at; without the "
        "wall-clock re-stamp both rows would share the engine `now` and the "
        "resolver's argMax(fired, computed_at) tie could pick the stale fired row"
    )
    assert r2.computed_at > r1.computed_at, "later run must be strictly newer"
    # And the re-stamp is the WALL-CLOCK value, not the engine `now`.
    assert r1.computed_at == datetime(2026, 6, 14, 12, 0, 0, tzinfo=timezone.utc)
    assert r2.computed_at == datetime(2026, 6, 14, 12, 0, 5, tzinfo=timezone.utc)
    assert r1.computed_at != now and r2.computed_at != now
