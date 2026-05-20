from __future__ import annotations

from datetime import date, datetime, timezone

from dev_health_ops.recommendations import registry
from dev_health_ops.recommendations.engine import RuleEngine
from dev_health_ops.recommendations.snapshot import MetricsSnapshot


class FakeMetricsLoader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, date, date]] = []

    def load_team_metrics_window(
        self,
        team_id: str,
        org_id: str,
        window_start: date,
        window_end: date,
    ) -> MetricsSnapshot:
        self.calls.append((team_id, org_id, window_start, window_end))
        return MetricsSnapshot(
            team_id=team_id,
            org_id=org_id,
            window_start=window_start,
            window_end=window_end,
            wip_by_day=[5.0, 6.0, 7.0, 8.0],
            throughput_by_cycle=[10.0, 10.0],
            review_latency_p75_hours=48.0,
            reviewer_gini=0.8,
            rework_churn_ratio=0.5,
            after_hours_ratio=0.4,
            cycle_time_by_day=[12.0, 18.0, 24.0, 30.0],
            hotspot_complexity_delta=0.5,
            hotspot_churn_overlap=0.8,
        )


def test_engine_evaluate_returns_expected_rules() -> None:
    loader = FakeMetricsLoader()
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    engine = RuleEngine(registry=registry, loader=loader, now=now)

    recommendations = engine.evaluate(
        team_id="team-alpha",
        org_id="org-1",
        window_start=date(2026, 4, 1),
        window_end=date(2026, 4, 14),
    )

    assert loader.calls == [("team-alpha", "org-1", date(2026, 4, 1), date(2026, 4, 14))]
    assert [item.rule_id for item in recommendations] == [rule.id for rule in registry.all_rules()]
    assert {item.computed_at for item in recommendations} == {now}


def test_engine_evaluate_is_deterministic() -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    engine = RuleEngine(registry=registry, loader=FakeMetricsLoader(), now=now)
    args = ("team-alpha", "org-1", date(2026, 4, 1), date(2026, 4, 14))

    first = engine.evaluate(*args)
    second = engine.evaluate(*args)

    assert first == second
