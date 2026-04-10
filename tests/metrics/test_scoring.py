from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

import pytest

from dev_health_ops.metrics.scoring.composite import CompositeScorer
from dev_health_ops.metrics.scoring.delivery import DeliveryScorer
from dev_health_ops.metrics.scoring.durability import DurabilityScorer
from dev_health_ops.metrics.scoring.dynamics import DynamicsScorer
from dev_health_ops.metrics.scoring.schemas import CompositeScore, DimensionScore
from dev_health_ops.metrics.scoring.wellbeing import WellbeingScorer


@dataclass
class FakeQueryResult:
    result_rows: list[tuple] = field(default_factory=list)
    column_names: list[str] = field(default_factory=list)


class FakeClickHouseClient:
    def __init__(self, responses: dict[str, FakeQueryResult] | None = None) -> None:
        self._responses = responses or {}
        self._default = FakeQueryResult()
        self.queries: list[str] = []

    def query(self, query: str, parameters: dict | None = None) -> FakeQueryResult:
        self.queries.append(query)
        for key, result in self._responses.items():
            if key in query:
                return result
        return self._default


_DAY = date(2025, 6, 15)
_NOW = datetime(2025, 6, 15, 12, 0, 0)
_ORG = "test-org"


class TestDeliveryScorer:
    def test_all_signals_present(self) -> None:
        client = FakeClickHouseClient(
            {
                "testops_pipeline_metrics_daily": FakeQueryResult(
                    result_rows=[(0.92, 1200.0)],
                    column_names=["avg_success_rate", "max_p95_duration"],
                ),
                "repo_metrics_daily": FakeQueryResult(
                    result_rows=[(24.0, 10)],
                    column_names=["avg_pr_cycle", "total_prs_merged"],
                ),
            }
        )
        scorer = DeliveryScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)

        assert result.dimension == "delivery"
        assert result.score is not None
        assert 0.0 <= result.score <= 1.0
        assert len(result.signals) == 4

        signal_map = {s.name: s for s in result.signals}
        assert signal_map["pipeline_success_rate"].raw_value == pytest.approx(0.92)
        assert signal_map["pipeline_success_rate"].normalized_value == pytest.approx(
            0.92
        )
        assert signal_map["pipeline_duration_p95"].normalized_value == pytest.approx(
            1.0 - 1200.0 / 3600.0
        )
        assert signal_map["pr_cycle_time"].normalized_value == pytest.approx(
            1.0 - 24.0 / 168.0
        )
        assert signal_map["throughput"].normalized_value == pytest.approx(10.0 / 50.0)

    def test_missing_pipeline_data(self) -> None:
        client = FakeClickHouseClient(
            {
                "repo_metrics_daily": FakeQueryResult(
                    result_rows=[(48.0, 5)],
                    column_names=["avg_pr_cycle", "total_prs_merged"],
                ),
            }
        )
        scorer = DeliveryScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)

        assert result.score is not None
        signal_map = {s.name: s for s in result.signals}
        assert signal_map["pipeline_success_rate"].normalized_value is None
        assert signal_map["pr_cycle_time"].normalized_value is not None

    def test_team_id_passed(self) -> None:
        client = FakeClickHouseClient()
        scorer = DeliveryScorer()
        result = scorer.compute(client, _ORG, _DAY, team_id="team-1", computed_at=_NOW)
        assert result.team_id == "team-1"
        assert any("team_id" in q for q in client.queries)


class TestDurabilityScorer:
    def test_all_signals_present(self) -> None:
        client = FakeClickHouseClient(
            {
                "testops_test_metrics_daily": FakeQueryResult(
                    result_rows=[(0.95, 0.08)],
                    column_names=["avg_pass_rate", "avg_flake_rate"],
                ),
                "testops_coverage_metrics_daily": FakeQueryResult(
                    result_rows=[(82.5, 70.0)],
                    column_names=["avg_line_cov", "avg_branch_cov"],
                ),
            }
        )
        scorer = DurabilityScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)

        assert result.score is not None
        assert 0.0 <= result.score <= 1.0
        signal_map = {s.name: s for s in result.signals}
        assert signal_map["test_pass_rate"].normalized_value == pytest.approx(0.95)
        assert signal_map["test_flake_rate_inverse"].normalized_value == pytest.approx(
            0.92
        )
        assert signal_map["coverage_line_pct"].normalized_value == pytest.approx(0.825)
        assert signal_map["coverage_branch_pct"].normalized_value == pytest.approx(0.70)

    def test_no_coverage_data(self) -> None:
        client = FakeClickHouseClient(
            {
                "testops_test_metrics_daily": FakeQueryResult(
                    result_rows=[(0.99, 0.01)],
                    column_names=["avg_pass_rate", "avg_flake_rate"],
                ),
            }
        )
        scorer = DurabilityScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)

        assert result.score is not None
        signal_map = {s.name: s for s in result.signals}
        assert signal_map["coverage_line_pct"].normalized_value is None


class TestWellbeingScorer:
    def test_all_signals_present(self) -> None:
        client = FakeClickHouseClient(
            {
                "testops_pipeline_metrics_daily": FakeQueryResult(
                    result_rows=[(120.0, 0.15)],
                    column_names=["avg_queue", "avg_rerun"],
                ),
                "team_metrics_daily": FakeQueryResult(
                    result_rows=[(0.12, 0.05)],
                    column_names=["avg_after_hours", "avg_weekend"],
                ),
            }
        )
        scorer = WellbeingScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)

        assert result.score is not None
        assert 0.0 <= result.score <= 1.0
        signal_map = {s.name: s for s in result.signals}
        assert signal_map[
            "pipeline_queue_time_inverse"
        ].normalized_value == pytest.approx(1.0 - 120.0 / 600.0)
        assert signal_map["rerun_rate_inverse"].normalized_value == pytest.approx(0.85)
        assert signal_map[
            "after_hours_ratio_inverse"
        ].normalized_value == pytest.approx(0.88)
        assert signal_map["weekend_ratio_inverse"].normalized_value == pytest.approx(
            0.95
        )

    def test_no_data(self) -> None:
        client = FakeClickHouseClient()
        scorer = WellbeingScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)
        assert result.score is None


class TestDynamicsScorer:
    def test_all_signals_present(self) -> None:
        client = FakeClickHouseClient(
            {
                "testops_quality_drag": FakeQueryResult(
                    result_rows=[(2.5,)],
                    column_names=["avg_drag"],
                ),
                "testops_pipeline_metrics_daily": FakeQueryResult(
                    result_rows=[(0.1, 0.6)],
                    column_names=["avg_failure_rate", "avg_rerun_on_failure"],
                ),
                "work_item_metrics_daily": FakeQueryResult(
                    result_rows=[(0.3,)],
                    column_names=["avg_congestion"],
                ),
            }
        )
        scorer = DynamicsScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)

        assert result.score is not None
        assert 0.0 <= result.score <= 1.0
        signal_map = {s.name: s for s in result.signals}
        assert signal_map["quality_drag_inverse"].normalized_value == pytest.approx(
            1.0 - 2.5 / 8.0
        )
        assert signal_map[
            "pipeline_failure_rate_inverse"
        ].normalized_value == pytest.approx(0.9)
        assert signal_map["failure_ownership"].normalized_value == pytest.approx(0.6)
        assert signal_map["wip_congestion_inverse"].normalized_value == pytest.approx(
            0.7
        )

    def test_partial_data(self) -> None:
        client = FakeClickHouseClient(
            {
                "testops_pipeline_metrics_daily": FakeQueryResult(
                    result_rows=[(0.2, 0.4)],
                    column_names=["avg_failure_rate", "avg_rerun_on_failure"],
                ),
            }
        )
        scorer = DynamicsScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)
        assert result.score is not None
        signal_map = {s.name: s for s in result.signals}
        assert signal_map["quality_drag_inverse"].normalized_value is None
        assert signal_map["wip_congestion_inverse"].normalized_value is None


class TestCompositeScorer:
    def test_full_composite(self) -> None:
        client = FakeClickHouseClient(
            {
                "testops_pipeline_metrics_daily": FakeQueryResult(
                    result_rows=[(0.9, 900.0, 60.0, 0.1, 0.08, 0.5)],
                    column_names=[
                        "avg_success_rate",
                        "max_p95_duration",
                        "avg_queue",
                        "avg_rerun",
                        "avg_failure_rate",
                        "avg_rerun_on_failure",
                    ],
                ),
                "repo_metrics_daily": FakeQueryResult(
                    result_rows=[(20.0, 8)],
                    column_names=["avg_pr_cycle", "total_prs_merged"],
                ),
                "testops_test_metrics_daily": FakeQueryResult(
                    result_rows=[(0.96, 0.03)],
                    column_names=["avg_pass_rate", "avg_flake_rate"],
                ),
                "testops_coverage_metrics_daily": FakeQueryResult(
                    result_rows=[(85.0, 72.0)],
                    column_names=["avg_line_cov", "avg_branch_cov"],
                ),
                "team_metrics_daily": FakeQueryResult(
                    result_rows=[(0.08, 0.02)],
                    column_names=["avg_after_hours", "avg_weekend"],
                ),
                "testops_quality_drag": FakeQueryResult(
                    result_rows=[(1.5,)],
                    column_names=["avg_drag"],
                ),
                "work_item_metrics_daily": FakeQueryResult(
                    result_rows=[(0.2,)],
                    column_names=["avg_congestion"],
                ),
            }
        )
        scorer = CompositeScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)

        assert isinstance(result, CompositeScore)
        assert result.score is not None
        assert 0.0 <= result.score <= 1.0
        assert len(result.dimensions) == 4
        assert result.day == _DAY
        assert result.org_id == _ORG

        dim_map = {d.dimension: d for d in result.dimensions}
        for name in ("delivery", "durability", "wellbeing", "dynamics"):
            assert dim_map[name].score is not None

    def test_composite_no_data(self) -> None:
        client = FakeClickHouseClient()
        scorer = CompositeScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)
        assert result.score is None
        assert len(result.dimensions) == 4

    def test_composite_partial_dimensions(self) -> None:
        client = FakeClickHouseClient(
            {
                "testops_test_metrics_daily": FakeQueryResult(
                    result_rows=[(0.90, 0.05)],
                    column_names=["avg_pass_rate", "avg_flake_rate"],
                ),
            }
        )
        scorer = CompositeScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)

        assert result.score is not None
        dim_map = {d.dimension: d for d in result.dimensions}
        assert dim_map["durability"].score is not None
        assert dim_map["delivery"].score is None


class TestEdgeCases:
    def test_clamp_extreme_values(self) -> None:
        client = FakeClickHouseClient(
            {
                "testops_pipeline_metrics_daily": FakeQueryResult(
                    result_rows=[(1.5, 99999.0)],
                    column_names=["avg_success_rate", "max_p95_duration"],
                ),
                "repo_metrics_daily": FakeQueryResult(
                    result_rows=[(500.0, 200)],
                    column_names=["avg_pr_cycle", "total_prs_merged"],
                ),
            }
        )
        scorer = DeliveryScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)

        signal_map = {s.name: s for s in result.signals}
        assert signal_map["pipeline_success_rate"].normalized_value == 1.0
        assert signal_map["pipeline_duration_p95"].normalized_value == 0.0
        assert signal_map["pr_cycle_time"].normalized_value == 0.0
        assert signal_map["throughput"].normalized_value == 1.0

    def test_zero_throughput(self) -> None:
        client = FakeClickHouseClient(
            {
                "repo_metrics_daily": FakeQueryResult(
                    result_rows=[(0.0, 0)],
                    column_names=["avg_pr_cycle", "total_prs_merged"],
                ),
            }
        )
        scorer = DeliveryScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)
        signal_map = {s.name: s for s in result.signals}
        assert signal_map["throughput"].normalized_value is None

    def test_none_values_in_row(self) -> None:
        client = FakeClickHouseClient(
            {
                "testops_pipeline_metrics_daily": FakeQueryResult(
                    result_rows=[(None, None)],
                    column_names=["avg_success_rate", "max_p95_duration"],
                ),
            }
        )
        scorer = DeliveryScorer()
        result = scorer.compute(client, _ORG, _DAY, computed_at=_NOW)
        signal_map = {s.name: s for s in result.signals}
        assert signal_map["pipeline_success_rate"].normalized_value is None
        assert signal_map["pipeline_duration_p95"].normalized_value is None

    def test_dimension_score_frozen(self) -> None:
        score = DimensionScore(dimension="test", score=0.5)
        with pytest.raises(AttributeError):
            score.score = 0.9  # type: ignore[misc]

    def test_signal_weights_sum(self) -> None:
        for scorer_cls in (
            DeliveryScorer,
            DurabilityScorer,
            WellbeingScorer,
            DynamicsScorer,
        ):
            scorer = scorer_cls()
            total = sum(w for _, w, _ in scorer.signal_definitions)
            assert total == pytest.approx(1.0), (
                f"{scorer.dimension_name} signal weights sum to {total}"
            )
