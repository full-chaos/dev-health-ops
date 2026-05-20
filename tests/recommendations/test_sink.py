"""Tests for RecommendationsMixin.write_recommendations.

Uses a mock ClickHouse client — no live database required.
Verifies that the mixin calls _insert_rows with the correct table name
and column list.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock

from dev_health_ops.recommendations.snapshot import RecommendationRecord

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(
    team_id: str = "team-1",
    rule_id: str = "saturation",
    fired: bool = True,
) -> RecommendationRecord:
    return RecommendationRecord(
        team_id=team_id,
        org_id="org-1",
        rule_id=rule_id,
        rule_version="1.0.0",
        window_start=date(2025, 1, 1),
        window_end=date(2025, 1, 8),
        fired=fired,
        severity="warning",
        title="Team is saturating.",
        rationale="WIP slope exceeds threshold.",
        success_criterion="WIP trend turns negative within 2 cycles.",
        evidence_json='[{"team_id": "team-1", "metric_table": "work_item_metrics_daily",'
        ' "window_start": "2025-01-01", "window_end": "2025-01-08",'
        ' "field": "wip_count_end_of_day", "value": 0.3}]',
        computed_at=datetime(2025, 1, 8, 12, 0, 0, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# RecommendationsMixin
# ---------------------------------------------------------------------------


class TestRecommendationsMixin:
    def _make_sink(self) -> ClickHouseMetricsSink:
        """Build a ClickHouseMetricsSink with a mocked client."""
        from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

        mock_client = MagicMock()
        sink = ClickHouseMetricsSink.__new__(ClickHouseMetricsSink)
        sink.client = mock_client
        sink.org_id = ""
        object.__setattr__(sink, "_insert_rows", MagicMock())
        return sink

    def test_write_recommendations_calls_insert_rows(self) -> None:
        sink = self._make_sink()
        insert_rows = cast(MagicMock, sink._insert_rows)
        records = [_make_record()]
        sink.write_recommendations(records)

        insert_rows.assert_called_once()
        call_args = insert_rows.call_args
        table_name = call_args[0][0]
        columns = call_args[0][1]

        assert table_name == "recommendations_daily"
        assert "team_id" in columns
        assert "rule_id" in columns
        assert "fired" in columns
        assert "evidence_json" in columns
        assert "computed_at" in columns

    def test_write_recommendations_empty_is_noop(self) -> None:
        sink = self._make_sink()
        insert_rows = cast(MagicMock, sink._insert_rows)
        sink.write_recommendations([])
        insert_rows.assert_not_called()

    def test_write_recommendations_multiple_records(self) -> None:
        sink = self._make_sink()
        records = [
            _make_record(rule_id="saturation"),
            _make_record(rule_id="thrash"),
            _make_record(rule_id="review-concentration"),
        ]
        sink.write_recommendations(records)
        insert_rows = cast(MagicMock, sink._insert_rows)
        insert_rows.assert_called_once()
        # Rows passed are the records
        passed_rows = insert_rows.call_args[0][2]
        assert len(passed_rows) == 3

    def test_write_recommendations_includes_rule_version(self) -> None:
        sink = self._make_sink()
        sink.write_recommendations([_make_record()])
        insert_rows = cast(MagicMock, sink._insert_rows)
        columns = insert_rows.call_args[0][1]
        assert "rule_version" in columns


# ---------------------------------------------------------------------------
# recommendation_to_record helper
# ---------------------------------------------------------------------------


class TestRecommendationToRecord:
    def test_converts_recommendation_to_record(self) -> None:
        from datetime import date, datetime, timezone

        from dev_health_ops.recommendations.loader import recommendation_to_record
        from dev_health_ops.recommendations.schema import EvidenceRef, Recommendation

        ev = EvidenceRef(
            team_id="t1",
            metric_table="work_item_metrics_daily",
            window_start=date(2025, 1, 1),
            window_end=date(2025, 1, 8),
            field="wip_count_end_of_day",
            value=0.3,
        )
        rec = Recommendation(
            rule_id="saturation",
            team_id="t1",
            org_id="o1",
            computed_at=datetime(2025, 1, 8, tzinfo=timezone.utc),
            window_start=date(2025, 1, 1),
            window_end=date(2025, 1, 8),
            severity="warning",
            title="Team is saturating.",
            rationale="WIP slope.",
            success_criterion="WIP turns negative.",
            evidence=(ev,),
        )
        record = recommendation_to_record(rec)

        assert record.rule_id == "saturation"
        assert record.team_id == "t1"
        assert record.fired is True
        assert record.severity == "warning"

    def test_evidence_serialised_as_json(self) -> None:
        import json

        from dev_health_ops.recommendations.loader import recommendation_to_record
        from dev_health_ops.recommendations.schema import EvidenceRef, Recommendation

        ev = EvidenceRef(
            team_id="t1",
            metric_table="work_item_metrics_daily",
            window_start=date(2025, 1, 1),
            window_end=date(2025, 1, 8),
            field="wip_count_end_of_day",
            value=1.5,
        )
        rec = Recommendation(
            rule_id="saturation",
            team_id="t1",
            org_id="o1",
            computed_at=datetime(2025, 1, 8, tzinfo=timezone.utc),
            window_start=date(2025, 1, 1),
            window_end=date(2025, 1, 8),
            severity="warning",
            title="T",
            rationale="R",
            success_criterion="S",
            evidence=(ev,),
        )
        record = recommendation_to_record(rec)
        evidence = json.loads(record.evidence_json)
        assert len(evidence) == 1
        assert evidence[0]["field"] == "wip_count_end_of_day"
        assert evidence[0]["value"] == 1.5
        assert evidence[0]["metric_table"] == "work_item_metrics_daily"
