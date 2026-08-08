from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from dev_health_ops.analytics.investment import InvestmentClassifier
from dev_health_ops.metrics.work_item_engine_destinations import (
    compute_work_item_engine_destinations_daily,
)
from dev_health_ops.models.work_items import WorkItem
from dev_health_ops.providers.status_mapping import StatusMapping, load_status_mapping

_ROOT = Path(__file__).resolve().parents[2]


def _engines() -> tuple[StatusMapping, InvestmentClassifier]:
    status = load_status_mapping(
        _ROOT / "src/dev_health_ops/config/status_mapping.yaml"
    )
    investment = InvestmentClassifier(
        _ROOT / "src/dev_health_ops/config/investment_areas.yaml"
    )
    return status, investment


def test_engine_destination_records_carry_explicit_tenant_before_sink_projection():
    status, investment = _engines()
    issue_types, classifications, metrics = compute_work_item_engine_destinations_daily(
        day=date(2026, 8, 4),
        work_items=[
            WorkItem(
                work_item_id="acme/api#1",
                provider="github",
                title="security fix",
                type="issue",
                status="done",
                status_raw="closed",
                created_at=datetime(2026, 8, 4, 1, tzinfo=timezone.utc),
                started_at=datetime(2026, 8, 4, 2, tzinfo=timezone.utc),
                completed_at=datetime(2026, 8, 4, 3, tzinfo=timezone.utc),
                labels=["security", "bug"],
                story_points=2,
            )
        ],
        computed_at=datetime(2026, 8, 5, 0, 30, tzinfo=timezone.utc),
        org_id="org-acme",
        status_mapping=status,
        investment_classifier=investment,
    )

    assert issue_types and classifications and metrics
    assert {row.org_id for row in issue_types} == {"org-acme"}
    assert {row.org_id for row in classifications} == {"org-acme"}
    assert {row.org_id for row in metrics} == {"org-acme"}


def test_empty_input_evaluates_all_three_destinations_as_empty_lists():
    status, investment = _engines()
    result = compute_work_item_engine_destinations_daily(
        day=date(2026, 8, 4),
        work_items=[],
        computed_at=datetime(2026, 8, 5, 0, 30, tzinfo=timezone.utc),
        org_id="org-acme",
        status_mapping=status,
        investment_classifier=investment,
    )

    assert result == ([], [], [])
