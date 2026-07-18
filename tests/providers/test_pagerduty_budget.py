from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dev_health_ops.providers.pagerduty.budget import PagerDutyBudgetEstimator
from dev_health_ops.sync.budget import BudgetDimension, estimate_provider_budget
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext


def _context(*, dataset_key: str, enrichment_cap: int) -> SyncTaskContext:
    return SyncTaskContext(
        unit_id="unit-1",
        sync_run_id="run-1",
        org_id="org-1",
        integration_id="integration-1",
        source_id="source-1",
        source_external_id="acme",
        provider="pagerduty",
        dataset_key=dataset_key,
        cost_class="medium",
        mode="incremental",
        window_start=datetime(2026, 7, 17, tzinfo=timezone.utc),
        window_end=datetime(2026, 7, 18, tzinfo=timezone.utc),
        processor_flags={},
        credential_id="credential-1",
        decrypted_credentials={"subdomain": "acme"},
        db_url="clickhouse://localhost/default",
        dataset_options={"enrichment_cap": enrichment_cap},
    )


@pytest.mark.parametrize(
    ("dataset_key", "enrichment_cap", "route_family", "expected_units"),
    [
        ("incident-alerts", 3, "pagerduty_alerts", 200),
        ("incident-log-entries", 101, "pagerduty_log_entries", 400),
        ("incident-notes", 3, "pagerduty_notes", 200),
    ],
)
def test_incident_enrichment_budget_reserves_bounded_fan_out(
    dataset_key: str,
    enrichment_cap: int,
    route_family: str,
    expected_units: int,
) -> None:
    estimates = PagerDutyBudgetEstimator().estimate(
        _context(dataset_key=dataset_key, enrichment_cap=enrichment_cap)
    )

    units_by_family = {
        estimate.route_family: estimate.estimated_units for estimate in estimates
    }
    assert units_by_family == {
        "pagerduty_incidents": 2,
        route_family: expected_units,
    }


def test_estimate_provider_budget_reserves_pagerduty_bounded_fan_out() -> None:
    estimates = estimate_provider_budget(
        _context(dataset_key="incident-alerts", enrichment_cap=3)
    )

    assert [
        (estimate.route_family, estimate.estimated_units) for estimate in estimates
    ] == [
        ("pagerduty_incidents", 2),
        ("pagerduty_alerts", 200),
    ]
    assert all(
        estimate.bucket.dimension is BudgetDimension.REST_CORE for estimate in estimates
    )
