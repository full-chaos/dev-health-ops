from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dev_health_ops.providers.launchdarkly.budget import (
    LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES,
    LAUNCHDARKLY_BUDGET_ROUTE_FAMILY_KEYS,
)
from dev_health_ops.sync.budget import BudgetDimension, estimate_provider_budget
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

WINDOW_START = datetime(2026, 1, 10, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 1, 12, tzinfo=timezone.utc)


def _context() -> SyncTaskContext:
    return SyncTaskContext(
        unit_id="unit-ld-1",
        sync_run_id="run-ld-1",
        org_id="org-1",
        integration_id="integration-ld-1",
        source_id="source-ld-1",
        source_external_id="project:default",
        provider="launchdarkly",
        dataset_key="feature-flags",
        cost_class="medium",
        mode="incremental",
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        processor_flags={"sync_feature_flags": True},
        credential_id="credential-ld-1",
        decrypted_credentials={"api_key": "secret-token", "project_key": "default"},
        db_url="clickhouse://localhost/default",
    )


def test_launchdarkly_budget_contract_defines_expected_route_families() -> None:
    assert LAUNCHDARKLY_BUDGET_ROUTE_FAMILY_KEYS == {
        "projects",
        "flags",
        "segments",
        "audit_log",
        "members",
        "code_refs",
    }

    assert all(
        family.endpoint_patterns for family in LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES
    )
    assert all(family.cost_drivers for family in LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES)
    assert {family.dimension for family in LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES} == {
        BudgetDimension.REST_CORE,
        BudgetDimension.SECONDARY_ABUSE_RISK,
    }


@pytest.mark.xfail(
    reason=(
        "CHAOS-2687 is a planning gate; the future LaunchDarkly provider must add "
        "an estimator before raw sync ships."
    ),
    strict=True,
)
def test_future_launchdarkly_sync_units_emit_budget_estimates() -> None:
    estimates = estimate_provider_budget(_context())

    assert estimates
    assert {estimate.bucket.provider for estimate in estimates} == {"launchdarkly"}
    assert "flags" in {estimate.route_family for estimate in estimates}
