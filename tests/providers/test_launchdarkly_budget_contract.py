from __future__ import annotations

from datetime import datetime, timezone

from dev_health_ops.providers.launchdarkly.budget import (
    LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES,
    LAUNCHDARKLY_BUDGET_ROUTE_FAMILY_KEYS,
    LaunchDarklyBudgetEstimator,
)
from dev_health_ops.sync.budget import BudgetDimension, estimate_provider_budget
from dev_health_ops.sync.datasets import DatasetKey
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

WINDOW_START = datetime(2026, 1, 10, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 1, 12, tzinfo=timezone.utc)


def _context(
    *,
    provider: str = "launchdarkly",
    dataset_key: str = DatasetKey.FEATURE_FLAGS.value,
    credentials: dict[str, object] | None = None,
    credential_id: str | None = "credential-ld-1",
    integration_id: str = "integration-ld-1",
) -> SyncTaskContext:
    return SyncTaskContext(
        unit_id="unit-ld-1",
        sync_run_id="run-ld-1",
        org_id="org-1",
        integration_id=integration_id,
        source_id="source-ld-1",
        source_external_id="project:default",
        provider=provider,
        dataset_key=dataset_key,
        cost_class="medium",
        mode="incremental",
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        processor_flags={"sync_feature_flags": True},
        credential_id=credential_id,
        decrypted_credentials=credentials
        or {"api_key": "secret-token", "project_key": "default"},
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
    assert {
        (family.route_family, family.dimension)
        for family in LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES
    } >= {
        ("flags", BudgetDimension.REST_CORE),
        ("audit_log", BudgetDimension.REST_CORE),
        ("code_refs", BudgetDimension.REST_CORE),
        ("code_refs", BudgetDimension.SECONDARY_ABUSE_RISK),
    }


def test_launchdarkly_feature_flags_estimator_returns_launchdarkly_buckets() -> None:
    estimates = LaunchDarklyBudgetEstimator().estimate(_context())

    assert estimates
    assert {estimate.bucket.provider for estimate in estimates} == {"launchdarkly"}
    assert {estimate.route_family for estimate in estimates} == {
        "flags",
        "audit_log",
        "code_refs",
    }
    assert {estimate.bucket.dimension for estimate in estimates} == {
        BudgetDimension.REST_CORE,
        BudgetDimension.SECONDARY_ABUSE_RISK,
    }
    assert [
        (estimate.route_family, estimate.estimated_units) for estimate in estimates
    ] == [
        ("flags", 2),
        ("audit_log", 52),
        ("code_refs", 1),
        ("code_refs", 1),
    ]


def test_launchdarkly_budget_estimator_defaults_to_launchdarkly_host() -> None:
    estimates = LaunchDarklyBudgetEstimator().estimate(_context(credentials={}))

    assert {estimate.bucket.host for estimate in estimates} == {"app.launchdarkly.com"}


def test_launchdarkly_budget_estimator_uses_base_url_host_override() -> None:
    estimates = LaunchDarklyBudgetEstimator().estimate(
        _context(
            credentials={
                "api_key": "secret-token",
                "baseUrl": "https://ld.example.com",
                "project_key": "default",
            }
        )
    )

    assert {estimate.bucket.host for estimate in estimates} == {"ld.example.com"}


def test_launchdarkly_budget_fingerprint_uses_safe_scope_without_api_key() -> None:
    base = LaunchDarklyBudgetEstimator().estimate(
        _context(
            credentials={
                "api_key": "secret-token",
                "project_key": "default",
                "environment": "production",
            }
        )
    )[0]
    rotated_key = LaunchDarklyBudgetEstimator().estimate(
        _context(
            credentials={
                "api_key": "rotated-secret-token",
                "project_key": "default",
                "environment": "production",
            }
        )
    )[0]
    different_project = LaunchDarklyBudgetEstimator().estimate(
        _context(
            credentials={
                "api_key": "secret-token",
                "project_key": "other-project",
                "environment": "production",
            }
        )
    )[0]

    assert (
        base.bucket.credential_fingerprint == rotated_key.bucket.credential_fingerprint
    )
    assert (
        base.bucket.credential_fingerprint
        != different_project.bucket.credential_fingerprint
    )
    assert "secret-token" not in str(base.to_dict())
    assert "rotated-secret-token" not in str(rotated_key.to_dict())


def test_launchdarkly_budget_estimator_returns_empty_for_unknown_dataset() -> None:
    assert (
        LaunchDarklyBudgetEstimator().estimate(_context(dataset_key="segments")) == ()
    )


def test_estimate_provider_budget_delegates_to_launchdarkly_estimator() -> None:
    estimates = estimate_provider_budget(_context())

    assert estimates
    assert {estimate.bucket.provider for estimate in estimates} == {"launchdarkly"}


def test_estimate_provider_budget_returns_empty_for_non_launchdarkly_provider() -> None:
    assert estimate_provider_budget(_context(provider="bitbucket")) == ()
