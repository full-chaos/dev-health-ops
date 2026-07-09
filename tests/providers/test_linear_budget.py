from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone

from dev_health_ops.providers.linear.budget import LinearBudgetEstimator
from dev_health_ops.sync.budget import BudgetDimension, estimate_provider_budget
from dev_health_ops.sync.budget_guard import (
    _budget_key,
    _limit_for_bucket,
    _parse_budget_limits,
)
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

WINDOW_START = datetime(2026, 1, 10, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 1, 12, tzinfo=timezone.utc)


def _context(
    *,
    provider: str = "linear",
    dataset_key: str,
    processor_flags: dict[str, bool] | None = None,
    credentials: Mapping[str, object] | None = None,
    credential_id: str | None = "credential-1",
) -> SyncTaskContext:
    decrypted_credentials: dict[str, object] = {"api_key": "secret-api-key"}
    if credentials is not None:
        decrypted_credentials = {str(key): value for key, value in credentials.items()}
    return SyncTaskContext(
        unit_id="unit-1",
        sync_run_id="run-1",
        org_id="org-1",
        integration_id="integration-1",
        source_id="source-1",
        source_external_id="TEAM",
        provider=provider,
        dataset_key=dataset_key,
        cost_class="medium",
        mode="incremental",
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        processor_flags=processor_flags or {},
        credential_id=credential_id,
        decrypted_credentials=decrypted_credentials,
        db_url="clickhouse://localhost/default",
    )


def test_linear_budget_estimator_returns_graphql_budget_for_work_items() -> None:
    estimates = LinearBudgetEstimator().estimate(_context(dataset_key="work-items"))

    assert len(estimates) == 6
    estimate = estimates[0]
    assert estimate.bucket.provider == "linear"
    assert estimate.bucket.org_id == "org-1"
    assert estimate.bucket.host == "api.linear.app"
    assert estimate.bucket.dimension is BudgetDimension.GRAPHQL_COST
    assert estimate.estimated_units == 1
    assert estimate.confidence == "medium"
    assert estimate.to_dict()["bucket"]["dimension"] == "graphql_cost"


def test_linear_budget_work_item_routes_model_cursor_paginated_edges() -> None:
    estimates = LinearBudgetEstimator().estimate(_context(dataset_key="work-items"))

    assert [estimate.bucket.dimension for estimate in estimates] == [
        BudgetDimension.GRAPHQL_COST,
        BudgetDimension.GRAPHQL_COST,
        BudgetDimension.GRAPHQL_COST,
        BudgetDimension.GRAPHQL_COST,
        BudgetDimension.GRAPHQL_COST,
        BudgetDimension.GRAPHQL_COST,
    ]
    assert {estimate.route_family for estimate in estimates} == {
        "attachments",
        "comments",
        "cycles",
        "history",
        "issues",
        "teams",
    }


def test_linear_budget_estimator_splits_work_item_dimension_datasets() -> None:
    labels = LinearBudgetEstimator().estimate(_context(dataset_key="work-item-labels"))
    projects = LinearBudgetEstimator().estimate(
        _context(dataset_key="work-item-projects")
    )
    history = LinearBudgetEstimator().estimate(
        _context(dataset_key="work-item-history")
    )
    comments = LinearBudgetEstimator().estimate(
        _context(dataset_key="work-item-comments")
    )

    assert {estimate.route_family for estimate in labels} == {"teams", "team_members"}
    assert [estimate.route_family for estimate in projects] == ["projects"]
    assert [estimate.route_family for estimate in history] == ["history"]
    assert [estimate.route_family for estimate in comments] == ["comments"]
    assert all(
        estimate.bucket.dimension is BudgetDimension.GRAPHQL_COST
        for estimate in labels + projects + history + comments
    )


def test_linear_budget_route_family_limits_override_dimension_defaults() -> None:
    issue_budget = next(
        estimate
        for estimate in LinearBudgetEstimator().estimate(
            _context(dataset_key="work-items")
        )
        if estimate.route_family == "issues"
    )
    comment_budget = next(
        estimate
        for estimate in LinearBudgetEstimator().estimate(
            _context(dataset_key="work-items")
        )
        if estimate.route_family == "comments"
    )
    limits = _parse_budget_limits(
        '{"linear:graphql_cost:issues": 9, "linear:graphql_cost": 2}'
    )

    assert _budget_key(
        issue_budget.bucket.to_dict(), route_family=issue_budget.route_family
    ).endswith(":graphql_cost:issues")
    assert (
        _limit_for_bucket(
            issue_budget.bucket.to_dict(),
            route_family=issue_budget.route_family,
            limits=limits,
            default_limit=100,
        )
        == 9
    )
    assert (
        _limit_for_bucket(
            comment_budget.bucket.to_dict(),
            route_family=comment_budget.route_family,
            limits=limits,
            default_limit=100,
        )
        == 2
    )


def test_linear_budget_all_route_families_can_override_graphql_default() -> None:
    estimates = LinearBudgetEstimator().estimate(_context(dataset_key="work-items"))
    limits = _parse_budget_limits(
        "{"
        '"linear:graphql_cost:teams": 1,'
        '"linear:graphql_cost:issues": 2,'
        '"linear:graphql_cost:cycles": 3,'
        '"linear:graphql_cost:comments": 4,'
        '"linear:graphql_cost:attachments": 5,'
        '"linear:graphql_cost:history": 6,'
        '"linear:graphql_cost": 99'
        "}"
    )

    assert {
        estimate.route_family: _limit_for_bucket(
            estimate.bucket.to_dict(),
            route_family=estimate.route_family,
            limits=limits,
            default_limit=100,
        )
        for estimate in estimates
    } == {
        "teams": 1,
        "issues": 2,
        "cycles": 3,
        "comments": 4,
        "attachments": 5,
        "history": 6,
    }


def test_linear_budget_route_family_override_parsing_ignores_invalid_values() -> None:
    limits = _parse_budget_limits(
        '{"linear:graphql_cost:issues": "8", "bad": "nope", "negative": -1}'
    )

    assert limits == {"linear:graphql_cost:issues": 8, "negative": 0}
    assert _parse_budget_limits("not-json") == {}
    assert _parse_budget_limits("[]") == {}


def test_linear_budget_estimator_scopes_bucket_to_host_and_safe_credential_fields() -> (
    None
):
    base_credentials = {
        "api_key": "secret-api-key",
        "base_url": "https://linear.example.com/graphql",
    }
    rotated_key_credentials = {
        "api_key": "rotated-api-key",
        "base_url": "https://linear.example.com/graphql",
    }
    workspace_credentials = {
        "api_key": "secret-api-key",
        "workspace_id": "workspace-1",
        "base_url": "https://linear.example.com/graphql",
    }

    base = LinearBudgetEstimator().estimate(
        _context(dataset_key="work-items", credentials=base_credentials)
    )[0]
    rotated = LinearBudgetEstimator().estimate(
        _context(
            dataset_key="work-items",
            credentials=rotated_key_credentials,
            credential_id="credential-2",
        )
    )[0]
    workspace = LinearBudgetEstimator().estimate(
        _context(dataset_key="work-items", credentials=workspace_credentials)
    )[0]

    assert base.bucket.host == "linear.example.com"
    assert base.bucket.credential_fingerprint != rotated.bucket.credential_fingerprint
    assert base.bucket.credential_fingerprint != workspace.bucket.credential_fingerprint
    assert "secret-api-key" not in str(base.to_dict())
    assert "rotated-api-key" not in str(rotated.to_dict())


def test_linear_budget_estimator_normalizes_camel_case_base_url_for_fingerprint() -> (
    None
):
    snake = LinearBudgetEstimator().estimate(
        _context(
            dataset_key="work-items",
            credentials={
                "api_key": "secret-api-key",
                "base_url": "https://linear.test",
            },
        )
    )[0]
    camel = LinearBudgetEstimator().estimate(
        _context(
            dataset_key="work-items",
            credentials={"api_key": "secret-api-key", "baseUrl": "https://linear.test"},
        )
    )[0]

    assert snake.bucket.host == camel.bucket.host == "linear.test"
    assert snake.bucket.credential_fingerprint == camel.bucket.credential_fingerprint


def test_linear_budget_estimator_scopes_empty_credentials_to_integration() -> None:
    first = LinearBudgetEstimator().estimate(
        _context(dataset_key="work-items", credentials={}, credential_id=None)
    )[0]
    second = LinearBudgetEstimator().estimate(
        _context(dataset_key="work-items", credentials={}, credential_id="credential-2")
    )[0]

    assert first.bucket.credential_fingerprint != second.bucket.credential_fingerprint


def test_linear_budget_estimator_returns_empty_for_unknown_dataset() -> None:
    assert LinearBudgetEstimator().estimate(_context(dataset_key="unknown")) == ()


def test_estimate_provider_budget_delegates_to_linear_estimator() -> None:
    estimates = estimate_provider_budget(_context(dataset_key="work-items"))

    assert len(estimates) == 6
    assert estimates[0].bucket.provider == "linear"


def test_estimate_provider_budget_returns_empty_for_different_provider() -> None:
    estimates = estimate_provider_budget(
        _context(provider="bitbucket", dataset_key="work-items")
    )

    assert estimates == ()
