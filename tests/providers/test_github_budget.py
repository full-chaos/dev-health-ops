from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone

from dev_health_ops.providers.github.budget import (
    GITHUB_USAGE_ROUTE_FAMILIES,
    GitHubBudgetEstimator,
)
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
    provider: str = "github",
    dataset_key: str,
    processor_flags: dict[str, bool] | None = None,
    credentials: Mapping[str, object] | None = None,
    credential_id: str | None = "credential-1",
) -> SyncTaskContext:
    decrypted_credentials: dict[str, object] = {"token": "secret-token"}
    if credentials is not None:
        decrypted_credentials = {str(key): value for key, value in credentials.items()}
    return SyncTaskContext(
        unit_id="unit-1",
        sync_run_id="run-1",
        org_id="org-1",
        integration_id="integration-1",
        source_id="source-1",
        source_external_id="full-chaos/dev-health",
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


def test_github_budget_estimator_returns_core_budget_for_repo_metadata() -> None:
    estimates = GitHubBudgetEstimator().estimate(_context(dataset_key="repo-metadata"))

    assert len(estimates) == 1
    estimate = estimates[0]
    assert estimate.bucket.provider == "github"
    assert estimate.bucket.org_id == "org-1"
    assert estimate.bucket.host == "api.github.com"
    assert estimate.bucket.dimension is BudgetDimension.REST_CORE
    assert estimate.estimated_units == 1
    assert estimate.confidence == "high"
    assert estimate.to_dict()["bucket"]["dimension"] == "rest_core"


def test_github_budget_light_metadata_stays_on_core_route_family() -> None:
    estimates = GitHubBudgetEstimator().estimate(_context(dataset_key="repo-metadata"))

    assert [
        (estimate.bucket.dimension, estimate.route_family) for estimate in estimates
    ] == [(BudgetDimension.REST_CORE, "repo")]


def test_github_git_and_commit_stats_actuals_markers_are_live() -> None:
    markers = {
        (family.route_family, family.dimension): family.operation_markers
        for family in GITHUB_USAGE_ROUTE_FAMILIES
    }

    assert markers[("git", BudgetDimension.REST_CORE)] == ("git:",)
    assert markers[("commit_stats", BudgetDimension.REST_CORE)] == ("commit_stats:",)
    assert markers[("commit_stats", BudgetDimension.CONTENTS_BLOB)] == ()


def test_github_budget_estimator_splits_pr_social_pressure() -> None:
    estimates = GitHubBudgetEstimator().estimate(_context(dataset_key="pr-comments"))

    dimensions = {estimate.bucket.dimension for estimate in estimates}
    assert dimensions == {
        BudgetDimension.REST_CORE,
        BudgetDimension.GRAPHQL_COST,
        BudgetDimension.SECONDARY_ABUSE_RISK,
    }
    assert {estimate.route_family for estimate in estimates} == {"prs", "pr_social"}


def test_github_budget_route_family_limits_override_dimension_defaults() -> None:
    files_contents = next(
        estimate
        for estimate in GitHubBudgetEstimator().estimate(_context(dataset_key="files"))
        if estimate.bucket.dimension is BudgetDimension.CONTENTS_BLOB
    )
    blame_contents = next(
        estimate
        for estimate in GitHubBudgetEstimator().estimate(_context(dataset_key="blame"))
        if estimate.bucket.dimension is BudgetDimension.CONTENTS_BLOB
    )
    limits = _parse_budget_limits(
        '{"github:contents_blob:files": 7, "github:contents_blob": 1}'
    )

    assert _budget_key(
        files_contents.bucket.to_dict(), route_family=files_contents.route_family
    ).endswith(":contents_blob:files")
    assert (
        _limit_for_bucket(
            files_contents.bucket.to_dict(),
            route_family=files_contents.route_family,
            limits=limits,
            default_limit=100,
        )
        == 7
    )
    assert (
        _limit_for_bucket(
            blame_contents.bucket.to_dict(),
            route_family=blame_contents.route_family,
            limits=limits,
            default_limit=100,
        )
        == 1
    )


def test_github_budget_route_family_override_parsing_ignores_invalid_values() -> None:
    limits = _parse_budget_limits(
        '{"github:contents_blob:files": "8", "bad": "nope", "negative": -1}'
    )

    assert limits == {"github:contents_blob:files": 8, "negative": 0}
    assert _parse_budget_limits("not-json") == {}
    assert _parse_budget_limits("[]") == {}


def test_github_budget_estimator_uses_work_item_pr_flag_for_pr_expansion() -> None:
    estimates_without_prs = GitHubBudgetEstimator().estimate(
        _context(dataset_key="work-items", processor_flags={"sync_prs": False})
    )
    estimates_with_prs = GitHubBudgetEstimator().estimate(
        _context(dataset_key="work-items", processor_flags={"sync_prs": True})
    )

    assert [estimate.bucket.dimension for estimate in estimates_without_prs] == [
        BudgetDimension.REST_CORE
    ]
    assert {estimate.bucket.dimension for estimate in estimates_with_prs} == {
        BudgetDimension.REST_CORE,
        BudgetDimension.GRAPHQL_COST,
        BudgetDimension.SECONDARY_ABUSE_RISK,
    }


def test_github_budget_estimator_scopes_bucket_to_host_and_safe_credential_fields() -> (
    None
):
    base_credentials = {
        "token": "secret-token",
        "base_url": "https://github.example.com/api/v3",
    }
    rotated_token_credentials = {
        "token": "rotated-token",
        "base_url": "https://github.example.com/api/v3",
    }
    app_credentials = {
        "app_id": "123",
        "private_key": "secret-private-key",
        "installation_id": "456",
        "base_url": "https://github.example.com/api/v3",
    }

    base = GitHubBudgetEstimator().estimate(
        _context(dataset_key="commits", credentials=base_credentials)
    )[0]
    rotated = GitHubBudgetEstimator().estimate(
        _context(dataset_key="commits", credentials=rotated_token_credentials)
    )[0]
    app = GitHubBudgetEstimator().estimate(
        _context(dataset_key="commits", credentials=app_credentials)
    )[0]

    assert base.bucket.host == "github.example.com"
    assert base.bucket.credential_fingerprint != rotated.bucket.credential_fingerprint
    assert base.bucket.credential_fingerprint != app.bucket.credential_fingerprint
    assert "secret-token" not in str(base.to_dict())
    assert "rotated-token" not in str(rotated.to_dict())
    assert "secret-private-key" not in str(app.to_dict())


def test_github_budget_estimator_normalizes_camel_case_base_url_for_fingerprint() -> (
    None
):
    snake = GitHubBudgetEstimator().estimate(
        _context(
            dataset_key="commits",
            credentials={"token": "secret-token", "base_url": "https://github.test"},
        )
    )[0]
    camel = GitHubBudgetEstimator().estimate(
        _context(
            dataset_key="commits",
            credentials={"token": "secret-token", "baseUrl": "https://github.test"},
        )
    )[0]

    assert snake.bucket.host == camel.bucket.host == "github.test"
    assert snake.bucket.credential_fingerprint == camel.bucket.credential_fingerprint


def test_github_budget_estimator_scopes_empty_credentials_to_integration() -> None:
    first = GitHubBudgetEstimator().estimate(
        _context(dataset_key="commits", credentials={}, credential_id=None)
    )[0]
    second = GitHubBudgetEstimator().estimate(
        _context(dataset_key="commits", credentials={}, credential_id="credential-2")
    )[0]

    assert first.bucket.credential_fingerprint != second.bucket.credential_fingerprint


def test_github_budget_estimator_returns_empty_for_unknown_dataset() -> None:
    assert GitHubBudgetEstimator().estimate(_context(dataset_key="unknown")) == ()


def test_estimate_provider_budget_delegates_to_github_estimator() -> None:
    estimates = estimate_provider_budget(_context(dataset_key="repo-metadata"))

    assert len(estimates) == 1
    assert estimates[0].bucket.provider == "github"


def test_estimate_provider_budget_returns_empty_for_unimplemented_provider() -> None:
    estimates = estimate_provider_budget(
        _context(provider="bitbucket", dataset_key="feature-flags")
    )

    assert estimates == ()
