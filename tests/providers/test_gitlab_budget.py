from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone

from dev_health_ops.providers.gitlab.budget import GitLabBudgetEstimator
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
    provider: str = "gitlab",
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


def test_gitlab_budget_estimator_returns_project_budget_for_repo_metadata() -> None:
    estimates = GitLabBudgetEstimator().estimate(_context(dataset_key="repo-metadata"))

    assert len(estimates) == 1
    estimate = estimates[0]
    assert estimate.bucket.provider == "gitlab"
    assert estimate.bucket.org_id == "org-1"
    assert estimate.bucket.host == "gitlab.com"
    assert estimate.bucket.dimension is BudgetDimension.REST_CORE
    assert estimate.estimated_units == 1
    assert estimate.confidence == "high"
    assert estimate.route_family == "project"
    assert estimate.to_dict()["bucket"]["dimension"] == "rest_core"


def test_gitlab_budget_estimator_maps_known_route_families_to_rest_core() -> None:
    work_items = GitLabBudgetEstimator().estimate(_context(dataset_key="work-items"))
    pipelines = GitLabBudgetEstimator().estimate(_context(dataset_key="tests"))
    pr_comments = GitLabBudgetEstimator().estimate(_context(dataset_key="pr-comments"))

    assert {estimate.bucket.dimension for estimate in work_items} == {
        BudgetDimension.REST_CORE
    }
    assert {estimate.route_family for estimate in work_items} == {
        "project",
        "milestones",
        "epics",
        "issues",
        "notes",
        "merge_requests",
    }
    assert [
        (estimate.bucket.dimension, estimate.route_family) for estimate in pipelines
    ] == [(BudgetDimension.REST_CORE, "pipelines")]
    assert {estimate.route_family for estimate in pr_comments} == {
        "merge_requests",
        "notes",
    }


def test_gitlab_budget_route_family_limits_override_dimension_defaults() -> None:
    issues = next(
        estimate
        for estimate in GitLabBudgetEstimator().estimate(
            _context(dataset_key="work-items")
        )
        if estimate.route_family == "issues"
    )
    notes = next(
        estimate
        for estimate in GitLabBudgetEstimator().estimate(
            _context(dataset_key="work-items")
        )
        if estimate.route_family == "notes"
    )
    limits = _parse_budget_limits(
        '{"gitlab:rest_core:issues": 7, "gitlab:rest_core": 1}'
    )

    assert _budget_key(
        issues.bucket.to_dict(), route_family=issues.route_family
    ).endswith(":rest_core:issues")
    assert (
        _limit_for_bucket(
            issues.bucket.to_dict(),
            route_family=issues.route_family,
            limits=limits,
            default_limit=100,
        )
        == 7
    )
    assert (
        _limit_for_bucket(
            notes.bucket.to_dict(),
            route_family=notes.route_family,
            limits=limits,
            default_limit=100,
        )
        == 1
    )


def test_gitlab_budget_route_family_override_parsing_ignores_invalid_values() -> None:
    limits = _parse_budget_limits(
        '{"gitlab:rest_core:issues": "8", "bad": "nope", "negative": -1}'
    )

    assert limits == {"gitlab:rest_core:issues": 8, "negative": 0}
    assert _parse_budget_limits("not-json") == {}
    assert _parse_budget_limits("[]") == {}


def test_gitlab_budget_estimator_uses_pr_flag_for_mr_expansion() -> None:
    estimates_without_mrs = GitLabBudgetEstimator().estimate(
        _context(dataset_key="work-items", processor_flags={"sync_prs": False})
    )
    estimates_with_mrs = GitLabBudgetEstimator().estimate(
        _context(dataset_key="work-items", processor_flags={"sync_prs": True})
    )

    assert "merge_requests" not in {
        estimate.route_family for estimate in estimates_without_mrs
    }
    assert "merge_requests" in {
        estimate.route_family for estimate in estimates_with_mrs
    }


def test_gitlab_budget_estimator_scopes_bucket_to_host_and_safe_credential_fields() -> (
    None
):
    base_credentials = {
        "token": "secret-token",
        "base_url": "https://gitlab.example.com",
    }
    rotated_token_credentials = {
        "token": "rotated-token",
        "base_url": "https://gitlab.example.com",
    }
    user_credentials = {
        "username": "octavia",
        "private_token": "secret-private-token",
        "base_url": "https://gitlab.example.com",
    }

    base = GitLabBudgetEstimator().estimate(
        _context(dataset_key="commits", credentials=base_credentials)
    )[0]
    rotated = GitLabBudgetEstimator().estimate(
        _context(dataset_key="commits", credentials=rotated_token_credentials)
    )[0]
    user = GitLabBudgetEstimator().estimate(
        _context(dataset_key="commits", credentials=user_credentials)
    )[0]

    assert base.bucket.host == "gitlab.example.com"
    assert base.bucket.credential_fingerprint != rotated.bucket.credential_fingerprint
    assert base.bucket.credential_fingerprint != user.bucket.credential_fingerprint
    assert "secret-token" not in str(base.to_dict())
    assert "rotated-token" not in str(rotated.to_dict())
    assert "secret-private-token" not in str(user.to_dict())


def test_gitlab_budget_estimator_normalizes_camel_case_base_url_for_fingerprint() -> (
    None
):
    snake = GitLabBudgetEstimator().estimate(
        _context(
            dataset_key="commits",
            credentials={"token": "secret-token", "base_url": "https://gitlab.test"},
        )
    )[0]
    camel = GitLabBudgetEstimator().estimate(
        _context(
            dataset_key="commits",
            credentials={"token": "secret-token", "baseUrl": "https://gitlab.test"},
        )
    )[0]

    assert snake.bucket.host == camel.bucket.host == "gitlab.test"
    assert snake.bucket.credential_fingerprint == camel.bucket.credential_fingerprint


def test_gitlab_budget_estimator_scopes_empty_credentials_to_integration() -> None:
    first = GitLabBudgetEstimator().estimate(
        _context(dataset_key="commits", credentials={}, credential_id=None)
    )[0]
    second = GitLabBudgetEstimator().estimate(
        _context(dataset_key="commits", credentials={}, credential_id="credential-2")
    )[0]

    assert first.bucket.credential_fingerprint != second.bucket.credential_fingerprint


def test_gitlab_budget_estimator_returns_empty_for_unknown_dataset() -> None:
    assert GitLabBudgetEstimator().estimate(_context(dataset_key="unknown")) == ()


def test_estimate_provider_budget_delegates_to_gitlab_estimator() -> None:
    estimates = estimate_provider_budget(_context(dataset_key="repo-metadata"))

    assert len(estimates) == 1
    assert estimates[0].bucket.provider == "gitlab"


def test_estimate_provider_budget_returns_empty_for_non_gitlab_provider() -> None:
    estimates = estimate_provider_budget(
        _context(provider="bitbucket", dataset_key="feature-flags")
    )

    assert estimates == ()
