from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationSource,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.providers.github.budget import GitHubBudgetEstimator
from dev_health_ops.providers.gitlab.budget import (
    GITLAB_USAGE_RESOLVER,
    GitLabBudgetEstimator,
)
from dev_health_ops.providers.gitlab.client import GitLabWorkClient
from dev_health_ops.providers.jira.budget import (
    JIRA_USAGE_RESOLVER,
    JiraBudgetEstimator,
)
from dev_health_ops.providers.jira.client import JiraClient
from dev_health_ops.providers.linear.budget import LinearBudgetEstimator
from dev_health_ops.providers.usage import UsageRecorder
from dev_health_ops.sync.budget_guard import BudgetGuard
from dev_health_ops.sync.budget_types import BudgetEstimator
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

NARROW_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
NARROW_END = datetime(2026, 1, 4, tzinfo=timezone.utc)
WIDE_END = datetime(2026, 4, 1, tzinfo=timezone.utc)


def _context(
    *,
    provider: str,
    dataset_key: str,
    window_start: datetime | None = NARROW_START,
    window_end: datetime | None = NARROW_END,
    processor_flags: dict[str, bool] | None = None,
    credentials: Mapping[str, object] | None = None,
) -> SyncTaskContext:
    default_credentials: dict[str, dict[str, object]] = {
        "github": {"token": "secret-token"},
        "gitlab": {"token": "secret-token"},
        "jira": {
            "email": "ops@example.com",
            "api_token": "secret-token",
            "base_url": "https://chaos.atlassian.net",
        },
        "linear": {"api_key": "secret-api-key"},
    }
    return SyncTaskContext(
        unit_id="unit-1",
        sync_run_id="run-1",
        org_id="org-1",
        integration_id="integration-1",
        source_id="source-1",
        source_external_id="source-1",
        provider=provider,
        dataset_key=dataset_key,
        cost_class="medium",
        mode="incremental",
        window_start=window_start,
        window_end=window_end,
        processor_flags=processor_flags or {},
        credential_id="credential-1",
        decrypted_credentials=dict(credentials or default_credentials[provider]),
        db_url="clickhouse://localhost/default",
    )


@pytest.mark.parametrize(
    ("provider", "dataset_key", "estimator"),
    [
        ("linear", "work-items", LinearBudgetEstimator()),
        ("github", "commits", GitHubBudgetEstimator()),
        ("jira", "work-items", JiraBudgetEstimator()),
        ("gitlab", "work-items", GitLabBudgetEstimator()),
    ],
)
def test_budget_estimates_are_monotonic_by_window_span(
    provider: str,
    dataset_key: str,
    estimator: BudgetEstimator,
) -> None:
    narrow = estimator.estimate(_context(provider=provider, dataset_key=dataset_key))
    wide = estimator.estimate(
        _context(
            provider=provider,
            dataset_key=dataset_key,
            window_start=NARROW_START,
            window_end=WIDE_END,
        )
    )

    narrow_by_route = {
        (estimate.bucket.dimension, estimate.route_family): estimate.estimated_units
        for estimate in narrow
    }
    wide_by_route = {
        (estimate.bucket.dimension, estimate.route_family): estimate.estimated_units
        for estimate in wide
    }
    assert wide_by_route.keys() == narrow_by_route.keys()
    assert all(
        wide_by_route[key] >= narrow_by_route[key] for key in narrow_by_route.keys()
    )
    assert sum(wide_by_route.values()) > sum(narrow_by_route.values())


def test_budget_estimate_missing_window_falls_back_to_fixed_floor() -> None:
    no_window = LinearBudgetEstimator().estimate(
        _context(
            provider="linear",
            dataset_key="work-items",
            window_start=None,
            window_end=None,
        )
    )

    assert {
        estimate.route_family: estimate.estimated_units for estimate in no_window
    } == {
        "teams": 1,
        "issues": 5,
        "cycles": 2,
        "comments": 2,
        "attachments": 1,
        "history": 2,
    }


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _seed_linear_run(
    session: Session, before_at: datetime = WIDE_END
) -> tuple[SyncRun, SyncRunUnit]:
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="linear",
        name="linear-demo",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration.id,
        provider="linear",
        source_type="team",
        external_id="CHAOS",
        name="CHAOS",
        full_name="CHAOS",
        metadata_={},
        is_enabled=True,
    )
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=SyncRunMode.BACKFILL.value,
        status=SyncRunStatus.PLANNED.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    session.add_all([source, run])
    session.flush()
    unit = SyncRunUnit(
        org_id=org_id,
        sync_run_id=run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider="linear",
        dataset_key="work-items",
        cost_class="high",
        mode=SyncRunMode.BACKFILL.value,
        since_at=NARROW_START,
        before_at=before_at,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={},
    )
    session.add(unit)
    session.flush()
    return run, unit


def test_linear_backfill_budget_guard_defers_wide_window_unit(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LINEAR_API_KEY", "secret-linear-token")
    monkeypatch.setenv(
        "SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"linear:graphql_cost": 500})
    )
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")
    run, unit = _seed_linear_run(db_session)

    result = BudgetGuard.enforce_run(db_session, str(run.id))

    db_session.refresh(unit)
    assert result.deferred_unit_ids == frozenset({str(unit.id)})
    assert unit.status == SyncRunUnitStatus.RETRYING.value
    assert unit.result is not None
    issue_observation = next(
        observation
        for observation in unit.result["budget_guard"]
        if observation["route_family"] == "issues"
    )
    assert issue_observation["estimated_units"] > 500
    assert issue_observation["bucket"]["provider"] == "linear"


def test_linear_incremental_narrow_window_not_deferred(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LINEAR_API_KEY", "secret-linear-token")
    monkeypatch.setenv(
        "SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"linear:graphql_cost": 500})
    )
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")
    run, unit = _seed_linear_run(db_session, before_at=NARROW_END)

    result = BudgetGuard.enforce_run(db_session, str(run.id))

    db_session.refresh(unit)
    assert result.deferred_unit_ids == frozenset()
    assert unit.status == SyncRunUnitStatus.PLANNED.value


def test_jira_usage_observation_captures_rate_headers_without_tokens() -> None:
    client = JiraClient.__new__(JiraClient)
    client._usage = UsageRecorder(resolver=JIRA_USAGE_RESOLVER)

    client._record_rest_usage(
        "GET /rest/api/3/search/jql",
        headers={
            "RateLimit-Remaining": "42",
            "RateLimit-Reset": "1710000000",
            "Retry-After": "3",
            "Authorization": "Bearer secret-token",
            "X-Request-Id": "req-1",
        },
        status=200,
    )

    observations = client.drain_usage_observations()

    assert observations == [
        {
            "transport": "rest",
            "route_family": "jira_jql",
            "dimension": "search",
            "request_count": 1,
            "example_operation": "GET /rest/api/3/search/jql",
            "latest_status": 200,
            "latest_headers": {
                "ratelimit-remaining": "42",
                "ratelimit-reset": "1710000000",
                "retry-after": "3",
                "x-request-id": "req-1",
            },
            "rate_limit": {
                "remaining": "42",
                "reset": "1710000000",
                "retry_after": "3",
            },
        }
    ]
    assert "secret-token" not in str(observations)


def test_gitlab_usage_observation_captures_rate_headers_without_tokens() -> None:
    client = GitLabWorkClient.__new__(GitLabWorkClient)
    client._usage = UsageRecorder(resolver=GITLAB_USAGE_RESOLVER)

    client._record_rest_usage(
        "GET iterator page",
        headers={
            "RateLimit-Remaining": "17",
            "RateLimit-Reset": "1710000000",
            "Retry-After": "9",
            "PRIVATE-TOKEN": "secret-token",
            "X-Request-Id": "req-2",
        },
        status=429,
    )

    observations = client.drain_usage_observations()

    assert observations == [
        {
            "transport": "rest",
            "route_family": "issues",
            "dimension": "rest_core",
            "request_count": 1,
            "example_operation": "GET iterator page",
            "latest_status": 429,
            "latest_headers": {
                "ratelimit-remaining": "17",
                "ratelimit-reset": "1710000000",
                "retry-after": "9",
                "x-request-id": "req-2",
            },
            "rate_limit": {
                "remaining": "17",
                "reset": "1710000000",
                "retry_after": "9",
            },
        }
    ]
    assert "secret-token" not in str(observations)
