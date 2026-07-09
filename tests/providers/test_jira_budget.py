from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.providers.jira.budget import JiraBudgetEstimator
from dev_health_ops.sync.budget import BudgetDimension, estimate_provider_budget
from dev_health_ops.sync.budget_guard import BudgetGuard, _budget_key, _limit_for_bucket
from dev_health_ops.sync.dispatch_policy import route
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

WINDOW_START = datetime(2026, 1, 10, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 1, 12, tzinfo=timezone.utc)


def _context(
    *,
    provider: str = "jira",
    dataset_key: str,
    processor_flags: dict[str, bool] | None = None,
    credentials: Mapping[str, object] | None = None,
    credential_id: str | None = "credential-1",
) -> SyncTaskContext:
    decrypted_credentials: dict[str, object] = {
        "email": "ops@example.com",
        "api_token": "secret-token",
        "base_url": "https://chaos.atlassian.net",
    }
    if credentials is not None:
        decrypted_credentials = {str(key): value for key, value in credentials.items()}
    return SyncTaskContext(
        unit_id="unit-1",
        sync_run_id="run-1",
        org_id="org-1",
        integration_id="integration-1",
        source_id="source-1",
        source_external_id="CHAOS",
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


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@contextmanager
def _fake_session_ctx(session):
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()


def _patch_db_session(monkeypatch, session) -> None:
    import dev_health_ops.db as db

    session.commit()
    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(session)
    )


def _seed_jira_run(
    session,
    *,
    dataset_key: str = "work-items",
    processor_flags: dict[str, bool] | None = None,
):
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="jira",
        name="jira-demo",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration.id,
        provider="jira",
        source_type="project",
        external_id="CHAOS",
        name="CHAOS",
        full_name="CHAOS",
        metadata_={},
        is_enabled=True,
    )
    dataset = IntegrationDataset(
        org_id=org_id,
        integration_id=integration.id,
        dataset_key=dataset_key,
        is_enabled=True,
        options={},
    )
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.PLANNED.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    session.add_all([source, dataset, run])
    session.flush()
    unit = SyncRunUnit(
        org_id=org_id,
        sync_run_id=run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider="jira",
        dataset_key=dataset_key,
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        since_at=WINDOW_START,
        before_at=WINDOW_END,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags=processor_flags or {},
    )
    session.add(unit)
    session.flush()
    return run, unit


def _jira_env(monkeypatch) -> None:
    monkeypatch.setenv("JIRA_BASE_URL", "https://chaos.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "ops@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "secret-token")


def test_jira_budget_estimator_returns_jql_and_enrichment_budgets() -> None:
    estimates = JiraBudgetEstimator().estimate(_context(dataset_key="work-items"))

    assert [
        (estimate.bucket.dimension, estimate.route_family) for estimate in estimates
    ] == [
        (BudgetDimension.SEARCH, "jira_jql"),
        (BudgetDimension.REST_CORE, "jira_issue_enrichment"),
    ]
    assert estimates[0].bucket.provider == "jira"
    assert estimates[0].bucket.org_id == "org-1"
    assert estimates[0].bucket.host == "chaos.atlassian.net"
    assert estimates[0].estimated_units == 4
    assert estimates[0].confidence == "medium"
    assert estimates[0].to_dict()["bucket"]["dimension"] == "search"


def test_jira_budget_estimator_adds_optional_worklog_and_gql_budgets() -> None:
    estimates = JiraBudgetEstimator().estimate(
        _context(
            dataset_key="work-items",
            processor_flags={
                "jira_fetch_worklogs": True,
                "atlassian_gql_enabled": True,
            },
        )
    )

    assert {estimate.route_family for estimate in estimates} == {
        "jira_jql",
        "jira_issue_enrichment",
        "jira_worklogs",
        "jira_gql_enrichment",
    }
    assert {estimate.bucket.dimension for estimate in estimates} == {
        BudgetDimension.SEARCH,
        BudgetDimension.REST_CORE,
        BudgetDimension.GRAPHQL_COST,
    }


def test_jira_budget_estimator_reads_worklog_and_gql_env_flags(monkeypatch) -> None:
    monkeypatch.setenv("JIRA_FETCH_WORKLOGS", "true")
    monkeypatch.setenv("ATLASSIAN_GQL_ENABLED", "true")

    estimates = JiraBudgetEstimator().estimate(_context(dataset_key="work-items"))

    assert "jira_worklogs" in {estimate.route_family for estimate in estimates}
    assert "jira_gql_enrichment" in {estimate.route_family for estimate in estimates}


def test_jira_budget_route_family_limits_override_dimension_defaults() -> None:
    jql = next(
        estimate
        for estimate in JiraBudgetEstimator().estimate(
            _context(dataset_key="work-items")
        )
        if estimate.route_family == "jira_jql"
    )
    limits = {"jira:search:jira_jql": 3, "jira:search": 1}

    assert _budget_key(jql.bucket.to_dict(), route_family=jql.route_family).endswith(
        ":search:jira_jql"
    )
    assert (
        _limit_for_bucket(
            jql.bucket.to_dict(),
            route_family=jql.route_family,
            limits=limits,
            default_limit=100,
        )
        == 3
    )


def test_jira_budget_estimator_scopes_bucket_to_host_and_safe_credentials() -> None:
    base = JiraBudgetEstimator().estimate(
        _context(
            dataset_key="work-items",
            credentials={
                "email": "ops@example.com",
                "api_token": "secret-token",
                "base_url": "chaos.atlassian.net",
            },
        )
    )[0]
    rotated = JiraBudgetEstimator().estimate(
        _context(
            dataset_key="work-items",
            credentials={
                "email": "ops@example.com",
                "api_token": "rotated-token",
                "base_url": "chaos.atlassian.net",
            },
        )
    )[0]

    assert base.bucket.host == "chaos.atlassian.net"
    assert base.bucket.credential_fingerprint != rotated.bucket.credential_fingerprint
    assert "secret-token" not in str(base.to_dict())
    assert "rotated-token" not in str(rotated.to_dict())


def test_estimate_provider_budget_delegates_to_jira_estimator() -> None:
    estimates = estimate_provider_budget(_context(dataset_key="work-items"))

    assert len(estimates) == 2
    assert estimates[0].bucket.provider == "jira"


def test_jira_budget_estimator_returns_empty_for_unknown_dataset() -> None:
    assert JiraBudgetEstimator().estimate(_context(dataset_key="commits")) == ()


def test_jira_budget_guard_defers_second_unit_by_jql_reservation(
    db_session, monkeypatch
):
    _jira_env(monkeypatch)
    run, first = _seed_jira_run(db_session)
    second = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=first.integration_id,
        source_id=first.source_id,
        provider="jira",
        dataset_key="work-items",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        since_at=WINDOW_START,
        before_at=WINDOW_END,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={},
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()
    monkeypatch.setenv(
        "SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"jira:search:jira_jql": 4})
    )
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    result = BudgetGuard.enforce_run(db_session, str(run.id))

    db_session.refresh(first)
    db_session.refresh(second)
    assert len(result.deferred_unit_ids) == 1
    statuses = {first.status, second.status}
    assert statuses == {
        SyncRunUnitStatus.PLANNED.value,
        SyncRunUnitStatus.RETRYING.value,
    }
    deferred = first if first.status == SyncRunUnitStatus.RETRYING.value else second
    assert deferred.available_at is not None
    assert deferred.result is not None
    assert deferred.result["budget_guard"][0]["bucket"]["provider"] == "jira"
    assert deferred.result["budget_guard"][0]["route_family"] == "jira_jql"


def test_jira_budget_guard_active_reservation_blocks_planned_unit(
    db_session, monkeypatch
):
    _jira_env(monkeypatch)
    run, planned = _seed_jira_run(db_session)
    active = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=planned.integration_id,
        source_id=planned.source_id,
        provider="jira",
        dataset_key="work-items",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        since_at=WINDOW_START,
        before_at=WINDOW_END,
        status=SyncRunUnitStatus.DISPATCHING.value,
        attempts=0,
        processor_flags={},
    )
    run.total_units = 2
    db_session.add(active)
    db_session.flush()
    active.updated_at = datetime.now(timezone.utc)
    monkeypatch.setenv(
        "SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"jira:search:jira_jql": 4})
    )
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    result = BudgetGuard.enforce_run(db_session, str(run.id))

    db_session.refresh(planned)
    db_session.refresh(active)
    assert result.deferred_unit_ids == frozenset({str(planned.id)})
    assert planned.status == SyncRunUnitStatus.RETRYING.value
    assert active.status == SyncRunUnitStatus.DISPATCHING.value


def test_jira_budget_estimate_is_persisted_on_success(db_session, monkeypatch):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units

    _jira_env(monkeypatch)
    run, unit = _seed_jira_run(db_session)
    unit.status = SyncRunUnitStatus.DISPATCHING.value
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        dataset_adapters, "run_dataset_unit", lambda ctx, runtime: {"ok": True}
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run, "apply_async", lambda *args, **kwargs: None
    )

    class RuntimeCache:
        def get(self, context):
            return None

    monkeypatch.setattr(sync_units, "_runtime_cache", RuntimeCache())

    result = getattr(sync_units.run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result["status"] == "success"
    assert unit.result is not None
    budget_estimate = unit.result["observations"]["budget_estimate"]
    assert budget_estimate[0]["bucket"]["provider"] == "jira"
    assert budget_estimate[0]["route_family"] == "jira_jql"


def test_jira_provider_queue_routing_supports_provider_and_cost_class_queues(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
    monkeypatch.delenv("SYNC_COST_CLASS_QUEUES", raising=False)
    provider_route = route(
        org_id="org-1",
        provider="jira",
        cost_class="medium",
        cost_class_queues_enabled=False,
    )

    monkeypatch.setenv("SYNC_COST_CLASS_QUEUES", "true")
    cost_route = route(
        org_id="org-1",
        provider="jira",
        cost_class="medium",
        cost_class_queues_enabled=True,
    )

    assert provider_route.queue == "sync.jira"
    assert cost_route.queue == "sync.jira.medium"
