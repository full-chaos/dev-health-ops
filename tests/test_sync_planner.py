from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

from dev_health_ops.db import normalize_sync_postgres_uri
from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationCredential,
    IntegrationDataset,
    IntegrationSource,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
    SyncWatermark,
)
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.models.users import Organization
from dev_health_ops.sync import planner
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISCOVERY,
    OUTBOX_STATUS_PENDING,
)
from dev_health_ops.sync.planner import BackfillSelector, SyncPlanRequest, plan_sync_run
from dev_health_ops.sync.watermarks import get_watermark, set_watermark

_POSTGRES_TEST_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
ORG_ID = "planner-org"


def _require_postgres_test_uri() -> None:
    if os.getenv(_POSTGRES_TEST_URI_ENV):
        return
    if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
        pytest.fail(f"{_POSTGRES_TEST_URI_ENV} must be configured for PostgreSQL tests")
    pytest.skip(f"requires {_POSTGRES_TEST_URI_ENV}")


@pytest.fixture(scope="module")
def require_postgres_test_uri() -> None:
    _require_postgres_test_uri()


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _create_integration(session: Session, provider: str = "github") -> Integration:
    integration = Integration(
        org_id=ORG_ID,
        provider=provider,
        name=f"{provider.title()} integration",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    return integration


def _create_source(
    session: Session,
    integration: Integration,
    *,
    external_id: str,
    provider: str | None = None,
    is_enabled: bool = True,
) -> IntegrationSource:
    source_provider = provider or integration.provider
    source = IntegrationSource(
        org_id=ORG_ID,
        integration_id=integration.id,
        provider=source_provider,
        source_type="repo",
        external_id=external_id,
        name=external_id.rsplit("/", 1)[-1],
        full_name=external_id,
        metadata_={},
        is_enabled=is_enabled,
        discovered_at=datetime.now(timezone.utc),
        last_seen_at=datetime.now(timezone.utc),
    )
    session.add(source)
    session.flush()
    return source


def _create_dataset(
    session: Session,
    integration: Integration,
    dataset_key: str,
    *,
    is_enabled: bool = True,
) -> IntegrationDataset:
    dataset = IntegrationDataset(
        org_id=ORG_ID,
        integration_id=integration.id,
        dataset_key=dataset_key,
        is_enabled=is_enabled,
        options={},
    )
    session.add(dataset)
    session.flush()
    return dataset


def _planned_units(session: Session, plan_sync_run_id: str) -> list[SyncRunUnit]:
    return (
        session.query(SyncRunUnit)
        .filter(SyncRunUnit.sync_run_id == plan_sync_run_id)
        .order_by(SyncRunUnit.provider, SyncRunUnit.dataset_key, SyncRunUnit.source_id)
        .all()
    )


def test_enabled_sources_and_enabled_datasets_fan_out_to_units(db_session):
    integration = _create_integration(db_session)
    sources = [
        _create_source(db_session, integration, external_id="full-chaos/dev-health"),
        _create_source(
            db_session, integration, external_id="full-chaos/dev-health-web"
        ),
    ]
    _create_dataset(db_session, integration, "commits")
    _create_dataset(db_session, integration, "prs")

    # NOTE: do NOT pass a hardcoded ``before`` here. These datasets cold-start at
    # ``now - initial_sync_depth``; a fixed calendar date drifts into the past and
    # eventually describes an INVERTED window (start > end), which now correctly
    # plans zero units (CHAOS-3412). This test is about fan-out, not windows.
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    units = _planned_units(db_session, plan.sync_run_id)
    outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=plan.sync_run_id, kind=OUTBOX_KIND_DISCOVERY)
        .one()
    )
    discovery = (
        db_session.query(SyncRunReferenceDiscovery)
        .filter_by(sync_run_id=plan.sync_run_id)
        .one()
    )

    assert plan.total_units == 4
    assert len(plan.unit_ids) == 4
    assert sync_run is not None
    assert sync_run.status == SyncRunStatus.PLANNED.value
    assert sync_run.total_units == 4
    assert {(str(unit.source_id), unit.dataset_key) for unit in units} == {
        (str(source.id), dataset_key)
        for source in sources
        for dataset_key in ("commits", "prs")
    }
    assert {unit.status for unit in units} == {SyncRunUnitStatus.PLANNED.value}
    assert {unit.mode for unit in units} == {SyncRunMode.INCREMENTAL.value}
    assert outbox.status == OUTBOX_STATUS_PENDING
    assert outbox.claim_token is None
    assert discovery.status == "planned"
    assert discovery.org_id == ORG_ID


def test_plan_sync_run_rejects_plan_over_unit_cap(db_session, monkeypatch):
    """An oversized plan is rejected BEFORE anything is persisted.

    Without the plan-time cap check the run + all its units are persisted,
    DispatchGuard hard-denies asynchronously, and the caller only learns
    from a FAILED run after the 202 (the '1872/1000' backfill bug).
    """
    from dev_health_ops.sync.planner import SyncPlanUnitCapExceededError

    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "3")
    integration = _create_integration(db_session)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_source(db_session, integration, external_id="full-chaos/dev-health-web")
    _create_dataset(db_session, integration, "commits")
    _create_dataset(db_session, integration, "prs")

    # No hardcoded ``before`` — see the note in the fan-out test above: a fixed
    # past date makes these cold-start windows inverted, which now plans zero
    # units and would make this cap assertion vacuous.
    with pytest.raises(SyncPlanUnitCapExceededError) as excinfo:
        plan_sync_run(
            db_session,
            SyncPlanRequest(
                integration_id=str(integration.id),
                org_id=ORG_ID,
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="manual",
            ),
        )

    assert excinfo.value.planned_units == 4
    assert excinfo.value.total_cap == 3
    assert "4/3" in str(excinfo.value)
    # Fail-fast means NOTHING was persisted: no run, no units, no outbox row.
    assert db_session.query(SyncRun).count() == 0
    assert db_session.query(SyncRunUnit).count() == 0
    assert db_session.query(SyncDispatchOutbox).count() == 0
    assert db_session.query(SyncRunReferenceDiscovery).count() == 0


def test_planner_stamps_single_credential_per_run(db_session):
    """CHAOS-2755: plan_sync_run stamps credential_id + fingerprint + auth_source
    ONCE on the SyncRun, and neither PlannedUnit nor SyncRunUnit gains a
    credential field (credentials are auth state, never dispatch capacity)."""
    import dataclasses

    from dev_health_ops.credentials.fingerprint import AUTH_SOURCE_ENVIRONMENT
    from dev_health_ops.sync.planner import PlannedUnit

    integration = _create_integration(db_session)  # env auth (credential_id=None)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_source(db_session, integration, external_id="full-chaos/dev-health-web")
    _create_dataset(db_session, integration, "commits")
    _create_dataset(db_session, integration, "prs")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    assert sync_run is not None
    # Stamped exactly once, on the run.
    assert sync_run.auth_source == AUTH_SOURCE_ENVIRONMENT
    assert sync_run.credential_id is None  # environment auth
    assert isinstance(sync_run.credential_fingerprint, str)
    assert len(sync_run.credential_fingerprint) == 64  # sha256 hex digest

    # The run-level columns exist ONLY on sync_runs, never on sync_run_units.
    unit_columns = set(SyncRunUnit.__table__.columns.keys())
    assert "credential_id" not in unit_columns
    assert "credential_fingerprint" not in unit_columns
    assert "auth_source" not in unit_columns

    # PlannedUnit likewise carries no credential field.
    planned_fields = {f.name for f in dataclasses.fields(PlannedUnit)}
    assert not any("credential" in name for name in planned_fields)


def test_unsupported_provider_dataset_pairs_are_skipped(db_session):
    integration = _create_integration(db_session, provider="jira")
    _create_source(db_session, integration, external_id="jira-project", provider="jira")
    _create_dataset(db_session, integration, "commits")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    assert plan.total_units == 0
    assert plan.unit_ids == ()
    assert sync_run is not None
    assert sync_run.total_units == 0
    assert _planned_units(db_session, plan.sync_run_id) == []


def test_backfill_creates_one_unit_per_source_dataset_window(db_session):
    integration = _create_integration(db_session)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_source(db_session, integration, external_id="full-chaos/dev-health-web")
    _create_dataset(db_session, integration, "commits")
    _create_dataset(db_session, integration, "prs")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="manual",
            since=datetime(2026, 6, 1, tzinfo=timezone.utc),
            before=datetime(2026, 6, 14, 23, 59, tzinfo=timezone.utc),
        ),
    )

    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    units = _planned_units(db_session, plan.sync_run_id)

    assert plan.total_units == 8
    assert sync_run is not None
    assert sync_run.total_units == 8
    assert len(units) == 8
    assert {unit.mode for unit in units} == {SyncRunMode.BACKFILL.value}

    windows = set()
    for unit in units:
        assert unit.since_at is not None
        assert unit.before_at is not None
        windows.add((unit.since_at.date(), unit.before_at.date()))

    assert windows == {
        (datetime(2026, 6, 1).date(), datetime(2026, 6, 7).date()),
        (datetime(2026, 6, 8).date(), datetime(2026, 6, 14).date()),
    }


def test_backfill_selector_object_collapses_family_and_preserves_source_order(
    db_session,
):
    integration = _create_integration(db_session, provider="linear")
    source_z = _create_source(db_session, integration, external_id="full-chaos/z-repo")
    source_a = _create_source(db_session, integration, external_id="full-chaos/a-repo")
    _create_dataset(db_session, integration, "work-item-comments")
    _create_dataset(db_session, integration, "work-item-labels")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="manual",
            backfill_selector=BackfillSelector(
                since=datetime(2026, 6, 1, tzinfo=timezone.utc),
                before=datetime(2026, 6, 2, tzinfo=timezone.utc),
                source_ids=(str(source_z.id), str(source_a.id)),
                dataset_keys=("work-item-labels", "work-item-comments"),
            ),
        ),
    )

    units = [
        db_session.get(SyncRunUnit, uuid.UUID(unit_id)) for unit_id in plan.unit_ids
    ]

    assert plan.total_units == 2
    assert all(unit is not None for unit in units)
    assert [str(unit.source_id) for unit in units if unit is not None] == [
        str(source_a.id),
        str(source_z.id),
    ]
    assert {unit.dataset_key for unit in units if unit is not None} == {"work-items"}
    assert {unit.mode for unit in units if unit is not None} == {
        SyncRunMode.BACKFILL.value
    }


def test_backfill_selector_object_rejects_legacy_flat_fields(db_session):
    integration = _create_integration(db_session)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")

    with pytest.raises(ValueError, match="backfill selector cannot be mixed"):
        plan_sync_run(
            db_session,
            SyncPlanRequest(
                integration_id=str(integration.id),
                org_id=ORG_ID,
                mode=SyncRunMode.BACKFILL.value,
                triggered_by="manual",
                backfill_selector=BackfillSelector(
                    since=datetime(2026, 6, 1, tzinfo=timezone.utc),
                    before=datetime(2026, 6, 2, tzinfo=timezone.utc),
                ),
                source_ids=(str(uuid.uuid4()),),
            ),
        )


def test_disabled_source_produces_zero_units_without_hydrating_credentials(
    db_session, monkeypatch: pytest.MonkeyPatch
):
    integration = _create_integration(db_session)
    _create_source(
        db_session,
        integration,
        external_id="full-chaos/dev-health",
        is_enabled=False,
    )
    _create_dataset(db_session, integration, "commits")
    monkeypatch.setattr(
        planner,
        "_resolve_credential_stamp",
        lambda *_args: pytest.fail("zero-unit plan hydrated credentials"),
    )

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    assert plan.total_units == 0
    assert sync_run is not None
    assert sync_run.total_units == 0
    assert _planned_units(db_session, plan.sync_run_id) == []


def test_disabled_dataset_produces_zero_units_without_hydrating_credentials(
    db_session, monkeypatch: pytest.MonkeyPatch
):
    integration = _create_integration(db_session)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits", is_enabled=False)
    monkeypatch.setattr(
        planner,
        "_resolve_credential_stamp",
        lambda *_args: pytest.fail("zero-unit plan hydrated credentials"),
    )

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    assert plan.total_units == 0
    assert sync_run is not None
    assert sync_run.total_units == 0
    assert _planned_units(db_session, plan.sync_run_id) == []


@pytest.mark.parametrize(
    ("existing_enabled", "expected_units"),
    [(None, 1), (False, 0)],
    ids=["missing-row-is-enabled", "disabled-row-stays-disabled"],
)
@pytest.mark.parametrize("provider", ["github", "gitlab"])
def test_requested_tests_dataset_reconciles_missing_row_without_overriding_disabled(
    db_session,
    provider: str,
    existing_enabled: bool | None,
    expected_units: int,
):
    # Given: an existing code-host integration with a source and either no tests
    # row or an explicitly disabled tests row.
    integration = _create_integration(db_session, provider)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    if existing_enabled is not None:
        _create_dataset(
            db_session,
            integration,
            "tests",
            is_enabled=existing_enabled,
        )

    # When: the planner is asked to run the newly supported tests dataset.
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            dataset_keys=("tests",),
        ),
    )

    # Then: a missing row becomes enabled and executable, while an explicit
    # disabled row remains disabled and produces no unit.
    dataset = (
        db_session.query(IntegrationDataset)
        .filter_by(integration_id=integration.id, dataset_key="tests")
        .one_or_none()
    )
    assert dataset is not None
    assert dataset.is_enabled is (existing_enabled is not False)
    assert plan.total_units == expected_units
    assert {
        unit.dataset_key for unit in _planned_units(db_session, plan.sync_run_id)
    } == ({"tests"} if expected_units else set())


@pytest.mark.parametrize("provider", ["github", "gitlab"])
def test_unrequested_tests_dataset_is_not_reconciled(db_session, provider: str):
    # Given: an existing code-host integration with no persisted dataset rows.
    integration = _create_integration(db_session, provider)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")

    # When: the planner receives no explicit dataset selection.
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    # Then: tests remains opt-in and no dataset row or unit is created.
    assert (
        db_session.query(IntegrationDataset)
        .filter_by(integration_id=integration.id, dataset_key="tests")
        .one_or_none()
        is None
    )
    assert plan.total_units == 0
    assert _planned_units(db_session, plan.sync_run_id) == []


def test_insert_dataset_if_missing_preserves_concurrent_disabled_row(db_session):
    # Given: another transaction has already inserted the requested dataset as disabled.
    integration = _create_integration(db_session)
    existing = _create_dataset(db_session, integration, "tests", is_enabled=False)

    # When: stale planner state attempts the same insert.
    planner._insert_dataset_if_missing(
        db_session,
        IntegrationDataset(
            org_id=integration.org_id,
            integration_id=integration.id,
            dataset_key="tests",
            is_enabled=True,
            options={},
        ),
    )

    # Then: the uniqueness conflict is isolated and the disabled row is unchanged.
    datasets = (
        db_session.query(IntegrationDataset)
        .filter_by(integration_id=integration.id, dataset_key="tests")
        .all()
    )
    assert [dataset.id for dataset in datasets] == [existing.id]
    assert datasets[0].is_enabled is False


# ---------------------------------------------------------------------------
# CHAOS-3400: security is opt-in, matching every other dataset. No scheduled
# code-host sync may silently enable it; existing auto-enabled rows are left
# exactly as they were (reversible via the dataset PATCH endpoint), never
# rewritten by the planner.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider", ["github", "gitlab"])
def test_scheduled_code_host_sync_does_not_auto_enable_security(
    db_session, provider: str
):
    # Given: a code-host integration configured the way CHAOS-3400 found in
    # production -- several datasets explicitly selected, security never
    # selected by the operator.
    integration = _create_integration(db_session, provider)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    for dataset_key in ("commits", "prs", "cicd", "deployments"):
        _create_dataset(db_session, integration, dataset_key)

    # When: a normal scheduled sync runs. dataset_keys=None is exactly how a
    # planner-managed parent config's scheduled trigger calls in (see
    # trigger_routing.plan_request_for_config): "all enabled" datasets.
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="schedule",
        ),
    )

    # Then: security was never selected, so scheduling must not silently
    # enable it or plan units for it. THIS IS THE LOAD-BEARING ASSERTION --
    # it must fail against pre-fix `main`, where the scheduled trigger
    # unconditionally inserted an enabled `security` IntegrationDataset row.
    assert (
        db_session.query(IntegrationDataset)
        .filter_by(integration_id=integration.id, dataset_key="security")
        .one_or_none()
        is None
    )
    planned_keys = {
        unit.dataset_key for unit in _planned_units(db_session, plan.sync_run_id)
    }
    assert "security" not in planned_keys
    assert planned_keys == {"commits", "prs", "cicd", "deployments"}


@pytest.mark.parametrize("provider", ["github", "gitlab"])
@pytest.mark.parametrize("triggered_by", ["schedule", "manual"])
def test_security_plans_when_explicitly_selected(
    db_session, provider: str, triggered_by: str
):
    # Given: a code-host integration with no dataset rows yet.
    integration = _create_integration(db_session, provider)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")

    # When: the caller explicitly asks for security, regardless of trigger --
    # the positive-path counterpart to the negative control above. Explicit
    # selection must keep working the same way every other dataset does.
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by=triggered_by,
            dataset_keys=("security",),
        ),
    )

    # Then: the dataset row is created enabled, with no special marker (it
    # was operator-requested, not auto-enabled), and its unit plans/succeeds.
    dataset = (
        db_session.query(IntegrationDataset)
        .filter_by(integration_id=integration.id, dataset_key="security")
        .one()
    )
    assert dataset.is_enabled is True
    assert dataset.options == {}
    assert plan.total_units == 1
    assert {
        unit.dataset_key for unit in _planned_units(db_session, plan.sync_run_id)
    } == {"security"}


@pytest.mark.parametrize(
    ("is_enabled", "expect_security_unit"),
    [(True, True), (False, False)],
    ids=["preexisting-auto-enabled-row-keeps-running", "disabled-row-stays-disabled"],
)
def test_preexisting_auto_enabled_security_row_is_left_alone(
    db_session, is_enabled: bool, expect_security_unit: bool
):
    # Given: a `security` row carrying the historical marker stamped by the
    # now-removed scheduled auto-enable path (CHAOS-3400 migration decision:
    # leave existing rows exactly as they are -- no migration disables them).
    integration = _create_integration(db_session, "github")
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    existing = IntegrationDataset(
        org_id=ORG_ID,
        integration_id=integration.id,
        dataset_key="security",
        is_enabled=is_enabled,
        options={"auto_enabled_by": "scheduled_code_host_sync"},
    )
    db_session.add(existing)
    db_session.flush()

    # When: a normal scheduled sync runs with no explicit dataset selection.
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="schedule",
        ),
    )

    # Then: the planner never rewrites this row -- its enabled state and its
    # `auto_enabled_by` marker (the UI's signal to render the one-click
    # disable affordance) are untouched either way, and its enabled state
    # alone controls whether a unit plans.
    dataset = db_session.get(IntegrationDataset, existing.id)
    assert dataset is not None
    assert dataset.is_enabled is is_enabled
    assert dataset.options == {"auto_enabled_by": "scheduled_code_host_sync"}
    planned_keys = {
        unit.dataset_key for unit in _planned_units(db_session, plan.sync_run_id)
    }
    assert ("security" in planned_keys) is expect_security_unit


def test_incremental_window_starts_at_dataset_watermark(db_session):
    integration = _create_integration(db_session)
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, "prs")
    watermark = datetime(2026, 6, 10, 9, 30, tzinfo=timezone.utc)
    set_watermark(db_session, ORG_ID, source.external_id, "prs", watermark)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        ),
    )

    unit = _planned_units(db_session, plan.sync_run_id)[0]
    assert unit.since_at is not None
    assert unit.since_at.replace(tzinfo=timezone.utc) == watermark


def test_planner_rejects_cross_org_integration(db_session):
    integration = _create_integration(db_session)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")
    with pytest.raises(ValueError):
        plan_sync_run(
            db_session,
            SyncPlanRequest(
                integration_id=str(integration.id),
                org_id="someone-elses-org",
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="manual",
            ),
        )


def test_planned_units_persist_isolated_processor_flags(db_session):
    integration = _create_integration(db_session)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "prs")
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )
    unit = _planned_units(db_session, plan.sync_run_id)[0]
    # prs unit must persist explicit flags and must NOT over-fetch unrelated datasets
    assert unit.processor_flags
    assert unit.processor_flags.get("sync_security", False) is False
    assert unit.processor_flags.get("sync_deployments", False) is False
    assert unit.processor_flags.get("sync_incidents", False) is False


# ---------------------------------------------------------------------------
# WS-A tests: cold-start depth + full_resync (CHAOS-2569)
# ---------------------------------------------------------------------------


def test_incremental_cold_start_uses_initial_sync_depth(db_session):
    """No watermark row → window_start == now - depth (±2s).

    Covers both a work-item dataset (prs) and a code dataset (commits).
    """
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")
    _create_dataset(db_session, integration, "prs")

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 2
    expected_start = now - timedelta(days=30)
    for unit in units:
        assert unit.since_at is not None
        since = unit.since_at.replace(tzinfo=timezone.utc)
        assert abs((since - expected_start).total_seconds()) < 2


def test_full_resync_uses_configured_depth(db_session):
    """full_resync mode -> window_start == now - depth, not None."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 14}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.FULL_RESYNC.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    unit = units[0]
    assert unit.since_at is not None, "full_resync must not produce a None window_start"
    expected_start = now - timedelta(days=14)
    since = unit.since_at.replace(tzinfo=timezone.utc)
    assert abs((since - expected_start).total_seconds()) < 2


def test_dataset_option_overrides_integration_depth(db_session):
    """Dataset options.initial_sync_depth wins over integration config."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    dataset = _create_dataset(db_session, integration, "commits")
    dataset.options = {"initial_sync_depth": 7}
    db_session.flush()

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None
    expected_start = now - timedelta(days=7)
    since = units[0].since_at.replace(tzinfo=timezone.utc)
    assert abs((since - expected_start).total_seconds()) < 2


def test_planner_durably_snapshots_legacy_github_work_item_env(db_session, monkeypatch):
    monkeypatch.setenv("GITHUB_FETCH_COMMENTS", "false")
    monkeypatch.setenv("GITHUB_FETCH_MILESTONES", "false")
    monkeypatch.setenv("GITHUB_COMMENTS_LIMIT", "37")
    integration = _create_integration(db_session, provider="github")
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    dataset = _create_dataset(db_session, integration, "work-items")
    dataset.options = {"unrelated": "preserve"}
    db_session.flush()

    plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    assert dataset.options == {
        "unrelated": "preserve",
        "fetch_comments": False,
        "fetch_milestones": False,
        "comments_limit": 37,
    }


def test_existing_watermark_incremental_unchanged(db_session):
    """With a watermark row, since_at == watermark (regression guard)."""
    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, "commits")
    watermark = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    set_watermark(db_session, ORG_ID, source.external_id, "commits", watermark)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    unit = units[0]
    assert unit.since_at is not None
    assert unit.since_at.replace(tzinfo=timezone.utc) == watermark


# ---------------------------------------------------------------------------
# Finding #1 regression: WatermarkBehavior.NONE datasets keep since_at=None
# ---------------------------------------------------------------------------


def test_none_watermark_behavior_incremental_keeps_since_at_none(db_session):
    """NONE-behavior datasets (repo-metadata only) must keep since_at=None on
    incremental — cold-start depth must NOT be applied.
    """
    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    # repo-metadata is the only remaining WatermarkBehavior.NONE dataset
    _create_dataset(db_session, integration, "repo-metadata")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is None, (
        "NONE-behavior dataset (repo-metadata) must keep since_at=None on incremental, "
        "not receive a cold-start depth window"
    )


# ---------------------------------------------------------------------------
# Tier cap: non-UUID org_id, unlimited tier, and real missing-table path
# ---------------------------------------------------------------------------


def test_tier_cap_non_uuid_org_id_returns_community_default(db_session):
    """Non-UUID org_id (e.g. test fixtures) returns community default (30).

    This is a defensive guard in _get_tier_backfill_days_cap for callers that
    pass string org_ids; it does NOT represent the missing-table path.
    """
    from dev_health_ops.sync.planner import (
        _DEFAULT_INITIAL_SYNC_DEPTH_DAYS,
        _get_tier_backfill_days_cap,
    )

    cap = _get_tier_backfill_days_cap(db_session, "not-a-uuid")
    assert cap == _DEFAULT_INITIAL_SYNC_DEPTH_DAYS


def test_tier_cap_unlimited_tier_does_not_cap_depth(db_session, monkeypatch):
    """get_limit returning None (unlimited/enterprise tier) must NOT cap depth.

    An enterprise org with initial_sync_depth=90 must plan 90 days, not 30.
    """
    import uuid as _uuid

    from dev_health_ops.sync.planner import _get_tier_backfill_days_cap

    monkeypatch.setattr(
        "dev_health_ops.api.services.licensing.TierLimitService.get_limit",
        lambda self, org_id, key: None,
    )

    cap = _get_tier_backfill_days_cap(db_session, str(_uuid.uuid4()))
    assert cap is None, "Unlimited tier must return None (no cap), not a default value"


def test_tier_limit_service_returns_empty_on_missing_table():
    """A real missing-table query rolls back only its SQLite SAVEPOINT.

    SQLite's driver does not provide reliable SAVEPOINT semantics unless
    SQLAlchemy takes over transaction control. Configure that production-like
    contract explicitly, then prove the tier_limits query—not SAVEPOINT setup—
    is what fails and that the outer session remains usable afterward.
    """
    from dev_health_ops.api.services.licensing import TierLimitService
    from dev_health_ops.licensing.types import LicenseTier

    engine = create_engine("sqlite:///:memory:")
    statements: list[str] = []
    errors: list[str] = []

    @event.listens_for(engine, "connect")
    def _disable_sqlite_driver_transaction_control(dbapi_connection, _record):
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, "begin")
    def _emit_explicit_begin(connection):
        connection.exec_driver_sql("BEGIN")

    @event.listens_for(engine, "before_cursor_execute")
    def _record_statement(
        _connection,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ):
        statements.append(statement)

    @event.listens_for(engine, "handle_error")
    def _record_error(exception_context):
        errors.append(str(exception_context.original_exception))

    with Session(engine) as session:
        svc = TierLimitService(session)
        result = svc._get_db_tier_limits(LicenseTier.COMMUNITY.value)
        assert result == {}, (
            "Missing tier_limits must return empty dict (use hardcoded defaults)"
        )

        mechanism = []
        for statement in statements:
            normalized = " ".join(statement.upper().split())
            if normalized.startswith("SAVEPOINT "):
                mechanism.append("savepoint")
            elif " FROM TIER_LIMITS " in f" {normalized} ":
                mechanism.append("tier_limits query")
            elif normalized.startswith("ROLLBACK TO SAVEPOINT "):
                mechanism.append("savepoint rollback")
        assert mechanism == [
            "savepoint",
            "tier_limits query",
            "savepoint rollback",
        ]
        assert errors == ["no such table: tier_limits"]
        assert session.execute(text("SELECT 1")).scalar_one() == 1

    engine.dispose()


def test_plan_sync_run_succeeds_when_tier_limits_unavailable(db_session, monkeypatch):
    """plan_sync_run must succeed and flush SyncRun when tier cap lookup fails.

    Monkeypatches _get_db_tier_limits to raise OperationalError (same shape as
    a missing tier_limits table) so we can verify the planner session stays
    usable end-to-end without needing to replicate the full licensing schema.
    Asserts (a) SyncRun flushes (session NOT poisoned) and (b) depth falls
    back to the community hardcoded default (30).
    """
    from datetime import timedelta

    from sqlalchemy.exc import OperationalError

    def _raise_op_error(self, tier):
        raise OperationalError("no such table: tier_limits", None, Exception())

    monkeypatch.setattr(
        "dev_health_ops.api.services.licensing.TierLimitService._get_db_tier_limits",
        _raise_op_error,
    )

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 90}
    db_session.flush()
    _create_source(db_session, integration, external_id="owner/repo")
    _create_dataset(db_session, integration, "commits")

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="test",
        ),
    )

    # (a) SyncRun flushed — session NOT poisoned
    run = db_session.get(SyncRun, plan.sync_run_id)
    assert run is not None, "SyncRun must flush even when tier_limits is unavailable"
    assert run.status == SyncRunStatus.PLANNED.value

    # (b) Depth falls back to community hardcoded default (30), not 90
    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None
    expected_start = now - timedelta(days=30)
    since = units[0].since_at.replace(tzinfo=timezone.utc)
    assert abs((since - expected_start).total_seconds()) < 2, (
        f"Expected depth=30 (community default), got since_at={since}"
    )


@pytest.mark.usefixtures("require_postgres_test_uri")
def test_postgres_missing_tier_limits_stays_inside_planner_savepoint():
    """CHAOS-2580: a pre-migration Postgres tier_limits miss must not poison planning.

    PostgreSQL marks the whole transaction failed after a missing-table error.
    The planner owns a SAVEPOINT around tier-limit resolution so the swallowed
    TierLimitService fallback does not prevent later SyncRun/SyncRunUnit/outbox
    flushes in the same transaction.
    """
    from tests._helpers import sync_postgres_test_url, tables_of

    schema = f"chaos_2580_{uuid.uuid4().hex}"
    engine = create_engine(sync_postgres_test_url())
    connection = engine.connect()
    try:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        connection.execute(text(f'SET search_path TO "{schema}"'))
        connection.commit()
        Base.metadata.create_all(
            connection,
            tables=tables_of(
                Organization,
                OrgLicense,
                # `integrations.credential_id` carries a FOREIGN KEY to
                # `integration_credentials`; an explicit `tables=` list does not
                # pull dependencies in, so omitting it makes CREATE TABLE
                # integrations fail with UndefinedTable. This was invisible
                # until CHAOS-3450 revived the coverage job, because the
                # MissingGreenlet from the async-driver URL aborted the test
                # before create_all ever reached the database.
                IntegrationCredential,
                Integration,
                IntegrationSource,
                IntegrationDataset,
                SyncRun,
                SyncRunReferenceDiscovery,
                SyncRunUnit,
                SyncDispatchOutbox,
                SyncWatermark,
            ),
        )
        with Session(bind=connection) as session:
            org_id = uuid.uuid4()
            session.add(
                Organization(
                    id=org_id,
                    slug=f"chaos-2580-{org_id.hex[:8]}",
                    name="CHAOS 2580",
                    tier="community",
                )
            )
            session.flush()
            integration = Integration(
                org_id=str(org_id),
                provider="github",
                name="Github integration",
                config={"initial_sync_depth": 90},
                is_active=True,
            )
            session.add(integration)
            session.flush()
            source = IntegrationSource(
                org_id=str(org_id),
                integration_id=integration.id,
                provider="github",
                source_type="repo",
                external_id="owner/repo",
                name="repo",
                full_name="owner/repo",
                metadata_={},
                is_enabled=True,
                discovered_at=datetime.now(timezone.utc),
                last_seen_at=datetime.now(timezone.utc),
            )
            dataset = IntegrationDataset(
                org_id=str(org_id),
                integration_id=integration.id,
                dataset_key="commits",
                is_enabled=True,
                options={},
            )
            session.add_all([source, dataset])
            session.flush()

            plan = plan_sync_run(
                session,
                SyncPlanRequest(
                    integration_id=str(integration.id),
                    org_id=str(org_id),
                    mode=SyncRunMode.INCREMENTAL.value,
                    triggered_by="test",
                ),
            )
            session.flush()

            run = session.get(SyncRun, plan.sync_run_id)
            units = _planned_units(session, plan.sync_run_id)
            outbox = (
                session.query(SyncDispatchOutbox)
                .filter_by(sync_run_id=plan.sync_run_id, kind=OUTBOX_KIND_DISCOVERY)
                .one()
            )
            assert run is not None
            assert run.status == SyncRunStatus.PLANNED.value
            assert len(units) == 1
            assert outbox.status == OUTBOX_STATUS_PENDING
            session.execute(text("SELECT 1"))
    finally:
        connection.rollback()
        connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        connection.commit()
        connection.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# CHAOS-2570: backfill -> incremental composition (no date gap)
# ---------------------------------------------------------------------------


def test_backfill_then_incremental_has_no_date_gap(db_session):
    """Canonical onboarding flow: a backfill (which never seeds a watermark per
    CHAOS-2514) followed by an incremental must leave NO date gap.

    With no watermark, the incremental cold-starts at ``now - initial_sync_depth``
    (CHAOS-2569), which reaches back past a backfill whose ``before`` is ~now, so
    coverage is continuous across the seam.
    """
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, "commits")

    now = datetime.now(timezone.utc)
    backfill_before = now  # canonical onboarding: backfill up to ~now

    # 1) Backfill plan: units are mode=backfill and NO watermark is seeded.
    backfill_plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="backfill",
            since=now - timedelta(days=14),
            before=backfill_before,
        ),
    )
    backfill_units = _planned_units(db_session, backfill_plan.sync_run_id)
    assert {u.mode for u in backfill_units} == {SyncRunMode.BACKFILL.value}
    assert get_watermark(db_session, ORG_ID, source.external_id, "commits") is None, (
        "backfill must not seed a watermark (CHAOS-2514)"
    )

    # 2) First incremental cold-starts; window_start <= backfill `before` => no gap.
    inc_plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )
    inc_units = _planned_units(db_session, inc_plan.sync_run_id)
    assert len(inc_units) == 1
    assert inc_units[0].since_at is not None
    since = inc_units[0].since_at.replace(tzinfo=timezone.utc)
    assert since <= backfill_before, (
        "incremental cold-start must reach back to the backfill's `before` "
        "so there is no date gap"
    )


def test_incremental_cold_start_seam_is_depth_bounded(db_session):
    """The no-gap guarantee is depth-bounded: the first incremental cold-start
    window_start is exactly ``now - initial_sync_depth``, so a backfill whose
    ``before`` is at/after that boundary is seamlessly covered. A backfill whose
    ``before`` is OLDER than the boundary is the documented residual edge
    (paused-then-resumed) and is intentionally out of scope here.
    """
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )
    units = _planned_units(db_session, plan.sync_run_id)
    assert units[0].since_at is not None
    since = units[0].since_at.replace(tzinfo=timezone.utc)
    boundary = now - timedelta(days=30)
    assert abs((since - boundary).total_seconds()) < 5
    # A backfill ending at/after the boundary is covered (no gap).
    assert since <= boundary + timedelta(seconds=5)


def test_cold_start_does_not_cover_backfill_before_older_than_depth(db_session):
    """CHAOS-2588 residual (documented boundary): a backfill whose ``before`` is
    OLDER than ``now - initial_sync_depth`` is NOT covered by the incremental
    cold-start window, so a gap ``[before, now - depth]`` remains. This proves the
    no-gap guarantee is BOUNDED (depth-driven, marker-less), not universal -- it
    is the accepted, tracked limit handed off to CHAOS-2588.
    """
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")

    now = datetime.now(timezone.utc)
    backfill_before = now - timedelta(days=90)  # older than depth (30d)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )
    units = _planned_units(db_session, plan.sync_run_id)
    assert units[0].since_at is not None
    since = units[0].since_at.replace(tzinfo=timezone.utc)
    # Cold-start starts at now-depth, which is AFTER the old backfill `before`,
    # so the residual gap [backfill_before, since] is non-empty.
    assert since > backfill_before


def test_github_work_items_unit_carries_prs_signal_when_prs_enabled(db_session):
    """CHAOS-646: when the PRS dataset is enabled, the planner stamps
    ``sync_prs=True`` on the github work-items unit so the adapter threads
    ``include_pull_requests=True`` into the work-items sync."""
    integration = _create_integration(db_session, provider="github")
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "work-items")
    _create_dataset(db_session, integration, "prs")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    work_items_units = [u for u in units if u.dataset_key == "work-items"]
    assert work_items_units
    for unit in work_items_units:
        assert (unit.processor_flags or {}).get("sync_prs") is True


def test_github_work_items_unit_omits_prs_signal_when_prs_disabled(db_session):
    """CHAOS-646 regression: with the PRS dataset off, the work-items unit must
    carry ``sync_prs=False`` so PRs are NOT ingested as work items."""
    integration = _create_integration(db_session, provider="github")
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "work-items")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    work_items_units = [u for u in units if u.dataset_key == "work-items"]
    assert work_items_units
    for unit in work_items_units:
        assert (unit.processor_flags or {}).get("sync_prs") is False


# ---------------------------------------------------------------------------
# CHAOS-2707: work-item-labels / work-item-projects are now INCREMENTAL
# ---------------------------------------------------------------------------


def test_work_item_labels_incremental_cold_start_uses_depth(db_session):
    """work-item-labels has WatermarkBehavior.INCREMENTAL (CHAOS-2707).

    With no saved watermark, since_at must equal now - initial_sync_depth,
    not None.
    """
    from datetime import timedelta

    integration = _create_integration(db_session, provider="github")
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "work-item-labels")

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None, (
        "work-item-labels must use cold-start depth (INCREMENTAL), not since_at=None"
    )
    expected_start = now - timedelta(days=30)
    since = units[0].since_at.replace(tzinfo=timezone.utc)
    assert abs((since - expected_start).total_seconds()) < 2


def test_work_item_projects_incremental_cold_start_uses_depth(db_session):
    """work-item-projects has WatermarkBehavior.INCREMENTAL (CHAOS-2707).

    With no saved watermark, since_at must equal now - initial_sync_depth,
    not None.
    """
    from datetime import timedelta

    integration = _create_integration(db_session, provider="jira")
    integration.config = {"initial_sync_depth": 14}
    db_session.flush()
    _create_source(db_session, integration, external_id="jira-project", provider="jira")
    _create_dataset(db_session, integration, "work-item-projects")

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None, (
        "work-item-projects must use cold-start depth (INCREMENTAL), not since_at=None"
    )
    expected_start = now - timedelta(days=14)
    since = units[0].since_at.replace(tzinfo=timezone.utc)
    assert abs((since - expected_start).total_seconds()) < 2


def test_work_item_labels_incremental_uses_saved_watermark(db_session):
    """work-item-labels: when a watermark exists, since_at == watermark (CHAOS-2707).

    Proves the saved watermark is honoured, not overridden by cold-start depth.
    """
    integration = _create_integration(db_session, provider="github")
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, "work-item-labels")
    watermark = datetime(2026, 6, 15, 8, 0, tzinfo=timezone.utc)
    set_watermark(db_session, ORG_ID, source.external_id, "work-item-labels", watermark)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None
    assert units[0].since_at.replace(tzinfo=timezone.utc) == watermark


def test_work_item_projects_incremental_uses_saved_watermark(db_session):
    """work-item-projects: when a watermark exists, since_at == watermark (CHAOS-2707).

    Proves the saved watermark is honoured, not overridden by cold-start depth.
    """
    integration = _create_integration(db_session, provider="jira")
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="jira-project", provider="jira"
    )
    _create_dataset(db_session, integration, "work-item-projects")
    watermark = datetime(2026, 6, 10, 0, 0, tzinfo=timezone.utc)
    set_watermark(
        db_session, ORG_ID, source.external_id, "work-item-projects", watermark
    )

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None
    assert units[0].since_at.replace(tzinfo=timezone.utc) == watermark


def test_repo_metadata_still_none_watermark_behavior(db_session):
    """repo-metadata remains WatermarkBehavior.NONE after CHAOS-2707.

    Regression guard: removing work-item-labels/projects from _NO_WATERMARK_DATASETS
    must not accidentally change repo-metadata.
    """
    from dev_health_ops.sync.datasets import WatermarkBehavior, get_dataset_spec

    spec = get_dataset_spec("github", "repo-metadata")
    assert spec is not None
    assert spec.watermark_behavior == WatermarkBehavior.NONE


def test_work_item_labels_and_projects_are_incremental_behavior(db_session):
    """Registry-level assertion: both datasets now carry WatermarkBehavior.INCREMENTAL.

    Covers all providers that support these datasets.
    """
    from dev_health_ops.sync.datasets import WatermarkBehavior, get_dataset_spec

    for provider in ("github", "gitlab", "jira", "linear"):
        for dataset_key in ("work-item-labels", "work-item-projects"):
            spec = get_dataset_spec(provider, dataset_key)
            assert spec is not None, f"{provider}/{dataset_key} not in registry"
            assert spec.watermark_behavior == WatermarkBehavior.INCREMENTAL, (
                f"{provider}/{dataset_key} must be INCREMENTAL after CHAOS-2707, "
                f"got {spec.watermark_behavior}"
            )


# ---------------------------------------------------------------------------
# CHAOS-2710: Linear backfill chunk policy
# ---------------------------------------------------------------------------


def test_linear_work_item_backfill_produces_bounded_windows(db_session, monkeypatch):
    """Large Linear work-item backfill is split into windows <= LINEAR_BACKFILL_MAX_WINDOW_DAYS.

    A 90-day range with the default 14-day max must produce 7 chunks, each at most
    14 days wide. Non-Linear providers with the same range keep the 7-day default.
    """
    monkeypatch.delenv("LINEAR_BACKFILL_MAX_WINDOW_DAYS", raising=False)

    integration = _create_integration(db_session, provider="linear")
    _create_source(
        db_session, integration, external_id="linear-team-1", provider="linear"
    )
    _create_dataset(db_session, integration, "work-items")

    since = datetime(2026, 3, 2, tzinfo=timezone.utc)
    before = datetime(2026, 5, 30, 23, 59, 59, tzinfo=timezone.utc)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="test",
            since=since,
            before=before,
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 7, f"expected 7 fortnightly chunks, got {len(units)}"
    for unit in units:
        assert unit.since_at is not None
        assert unit.before_at is not None
        window_days = (unit.before_at.date() - unit.since_at.date()).days + 1
        assert window_days <= 14, (
            f"Linear work-item backfill window too wide: {window_days} days"
            f" (since={unit.since_at.date()}, before={unit.before_at.date()})"
        )
    assert {unit.mode for unit in units} == {SyncRunMode.BACKFILL.value}


def test_linear_work_item_backfill_env_override_respected(db_session, monkeypatch):
    """LINEAR_BACKFILL_MAX_WINDOW_DAYS env override is applied to Linear work-item chunks."""
    monkeypatch.setenv("LINEAR_BACKFILL_MAX_WINDOW_DAYS", "5")

    integration = _create_integration(db_session, provider="linear")
    _create_source(
        db_session, integration, external_id="linear-team-2", provider="linear"
    )
    _create_dataset(db_session, integration, "work-item-history")

    since = datetime(2026, 5, 1, tzinfo=timezone.utc)
    before = datetime(2026, 5, 20, 23, 59, 59, tzinfo=timezone.utc)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="test",
            since=since,
            before=before,
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) > 0
    for unit in units:
        assert unit.since_at is not None
        assert unit.before_at is not None
        window_days = (unit.before_at.date() - unit.since_at.date()).days + 1
        assert window_days <= 5, (
            f"Linear work-item-history window too wide with env=5: {window_days} days"
        )


def test_non_linear_backfill_keeps_seven_day_chunks(db_session, monkeypatch):
    """Non-Linear providers are unaffected by the Linear chunk policy.

    A 14-day github backfill must still produce 2 chunks of 7 days each.
    """
    monkeypatch.delenv("LINEAR_BACKFILL_MAX_WINDOW_DAYS", raising=False)

    integration = _create_integration(db_session, provider="github")
    _create_source(db_session, integration, external_id="owner/repo", provider="github")
    _create_dataset(db_session, integration, "work-items")

    since = datetime(2026, 6, 1, tzinfo=timezone.utc)
    before = datetime(2026, 6, 14, 23, 59, 59, tzinfo=timezone.utc)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="test",
            since=since,
            before=before,
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 2, (
        f"Expected 2 chunks for 14-day github backfill, got {len(units)}"
    )
    windows = set()
    for u in units:
        assert u.since_at is not None and u.before_at is not None
        windows.add((u.since_at.date(), u.before_at.date()))
    assert windows == {
        (datetime(2026, 6, 1).date(), datetime(2026, 6, 7).date()),
        (datetime(2026, 6, 8).date(), datetime(2026, 6, 14).date()),
    }


def test_linear_backfill_units_never_write_watermarks(db_session, monkeypatch):
    """Regression: Linear backfill units must carry mode=backfill and no watermark.

    Mirrors the invariant in test_sync_units.py::test_run_sync_unit_success_skips_watermark_for_backfill.
    The planner side of the contract: all units produced for a Linear backfill
    carry mode='backfill', which is the gate the worker checks before writing
    watermarks (workers/sync_units.py:401-408). This test asserts the planner
    never emits a non-backfill mode for a backfill request, and that no
    SyncWatermark rows exist after planning (planning never writes watermarks).
    """
    monkeypatch.delenv("LINEAR_BACKFILL_MAX_WINDOW_DAYS", raising=False)

    integration = _create_integration(db_session, provider="linear")
    _create_source(
        db_session, integration, external_id="linear-team-3", provider="linear"
    )
    for dataset_key in (
        "work-items",
        "work-item-labels",
        "work-item-projects",
        "work-item-history",
        "work-item-comments",
    ):
        _create_dataset(db_session, integration, dataset_key)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="test",
            since=datetime(2026, 5, 1, tzinfo=timezone.utc),
            before=datetime(2026, 5, 10, 23, 59, 59, tzinfo=timezone.utc),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) > 0
    # All units must carry mode=backfill — the worker gate reads this field.
    assert all(u.mode == SyncRunMode.BACKFILL.value for u in units), (
        "All Linear backfill units must carry mode=backfill"
    )
    # Planning must never write watermarks.
    assert db_session.query(SyncWatermark).count() == 0, (
        "plan_sync_run must not write any SyncWatermark rows"
    )


# ---------------------------------------------------------------------------
# CHAOS-2721 (AD-3): work-item-family plan-time collapse
# ---------------------------------------------------------------------------

_FAMILY_DATASETS = (
    "work-items",
    "work-item-labels",
    "work-item-projects",
    "work-item-history",
    "work-item-comments",
)


def test_work_item_family_collapses_to_single_composite_unit(db_session):
    """Enabling all five work-item-family datasets emits ONE composite unit
    (canonical dataset_key="work-items") with a boolean family_dataset_<key>
    flag per enabled dataset, instead of five units each re-running the full
    crawl (CHAOS-2721)."""
    integration = _create_integration(db_session, provider="linear")
    _create_source(
        db_session, integration, external_id="linear-team-1", provider="linear"
    )
    for dataset_key in _FAMILY_DATASETS:
        _create_dataset(db_session, integration, dataset_key)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1, f"expected ONE composite work-items unit, got {len(units)}"
    unit = units[0]
    assert unit.dataset_key == "work-items"
    flags = unit.processor_flags or {}
    for dataset_key in _FAMILY_DATASETS:
        flag = "family_dataset_" + dataset_key.replace("-", "_")
        assert flags.get(flag) is True, f"{flag} must be set on the composite unit"


def test_work_item_family_collapse_uses_earliest_window_across_datasets(db_session):
    """The composite unit's since_at is the EARLIEST watermark across enabled
    family datasets, so the single crawl covers every dataset (over-fetch is
    safe; CHAOS-2721 / AD-3)."""
    integration = _create_integration(db_session, provider="linear")
    source = _create_source(
        db_session, integration, external_id="linear-team-1", provider="linear"
    )
    _create_dataset(db_session, integration, "work-items")
    _create_dataset(db_session, integration, "work-item-comments")
    # work-items synced more recently than comments -> comments is the laggard.
    newer = datetime(2026, 6, 15, 0, 0, tzinfo=timezone.utc)
    older = datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc)
    set_watermark(db_session, ORG_ID, source.external_id, "work-items", newer)
    set_watermark(db_session, ORG_ID, source.external_id, "work-item-comments", older)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None
    assert units[0].since_at.replace(tzinfo=timezone.utc) == older


@pytest.mark.parametrize(
    "provider, external_id",
    [
        ("github", "full-chaos/dev-health"),
        ("gitlab", "group/project"),
        ("jira", "jira-project"),
        ("linear", "linear-team-1"),
    ],
)
def test_work_item_family_collapse_provider_matrix(db_session, provider, external_id):
    """Family collapse is provider-agnostic: all four providers collapse the
    enabled family to a single composite unit (provider x entity contract)."""
    integration = _create_integration(db_session, provider=provider)
    _create_source(db_session, integration, external_id=external_id, provider=provider)
    for dataset_key in _FAMILY_DATASETS:
        _create_dataset(db_session, integration, dataset_key)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    work_item_units = [
        u
        for u in _planned_units(db_session, plan.sync_run_id)
        if u.dataset_key in _FAMILY_DATASETS
    ]
    assert len(work_item_units) == 1
    assert work_item_units[0].dataset_key == "work-items"


def test_work_item_family_collapse_backfill_one_composite_per_chunk(
    db_session, monkeypatch
):
    """A Linear backfill enabling all five family datasets produces ONE composite
    unit per chunk (7 for a 90-day/14-day backfill), not 5x7=35 (CHAOS-2721)."""
    monkeypatch.delenv("LINEAR_BACKFILL_MAX_WINDOW_DAYS", raising=False)
    integration = _create_integration(db_session, provider="linear")
    _create_source(
        db_session, integration, external_id="linear-team-1", provider="linear"
    )
    for dataset_key in _FAMILY_DATASETS:
        _create_dataset(db_session, integration, dataset_key)

    since = datetime(2026, 3, 2, tzinfo=timezone.utc)
    before = datetime(2026, 5, 30, 23, 59, 59, tzinfo=timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="test",
            since=since,
            before=before,
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 7, f"expected 7 composite chunks, got {len(units)}"
    assert {u.dataset_key for u in units} == {"work-items"}


def test_github_family_composite_carries_prs_signal_alongside_code_unit(db_session):
    """github: the composite work-items unit carries sync_prs while the PRS code
    dataset remains its own unit (collapse only folds the work-item family)."""
    integration = _create_integration(db_session, provider="github")
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    for dataset_key in _FAMILY_DATASETS:
        _create_dataset(db_session, integration, dataset_key)
    _create_dataset(db_session, integration, "prs")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    work_items = [u for u in units if u.dataset_key == "work-items"]
    prs_units = [u for u in units if u.dataset_key == "prs"]
    assert len(work_items) == 1
    assert (work_items[0].processor_flags or {}).get("sync_prs") is True
    assert len(prs_units) == 1


def test_merge_family_windows_rejects_mismatched_window_counts():
    """The index-aligned merge assumes every enabled family dataset resolves to
    the same number of windows; a mismatch is a planner invariant violation and
    must fail fast rather than silently dropping windows."""
    from dev_health_ops.sync.planner import _merge_family_windows

    a = datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc)
    b = datetime(2026, 6, 2, 0, 0, tzinfo=timezone.utc)
    c = datetime(2026, 6, 3, 0, 0, tzinfo=timezone.utc)
    d = datetime(2026, 6, 4, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="mismatched window counts"):
        _merge_family_windows([((a, b),), ((a, b), (c, d))])


# ---------------------------------------------------------------------------
# CHAOS-3412: HEAVY incremental window ratchet
#
# A HEAVY dataset cold-starting on a wide ``initial_sync_depth`` used to plan
# ONE window spanning the whole depth. Unit cost is linear in span, so that
# single unit could never fit the sync budget; it was deferred forever, no
# watermark was ever stamped, and every subsequent tick recomputed the same
# unfittable span. The ratchet caps the INCREMENTAL window span for HEAVY
# datasets so each tick plans an affordable slice and advances the watermark.
# ---------------------------------------------------------------------------


def _window(unit: SyncRunUnit) -> tuple[datetime, datetime]:
    """Return a unit's (since_at, before_at) as tz-aware UTC, asserting both are set."""
    assert unit.since_at is not None
    assert unit.before_at is not None
    return (
        unit.since_at.replace(tzinfo=timezone.utc),
        unit.before_at.replace(tzinfo=timezone.utc),
    )


_HEAVY_DATASET = "commit-stats"  # CostClass.HEAVY (sync/datasets.py _HEAVY_DATASETS)
_NON_HEAVY_DATASET = "commits"  # CostClass.MEDIUM


# ---------------------------------------------------------------------------
# Deterministic ratchet contract table.
#
# These cases pin the ratchet's behavioral clauses at EXACT timestamps with no
# wall-clock dependency and no tolerance, by driving ``_resolve_windows``
# directly with an injected ``now``. That makes them reproducible by any other
# implementation of the same contract (e.g. a Go-native scheduler planner), so
# this table is the Python side of a cross-implementation differential oracle:
# feed the same (dataset, watermark, now, requested_before, depth, cap) inputs
# to both implementations and the (since, before) pair must match exactly.
#
# Every case must stay expressible as pure data — no fixture reaching into
# Python-only internals — or it stops being portable to the other side.
# ---------------------------------------------------------------------------

_ORACLE_NOW = datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc)
_ORACLE_DEPTH_DAYS = 30
_ORACLE_CAP_DAYS = 7
# _ORACLE_NOW - 30 days, i.e. the cold-start window_start for every case below.
_ORACLE_COLD_START = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)


def _dt(year: int, month: int, day: int, hour: int = 12) -> datetime:
    return datetime(year, month, day, hour, 0, tzinfo=timezone.utc)


# (case_id, dataset_key, watermark, requested_before, expected_since, expected_before)
_RATCHET_CONTRACT_CASES: list[
    tuple[str, str, datetime | None, datetime | None, datetime | None, datetime]
] = [
    # --- Clause: HEAVY cold-start is capped at window_start + cap ------------
    (
        "heavy-cold-start-is-capped",
        "commit-stats",
        None,
        None,
        _ORACLE_COLD_START,
        _dt(2026, 5, 25),  # cold_start + 7d, NOT _ORACLE_NOW
    ),
    (
        "heavy-cold-start-is-capped-for-every-heavy-key",
        "files",
        None,
        None,
        _ORACLE_COLD_START,
        _dt(2026, 5, 25),
    ),
    # --- Clause: the cap is scoped to CostClass.HEAVY only -------------------
    (
        "medium-cold-start-is-uncapped",
        "commits",
        None,
        None,
        _ORACLE_COLD_START,
        _ORACLE_NOW,
    ),
    (
        "light-cold-start-is-uncapped",
        "work-item-labels",
        None,
        None,
        _ORACLE_COLD_START,
        _ORACLE_NOW,
    ),
    (
        "medium-behind-watermark-is-uncapped",
        "commits",
        _dt(2026, 3, 1),
        None,
        _dt(2026, 3, 1),
        _ORACLE_NOW,
    ),
    # --- Clause: the cap applies to the behind-watermark case too ------------
    (
        "heavy-behind-watermark-is-capped",
        "commit-stats",
        _dt(2026, 3, 1),
        None,
        _dt(2026, 3, 1),
        _dt(2026, 3, 8),  # watermark + 7d
    ),
    # --- Clause: the cap only ever moves the END in (it is a min, not a set) -
    (
        "heavy-watermark-inside-cap-keeps-natural-end",
        "commit-stats",
        _dt(2026, 6, 15),  # 2 days behind now
        None,
        _dt(2026, 6, 15),
        _ORACLE_NOW,
    ),
    (
        "heavy-watermark-exactly-at-cap-boundary-keeps-natural-end",
        "commit-stats",
        _dt(2026, 6, 10),  # exactly now - 7d; the tie must resolve to now
        None,
        _dt(2026, 6, 10),
        _ORACLE_NOW,
    ),
    (
        "heavy-requested-before-tighter-than-cap-wins",
        "commit-stats",
        _dt(2026, 3, 1),
        _dt(2026, 3, 4),  # requested end is nearer than watermark + cap
        _dt(2026, 3, 1),
        _dt(2026, 3, 4),
    ),
    # --- Clause: a NONE-watermark-behavior dataset is never capped -----------
    # NOTE: repo-metadata is the only NONE-behavior dataset and it is LIGHT, so
    # this case exercises the NONE path end-to-end but short-circuits on the
    # cost-class check and never reaches the ``window_start is not None`` guard.
    # That guard is covered separately by
    # ``test_open_start_window_is_never_capped_even_when_heavy``.
    (
        "none-watermark-behavior-keeps-open-start",
        "repo-metadata",
        None,
        None,
        None,  # window_start stays None; the cap must not synthesize one
        _ORACLE_NOW,
    ),
]


@pytest.mark.parametrize(
    "case_id,dataset_key,watermark,requested_before,expected_since,expected_before",
    _RATCHET_CONTRACT_CASES,
    ids=[case[0] for case in _RATCHET_CONTRACT_CASES],
)
def test_ratchet_window_contract(
    db_session,
    monkeypatch,
    case_id,
    dataset_key,
    watermark,
    requested_before,
    expected_since,
    expected_before,
):
    """CHAOS-3412: the exact (since, before) the ratchet must produce.

    Behavioral clauses pinned by this table:

    1. Cap default is 7 days, overridable via
       ``SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS``.
    2. The cap is scoped to ``CostClass.HEAVY`` datasets ONLY; LIGHT and MEDIUM
       keep a single full-span window.
    3. The cap applies to BOTH the cold-start case (no watermark, start =
       ``now - initial_sync_depth``) and the behind-watermark case (start =
       stored watermark less the configured overlap).
    4. The cap only moves the window END inward: ``before = min(requested_before
       or now, window_start + cap)``. ``window_start`` is never moved, so depth
       resolution and the tier ``backfill_days`` cap are unaffected.
    5. A ``WatermarkBehavior.NONE`` dataset keeps ``window_start = None`` and is
       never capped.
    """
    from dev_health_ops.sync.datasets import get_dataset_spec

    # Pin every input the contract depends on so the case is reproducible by
    # another implementation: default cap, zero overlap, injected ``now``.
    monkeypatch.delenv("SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS", raising=False)
    monkeypatch.setenv("SYNC_WATERMARK_OVERLAP", "0")

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": _ORACLE_DEPTH_DAYS}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    dataset = _create_dataset(db_session, integration, dataset_key)
    if watermark is not None:
        set_watermark(db_session, ORG_ID, source.external_id, dataset_key, watermark)

    spec = get_dataset_spec("github", dataset_key)
    assert spec is not None and spec.supported

    windows = planner._resolve_windows(
        session=db_session,
        request=SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="scheduled",
            before=requested_before,
        ),
        mode=SyncRunMode.INCREMENTAL.value,
        org_id=ORG_ID,
        source_provider="github",
        watermark_source_key=source.external_id,
        dataset_key=dataset_key,
        watermark_behavior=spec.watermark_behavior,
        now=_ORACLE_NOW,
        integration=integration,
        dataset=dataset,
    )

    assert len(windows) == 1, (
        f"{case_id}: the ratchet must not split a unit into multiple windows; "
        "it caps ONE window and lets the next scheduled tick continue"
    )
    since, before = windows[0]
    since = None if since is None else _as_utc_for_test(since)
    assert since == expected_since, f"{case_id}: window_start"
    assert before is not None
    assert _as_utc_for_test(before) == expected_before, f"{case_id}: window_end"


def _as_utc_for_test(value: datetime) -> datetime:
    """SQLite hands back naive datetimes; the contract is in UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def test_open_start_window_is_never_capped_even_when_heavy(db_session, monkeypatch):
    """The ``window_start is not None`` guard, exercised for real.

    No dataset is registered HEAVY *and* ``WatermarkBehavior.NONE`` today, so
    the contract table above can never reach this guard — every NONE dataset
    short-circuits on the cost-class check first. The guard is still load-
    bearing: a future HEAVY dataset registered with NONE behavior would reach
    ``window_start + cap`` with ``window_start=None`` and raise. Force that
    combination so the guard is covered rather than merely present.
    """
    from dev_health_ops.sync.datasets import WatermarkBehavior

    monkeypatch.delenv("SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS", raising=False)
    monkeypatch.setattr(planner, "_is_heavy_dataset", lambda provider, key: True)

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": _ORACLE_DEPTH_DAYS}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    dataset = _create_dataset(db_session, integration, "repo-metadata")

    windows = planner._resolve_windows(
        session=db_session,
        request=SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="scheduled",
        ),
        mode=SyncRunMode.INCREMENTAL.value,
        org_id=ORG_ID,
        source_provider="github",
        watermark_source_key=source.external_id,
        dataset_key="repo-metadata",
        watermark_behavior=WatermarkBehavior.NONE,
        now=_ORACLE_NOW,
        integration=integration,
        dataset=dataset,
    )

    assert windows == ((None, _ORACLE_NOW),), (
        "an open-start window must pass through uncapped, not raise and not "
        "acquire a synthesized start"
    )


def test_ratchet_contract_table_covers_every_heavy_dataset_key(db_session):
    """The cap is a COST-CLASS rule, not a per-key list.

    If a new dataset joins ``CostClass.HEAVY``, it inherits the cap
    automatically — assert that directly against the registry so the contract
    table above can never drift into describing only the keys it happens to
    name.
    """
    from dev_health_ops.sync.datasets import get_dataset_spec
    from dev_health_ops.sync.planner import _is_heavy_dataset

    heavy_keys = [
        key
        for key in ("commit-stats", "files", "blame", "tests", "commits", "prs")
        if (spec := get_dataset_spec("github", key)) is not None
        and spec.default_cost_class.value == "heavy"
    ]
    assert heavy_keys == ["commit-stats", "files", "blame", "tests"]
    for key in heavy_keys:
        assert _is_heavy_dataset("github", key) is True
    for key in ("commits", "prs", "work-item-labels", "repo-metadata"):
        assert _is_heavy_dataset("github", key) is False


def test_incremental_cold_start_heavy_dataset_is_capped(db_session):
    """CHAOS-3412: HEAVY cold-start plans ``[now - depth, now - depth + cap]``.

    The window must END at ``window_start + cap``, NOT at ``now``.
    """
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, _HEAVY_DATASET)

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    unit = units[0]
    since, before = _window(unit)

    assert abs((since - (now - timedelta(days=30))).total_seconds()) < 2
    assert abs((before - (since + timedelta(days=7))).total_seconds()) < 2, (
        "HEAVY cold-start window must be capped at 7 days from window_start"
    )
    # The decisive assertion: the window does NOT run to now.
    assert before < now - timedelta(days=20), (
        f"HEAVY cold-start window ended at {before} (now={now}); it must stop "
        "at window_start + cap, not run to now"
    )


def test_incremental_cold_start_non_heavy_dataset_is_not_capped(db_session):
    """Non-HEAVY datasets keep the full-depth single window (regression guard)."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, _NON_HEAVY_DATASET)

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    unit = units[0]
    since, before = _window(unit)
    assert abs((since - (now - timedelta(days=30))).total_seconds()) < 2
    assert abs((before - now).total_seconds()) < 2, (
        "MEDIUM dataset must keep the full-depth window ending at now"
    )


def test_incremental_behind_watermark_heavy_dataset_is_capped(db_session):
    """A long-idle org whose HEAVY watermark is far behind ratchets too."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, _HEAVY_DATASET)
    watermark = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    set_watermark(db_session, ORG_ID, source.external_id, _HEAVY_DATASET, watermark)

    requested_before = datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=requested_before,
        ),
    )

    unit = _planned_units(db_session, plan.sync_run_id)[0]
    since, before = _window(unit)
    assert since == watermark
    assert before == watermark + timedelta(days=7)
    assert before < requested_before


def test_incremental_watermark_within_cap_heavy_window_unchanged(db_session):
    """A caught-up HEAVY dataset keeps its natural window ending at ``before``."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, _HEAVY_DATASET)
    requested_before = datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc)
    watermark = requested_before - timedelta(days=2)
    set_watermark(db_session, ORG_ID, source.external_id, _HEAVY_DATASET, watermark)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=requested_before,
        ),
    )

    unit = _planned_units(db_session, plan.sync_run_id)[0]
    since, before = _window(unit)
    assert since == watermark
    assert before == requested_before, (
        "a watermark inside the cap must not shorten the window"
    )


def test_heavy_max_window_days_env_override_is_respected(db_session, monkeypatch):
    from datetime import timedelta

    monkeypatch.setenv("SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS", "3")
    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, _HEAVY_DATASET)
    watermark = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    set_watermark(db_session, ORG_ID, source.external_id, _HEAVY_DATASET, watermark)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        ),
    )
    unit = _planned_units(db_session, plan.sync_run_id)[0]
    _, before = _window(unit)
    assert before == watermark + timedelta(days=3)


def test_heavy_ratchet_marches_watermark_forward_without_gaps(db_session, monkeypatch):
    """Successive successful ticks walk the watermark to ``now``.

    Each tick's window must start exactly where the previous one ended (minus
    the configured overlap) — no gap, no overlap beyond the configured one —
    and the sequence must terminate at ``now`` in a bounded number of ticks.
    """
    from datetime import timedelta

    monkeypatch.setenv("SYNC_WATERMARK_OVERLAP", "0")
    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, _HEAVY_DATASET)

    start = datetime.now(timezone.utc)
    previous_before: datetime | None = None
    windows: list[tuple[datetime, datetime]] = []
    for _ in range(12):
        plan = plan_sync_run(
            db_session,
            SyncPlanRequest(
                integration_id=str(integration.id),
                org_id=ORG_ID,
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="scheduled",
            ),
        )
        unit = _planned_units(db_session, plan.sync_run_id)[0]
        since, before = _window(unit)
        windows.append((since, before))
        if previous_before is not None:
            assert since == previous_before, (
                f"window must resume exactly at the previous watermark "
                f"(gap/overlap): since={since} previous_before={previous_before}"
            )
        assert before - since <= timedelta(days=7) + timedelta(seconds=2)
        # Simulate the SUCCESS path: watermark stamped at the unit's window END.
        set_watermark(db_session, ORG_ID, source.external_id, _HEAVY_DATASET, before)
        previous_before = before
        if before >= start:
            break

    assert previous_before is not None
    assert previous_before >= start, (
        f"ratchet did not reach now within 12 ticks; last before={previous_before}"
    )
    # 30 days of depth at a 7-day cap: 5 capped ticks + a final partial tick.
    assert 4 <= len(windows) <= 7, f"unexpected tick count: {len(windows)}"
    assert windows[0][0] <= start - timedelta(days=29)


# ---------------------------------------------------------------------------
# CHAOS-3412 F2: incremental windows must never end in the future, and an
# empty/inverted window must plan NO unit.
#
# The success path stamps the watermark at the window END. So a future
# ``before`` persists a FUTURE watermark, and the next run starts in the future
# and silently skips everything up to it. An inverted window (end <= start) is
# worse than useless: the admission estimate floors a negative span to 1 day, so
# it reads as a cheap unit, sails through the budget guard, fetches nothing, and
# still finalizes as SUCCESS.
# ---------------------------------------------------------------------------


def test_incremental_window_end_is_clamped_to_now(db_session):
    """A future ``before`` must not persist a future window end."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, _NON_HEAVY_DATASET)
    set_watermark(
        db_session,
        ORG_ID,
        source.external_id,
        _NON_HEAVY_DATASET,
        datetime.now(timezone.utc) - timedelta(days=3),
    )

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=now + timedelta(days=45),  # far-future upper bound
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    _, before = _window(units[0])
    assert before <= now + timedelta(seconds=2), (
        f"window end {before} is in the future; the success path would stamp it "
        "as the watermark and the next run would start in the future"
    )


def test_incremental_window_end_is_clamped_to_now_for_heavy(db_session):
    """The future clamp applies to HEAVY datasets too, alongside the cap."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, _HEAVY_DATASET)
    watermark = datetime.now(timezone.utc) - timedelta(days=2)
    set_watermark(db_session, ORG_ID, source.external_id, _HEAVY_DATASET, watermark)

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=now + timedelta(days=45),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    _, before = _window(units[0])
    assert before <= now + timedelta(seconds=2)


@pytest.mark.parametrize(
    "case_id,watermark_offset_days,before_offset_days",
    [
        # `before` strictly older than the watermark start -> inverted window.
        ("before-older-than-watermark", 3, 10),
        # `before` exactly at the watermark start -> zero-width window.
        ("before-equals-watermark", 3, 3),
    ],
    ids=["before-older-than-start", "before-equals-start"],
)
def test_empty_or_inverted_incremental_window_plans_no_unit(
    db_session, case_id, watermark_offset_days, before_offset_days
):
    """end <= start must plan ZERO units, not an inverted/zero-width one."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, _NON_HEAVY_DATASET)
    anchor = datetime.now(timezone.utc)
    set_watermark(
        db_session,
        ORG_ID,
        source.external_id,
        _NON_HEAVY_DATASET,
        anchor - timedelta(days=watermark_offset_days),
    )

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=anchor - timedelta(days=before_offset_days),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert units == [], (
        f"{case_id}: an empty/inverted window must plan no unit; planned "
        f"{[(u.since_at, u.before_at) for u in units]}"
    )
    assert plan.total_units == 0


def test_cold_start_with_before_older_than_depth_plans_no_unit(db_session):
    """The drift case that was already live in this suite.

    Two pre-existing tests passed a hardcoded ``before`` of 2026-06-17 that was
    in the future when written. Once wall-clock time passed it, their cold-start
    windows became inverted (start = now - 30d, end = 2026-06-17) and they kept
    passing because nothing asserted window sanity. Assert it deliberately.
    """
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, _NON_HEAVY_DATASET)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime.now(timezone.utc) - timedelta(days=90),
        ),
    )

    assert _planned_units(db_session, plan.sync_run_id) == []


def test_github_family_empty_window_keeps_atomic_route_ownership(db_session):
    """A partially-caught-up GitHub family still plans one all-five unit.

    ``_merge_family_windows`` raises on mismatched window counts. Once an
    already-caught-up dataset resolves to ZERO windows, that guard would fire on
    an ordinary partially-synced family and take the whole plan down, so empty
    datasets are dropped before the merge. CHAOS-3606 keeps that window behavior
    while making all five flags literal ``True``: they describe the activated
    native writer's atomic ownership, not which inputs were non-empty.
    """
    from datetime import timedelta

    integration = _create_integration(db_session, provider="github")
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, "work-items")
    _create_dataset(db_session, integration, "work-item-labels")

    anchor = datetime.now(timezone.utc)
    requested_before = anchor - timedelta(days=5)
    # work-items is behind the requested end (contributes a window); labels is
    # already synced past it (resolves empty).
    set_watermark(
        db_session, ORG_ID, source.external_id, "work-items", anchor - timedelta(days=9)
    )
    set_watermark(
        db_session,
        ORG_ID,
        source.external_id,
        "work-item-labels",
        anchor - timedelta(days=2),
    )

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=requested_before,
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    composite = units[0]
    assert composite.dataset_key == "work-items"
    flags = composite.processor_flags or {}
    assert {
        key: flags.get("family_dataset_" + key.replace("-", "_"))
        for key in _FAMILY_DATASETS
    } == {key: True for key in _FAMILY_DATASETS}


# ---------------------------------------------------------------------------
# CHAOS-3412: FULL_RESYNC inherits the same window rules as INCREMENTAL.
#
# sync_units.py gates watermark stamping on exactly {INCREMENTAL, FULL_RESYNC},
# so both modes stamp the watermark at the unit's window END and both inherit
# the future-end and inverted-window defects. Both branches now share
# ``_watermark_stamping_window``; these tests hold FULL_RESYNC to the contract
# independently so the shared helper cannot be bypassed for one mode later.
# ---------------------------------------------------------------------------


def test_full_resync_window_end_is_clamped_to_now(db_session):
    """A future ``before`` must not persist a future full_resync window end."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, _NON_HEAVY_DATASET)

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.FULL_RESYNC.value,
            triggered_by="admin-api",
            before=now + timedelta(days=45),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    _, before = _window(units[0])
    assert before <= now + timedelta(seconds=2), (
        f"full_resync window end {before} is in the future; full_resync IS in "
        "the watermark-stamping set, so this would persist a future watermark"
    )


def test_full_resync_inverted_window_plans_no_unit(db_session):
    """``before`` older than ``now - depth`` must plan zero units, not an
    inverted one whose negative span floors to 1 day in admission."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, _NON_HEAVY_DATASET)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.FULL_RESYNC.value,
            triggered_by="admin-api",
            before=datetime.now(timezone.utc) - timedelta(days=90),  # depth is 30d
        ),
    )

    assert _planned_units(db_session, plan.sync_run_id) == []
    assert plan.total_units == 0


def test_full_resync_normal_window_is_unchanged(db_session):
    """Regression guard: the ordinary full_resync window still spans the full
    configured depth and ends at now."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 14}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, _NON_HEAVY_DATASET)

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.FULL_RESYNC.value,
            triggered_by="admin-api",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    since, before = _window(units[0])
    assert abs((since - (now - timedelta(days=14))).total_seconds()) < 2
    assert abs((before - now).total_seconds()) < 2


def test_backfill_windows_are_not_routed_through_the_watermark_normalizer(db_session):
    """BACKFILL must keep its own bounds handling.

    Backfill never stamps a watermark (CHAOS-2514) and legitimately targets a
    historical range that ends well before ``now``. Routing it through the
    watermark normalizer would be harmless for the clamp but would let the
    empty-window rule silently drop chunks, so backfill stays out of it.
    """
    from datetime import timedelta

    integration = _create_integration(db_session)
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, _NON_HEAVY_DATASET)

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="admin-api",
            since=now - timedelta(days=60),
            before=now - timedelta(days=30),  # entirely in the past
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert units, "a historical backfill must still plan units"
    for unit in units:
        since, before = _window(unit)
        assert since < before, "backfill chunks must stay forward-ordered"
        assert before <= now


# ---------------------------------------------------------------------------
# CHAOS-3412 round 2: a FUTURE watermark must self-heal, not stall forever.
#
# Suppressing empty/inverted windows means a watermark ahead of `now` makes
# every scheduled tick plan ZERO units — which finalizes FAILED, and with no
# unit there is nothing to re-stamp the watermark, so it never recovers.
# set_watermark is monotonic, so a future value can never be lowered by a later
# run either. Reachable: pagerduty derives watermark_at from source record
# timestamps (provider clock skew / bad data), and pre-fix planner code could
# persist a future window end directly, so live DBs may already hold one.
#
# The planner treats a start ahead of `now` as corrupt, clamps it back so a real
# window plans, and warns loudly. One successful run then re-stamps a sane,
# end-clamped watermark.
# ---------------------------------------------------------------------------


def _seed_corrupt_future_watermark(session, org_id, source_id, dataset_key, when):
    """Write a FUTURE watermark directly, bypassing set_watermark's clamp.

    CHAOS-3412 round 3 added a write-boundary invariant: ``set_watermark`` never
    persists a future value, from any caller. That is correct, and it means the
    corrupt state can no longer be created through the public API — so a test
    that seeds via ``set_watermark`` silently STOPS exercising the repair path
    while still passing. This writes the row directly, which is also how the
    state genuinely arises: rows persisted by pre-fix code, sitting in a live
    database.
    """
    row = (
        session.query(SyncWatermark)
        .filter(
            SyncWatermark.org_id == org_id,
            SyncWatermark.source_id == source_id,
            SyncWatermark.dataset_key == dataset_key,
        )
        .one_or_none()
    )
    if row is None:
        row = SyncWatermark(
            repo_id=source_id,
            target=dataset_key,
            org_id=org_id,
            source_id=source_id,
            dataset_key=dataset_key,
            last_synced_at=when,
        )
        session.add(row)
    else:
        row.last_synced_at = when
    session.flush()
    return row


def test_future_watermark_still_plans_a_unit(db_session, caplog):
    """A corrupt future watermark must NOT stall the scheduled path."""
    import logging
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, _NON_HEAVY_DATASET)
    now = datetime.now(timezone.utc)
    corrupt = now + timedelta(days=30)
    _seed_corrupt_future_watermark(
        db_session, ORG_ID, source.external_id, _NON_HEAVY_DATASET, corrupt
    )

    with caplog.at_level(logging.WARNING, logger="dev_health_ops.sync.planner"):
        plan = plan_sync_run(
            db_session,
            SyncPlanRequest(
                integration_id=str(integration.id),
                org_id=ORG_ID,
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="scheduled",  # scheduled path: no explicit before
            ),
        )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1, (
        "a future watermark must still plan a unit; zero units finalizes FAILED "
        "and leaves nothing to re-stamp the watermark, stalling forever"
    )
    since, before = _window(units[0])
    assert since < before, "the healed window must be forward-ordered"
    assert before <= now + timedelta(seconds=2)
    assert since <= now

    warned = [
        r
        for r in caplog.records
        if "future_watermark" in r.getMessage()
        or "watermark_ahead_of_now" in r.getMessage()
    ]
    assert warned, (
        "clamping a corrupt future watermark must warn loudly; silently "
        "rewriting a watermark is exactly the kind of invisible repair that "
        "hides data loss"
    )


def test_future_watermark_recovers_after_one_success(db_session):
    """After one successful run the watermark is sane again (self-healing)."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, _NON_HEAVY_DATASET)
    now = datetime.now(timezone.utc)
    _seed_corrupt_future_watermark(
        db_session,
        ORG_ID,
        source.external_id,
        _NON_HEAVY_DATASET,
        now + timedelta(days=30),
    )

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="scheduled",
        ),
    )
    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    _, before = _window(units[0])

    # Healing REQUIRES lowering the stored value, which plain monotonic
    # semantics would discard. Go through the REAL production write path so the
    # test proves the correction actually happens rather than assuming it.
    set_watermark(db_session, ORG_ID, source.external_id, _NON_HEAVY_DATASET, before)
    healed = get_watermark(db_session, ORG_ID, source.external_id, _NON_HEAVY_DATASET)
    assert healed is not None
    healed_aware = (
        healed.replace(tzinfo=timezone.utc) if healed.tzinfo is None else healed
    )
    assert healed_aware <= now + timedelta(seconds=2), (
        f"watermark still in the future after a successful run: {healed_aware}"
    )

    # And the next tick plans a normal forward window from the healed value.
    plan2 = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="scheduled",
        ),
    )
    units2 = _planned_units(db_session, plan2.sync_run_id)
    assert len(units2) == 1, "the tick after healing must plan normally"


def test_full_resync_heavy_window_is_deliberately_uncapped(db_session):
    """POLICY PIN (CHAOS-3412, decided): full_resync does NOT get the heavy cap.

    A capped one-shot full resync would cover only the cap's span and then
    finalize SUCCESS — a false coverage claim, which is worse than the exposure
    it would avoid. The wide-window exposure is instead bounded by the budget
    exhaustion path: an oversized unit terminalizes visibly as
    ``budget_deferral_exhausted`` naming the scoped-backfill remedy, which
    satisfies "syncs or fails visibly".

    This is a DECISION, not an oversight. If a future change adds the cap here,
    it must revisit that decision rather than assume this was missed.
    """
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, _HEAVY_DATASET)

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.FULL_RESYNC.value,
            triggered_by="admin-api",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    since, before = _window(units[0])
    assert abs((since - (now - timedelta(days=30))).total_seconds()) < 2
    assert abs((before - now).total_seconds()) < 2, (
        "full_resync of a HEAVY dataset must span the FULL configured depth. A "
        "7-day capped window finalizing SUCCESS would misreport a 30-day resync "
        "as complete."
    )
    span_days = (before - since).days
    assert span_days >= 29, (
        f"full_resync heavy span collapsed to {span_days} days — the heavy "
        "incremental cap must not leak into full_resync"
    )


# ---------------------------------------------------------------------------
# CHAOS-3412 CLOSURE ARGUMENT for the window-bounds defect family.
#
# Six members of one family were fixed on this branch: the heavy cap itself,
# overlap >= cap, future end, inverted end, both halves again in FULL_RESYNC,
# and a future start from a corrupt watermark. Rather than wait for a seventh to
# be found, this enumerates the ENTIRE sign-region state space of
# (window_start, window_end) relative to `now` and asserts the postcondition
# that every member of the family violated in some way.
#
# The postcondition, which is what "no more siblings" actually means:
#   for every input, the result is either
#     - zero windows, or
#     - exactly one window (s, e) with e <= now, and s < e when s is not None.
#   It is NEVER a window that ends in the future, is inverted, or is zero-width.
#
# A new sibling can only exist if it violates this postcondition or escapes the
# helper. This test closes the first; the mutation tests that break BOTH modes
# when the helper changes close the second.
# ---------------------------------------------------------------------------


def test_watermark_window_postcondition_holds_across_the_whole_state_space():
    from datetime import timedelta

    from dev_health_ops.sync.planner import _watermark_stamping_window

    now = datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc)
    offsets = [
        ("far-past", -timedelta(days=10)),
        ("near-past", -timedelta(hours=1)),
        ("exactly-now", timedelta(0)),
        ("near-future", timedelta(hours=1)),
        ("far-future", timedelta(days=10)),
    ]
    starts = [(name, now + delta) for name, delta in offsets] + [("none", None)]
    ends = [(name, now + delta) for name, delta in offsets]

    checked = 0
    for start_name, start in starts:
        for end_name, end in ends:
            case = f"start={start_name} end={end_name}"
            windows = _watermark_stamping_window(start, end, now)
            assert isinstance(windows, tuple)
            assert len(windows) <= 1, f"{case}: helper must never split windows"
            if not windows:
                continue
            s, e = windows[0]
            assert e is not None, f"{case}: a planned window needs an end"
            assert e <= now, f"{case}: window end {e} is in the future"
            if s is not None:
                assert _as_utc_for_test(s) < e, (
                    f"{case}: window is inverted or zero-width ({s} -> {e})"
                )
            checked += 1

    assert checked > 0, "the enumeration planned no windows at all — vacuous"


def test_future_start_always_heals_on_the_self_healing_path():
    """The precise anti-stall invariant, stated after analysing a near-miss.

    Written first as "a future start must ALWAYS plan a window, for every end",
    this FAILED for a future start combined with an explicitly requested PAST
    ``before``. That looked like a seventh sibling; it is not, and the
    distinction is the whole point:

      - SCHEDULED runs send no ``before``, so the end is ``now``. This is the
        only path that repeats unattended, so it is the only one where zero units
        would perpetuate forever. It MUST plan a window, and does.
      - A caller that explicitly asks for a bounded PAST range gets zero units.
        That does not perpetuate — the next scheduled tick heals the watermark —
        and planning a window anyway would fabricate a range the caller never
        requested, which is how false coverage claims get made.

    So the invariant is not "always plans", it is "always plans on the path that
    would otherwise never recover". Forcing the other case would have been a
    patch that made the system less correct.
    """
    from datetime import timedelta

    from dev_health_ops.sync.planner import _watermark_stamping_window

    now = datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc)
    for start_delta in (timedelta(seconds=1), timedelta(hours=1), timedelta(days=90)):
        # end == now is exactly what the scheduled path resolves to.
        windows = _watermark_stamping_window(now + start_delta, now, now)
        assert len(windows) == 1, (
            f"future start (+{start_delta}) planned no unit on the scheduled "
            "path — that is the permanent stall, not a safe no-op"
        )
        s, e = windows[0]
        assert s is not None and e is not None
        assert _as_utc_for_test(s) < _as_utc_for_test(e) <= now

    # The analysed non-defect, pinned so it stays a deliberate choice.
    assert (
        _watermark_stamping_window(
            now + timedelta(days=30), now - timedelta(days=10), now
        )
        == ()
    ), "an explicit past `before` should still plan nothing; it self-heals later"
