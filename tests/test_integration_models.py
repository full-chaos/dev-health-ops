from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
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
from dev_health_ops.sync.watermarks import (
    get_legacy_repo_watermark,
    get_watermark,
    set_legacy_repo_watermark,
)

ORG_ID = "default"


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _integration(session: Session) -> Integration:
    integration = Integration(
        org_id=ORG_ID,
        provider="github",
        name="GitHub",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    return integration


def _source(session: Session, integration: Integration) -> IntegrationSource:
    source = IntegrationSource(
        org_id=ORG_ID,
        integration_id=integration.id,
        provider="github",
        source_type="repo",
        external_id="full-chaos/dev-health",
        name="dev-health",
        full_name="full-chaos/dev-health",
        metadata_={},
        is_enabled=True,
        discovered_at=datetime.now(timezone.utc),
        last_seen_at=datetime.now(timezone.utc),
    )
    session.add(source)
    session.flush()
    return source


def test_duplicate_integration_source_rejected(db_session):
    integration = _integration(db_session)
    _source(db_session, integration)

    duplicate = IntegrationSource(
        org_id=ORG_ID,
        integration_id=integration.id,
        provider="github",
        source_type="repo",
        external_id="full-chaos/dev-health",
        name="dev-health-copy",
        full_name="full-chaos/dev-health",
        metadata_={},
        is_enabled=True,
        discovered_at=datetime.now(timezone.utc),
        last_seen_at=datetime.now(timezone.utc),
    )
    db_session.add(duplicate)

    with pytest.raises(IntegrityError):
        db_session.flush()


def test_duplicate_integration_dataset_rejected(db_session):
    integration = _integration(db_session)
    first = IntegrationDataset(
        org_id=ORG_ID,
        integration_id=integration.id,
        dataset_key="prs",
        is_enabled=True,
        options={},
    )
    duplicate = IntegrationDataset(
        org_id=ORG_ID,
        integration_id=integration.id,
        dataset_key="prs",
        is_enabled=False,
        options={},
    )
    db_session.add_all([first, duplicate])

    with pytest.raises(IntegrityError):
        db_session.flush()


def test_sync_run_unit_queryable_by_run_source_dataset_status(db_session):
    integration = _integration(db_session)
    source = _source(db_session, integration)
    sync_run = SyncRun(
        org_id=ORG_ID,
        integration_id=integration.id,
        triggered_by="manual",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.RUNNING.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    db_session.add(sync_run)
    db_session.flush()

    unit = SyncRunUnit(
        org_id=ORG_ID,
        sync_run_id=sync_run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider="github",
        dataset_key="prs",
        cost_class="standard",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
    )
    db_session.add(unit)
    db_session.flush()

    found = (
        db_session.query(SyncRunUnit)
        .filter(
            SyncRunUnit.sync_run_id == sync_run.id,
            SyncRunUnit.source_id == source.id,
            SyncRunUnit.dataset_key == "prs",
            SyncRunUnit.status == SyncRunUnitStatus.PLANNED.value,
        )
        .one()
    )
    assert found.id == unit.id
    assert found.mode == SyncRunMode.INCREMENTAL.value


def test_legacy_watermark_wrapper_visible_via_new_api(db_session):
    ts = datetime(2026, 6, 17, 12, 0, 0, tzinfo=timezone.utc)

    set_legacy_repo_watermark(db_session, ORG_ID, "full-chaos/dev-health", "prs", ts)

    legacy = get_legacy_repo_watermark(
        db_session, ORG_ID, "full-chaos/dev-health", "prs"
    )
    generalized = get_watermark(db_session, ORG_ID, "full-chaos/dev-health", "prs")
    assert legacy is not None
    assert generalized is not None
    assert legacy.replace(tzinfo=None) == ts.replace(tzinfo=None)
    assert generalized.replace(tzinfo=None) == ts.replace(tzinfo=None)
