from __future__ import annotations

import os
import threading
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, delete
from sqlalchemy.orm import Session

from dev_health_ops.api.services.sync_coverage import (
    SYNC_COVERAGE_PROJECTION_VERSION,
    _sync_coverage_lock_statement,
    invalidate_sync_coverage_projection_sync,
)
from dev_health_ops.models.integrations import Integration
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.models.sync_coverage import SyncCoverageProjection
from tests._helpers import sync_postgres_test_url


@pytest.mark.skipif(
    not os.getenv("DEV_HEALTH_POSTGRES_TEST_URI"),
    reason="requires DEV_HEALTH_POSTGRES_TEST_URI",
)
def test_invalidation_waits_for_inflight_projection_publication():
    """A rebuild cannot publish over an invalidation that began mid-scan."""

    engine = create_engine(sync_postgres_test_url())
    org_id = str(uuid.uuid4())
    integration_id: uuid.UUID | None = None
    config_id: uuid.UUID | None = None
    projection_id: uuid.UUID | None = None
    invalidation_started = threading.Event()
    invalidation_finished = threading.Event()
    invalidation_errors: list[BaseException] = []
    try:
        with Session(engine) as setup_session:
            integration = Integration(
                org_id=org_id,
                provider="github",
                name=f"coverage-concurrency-{org_id}",
                config={},
                is_active=True,
            )
            setup_session.add(integration)
            setup_session.flush()
            config = SyncConfiguration(
                org_id=org_id,
                name=f"coverage-concurrency-{org_id}",
                provider="github",
                sync_targets=["git"],
                integration_id=integration.id,
                planner_managed=True,
            )
            setup_session.add(config)
            setup_session.flush()
            projection = SyncCoverageProjection(
                org_id=org_id,
                sync_config_id=config.id,
                history_lookback_days=3650,
                projection_version=SYNC_COVERAGE_PROJECTION_VERSION,
                generated_at=datetime.now(timezone.utc),
                payload={"generation": "before"},
            )
            setup_session.add(projection)
            setup_session.commit()
            integration_id = integration.id
            config_id = config.id
            projection_id = projection.id

        assert config_id is not None
        assert projection_id is not None

        def invalidate() -> None:
            try:
                with Session(engine) as invalidation_session:
                    invalidation_started.set()
                    invalidate_sync_coverage_projection_sync(
                        invalidation_session,
                        org_id,
                        sync_config_id=config_id,
                    )
                    invalidation_session.commit()
            except BaseException as exc:  # pragma: no cover - surfaced below
                invalidation_errors.append(exc)
            finally:
                invalidation_finished.set()

        with Session(engine) as rebuild_session:
            rebuild_session.execute(_sync_coverage_lock_statement(org_id, config_id))
            invalidator = threading.Thread(target=invalidate, daemon=True)
            invalidator.start()
            assert invalidation_started.wait(timeout=5)
            assert not invalidation_finished.wait(timeout=0.2)

            rebuild_projection = rebuild_session.get(
                SyncCoverageProjection, projection_id
            )
            assert rebuild_projection is not None
            rebuild_projection.payload = {"generation": "rebuilt"}
            rebuild_projection.invalidated_at = None
            rebuild_session.commit()

        assert invalidation_finished.wait(timeout=5)
        invalidator.join(timeout=5)
        assert invalidation_errors == []
        with Session(engine) as verify_session:
            verified_projection = verify_session.get(
                SyncCoverageProjection, projection_id
            )
            assert verified_projection is not None
            assert verified_projection.payload == {"generation": "rebuilt"}
            assert verified_projection.invalidated_at is not None
    finally:
        with Session(engine) as cleanup_session:
            if projection_id is not None:
                cleanup_session.execute(
                    delete(SyncCoverageProjection).where(
                        SyncCoverageProjection.id == projection_id
                    )
                )
            if config_id is not None:
                cleanup_session.execute(
                    delete(SyncConfiguration).where(SyncConfiguration.id == config_id)
                )
            if integration_id is not None:
                cleanup_session.execute(
                    delete(Integration).where(Integration.id == integration_id)
                )
            cleanup_session.commit()
        engine.dispose()
