from __future__ import annotations

import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from celery.exceptions import Retry

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

_fake_croniter_mod = MagicMock()
if "croniter" not in sys.modules:
    sys.modules["croniter"] = _fake_croniter_mod

from dev_health_ops.models.git import Base  # noqa: E402
from dev_health_ops.models.settings import (  # noqa: E402
    SyncConfiguration,
    SyncWatermark,
)
from dev_health_ops.sync.watermarks import get_watermark, set_watermark  # noqa: E402

ORG_ID = "default"
REPO_ID = "my-org/my-repo"


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@contextmanager
def _fake_session_ctx(session):
    yield session


def _make_config(
    provider: str = "github",
    sync_options: dict | None = None,
    sync_targets: list | None = None,
    is_active: bool = True,
    name: str = "test-config",
    org_id: str = "default",
) -> SyncConfiguration:
    return SyncConfiguration(
        name=name,
        provider=provider,
        org_id=org_id,
        sync_targets=sync_targets or ["git", "prs"],
        sync_options=sync_options or {},
        is_active=is_active,
    )


class TestSyncWatermarkModel:
    def test_create_watermark(self, db_session):
        wm = SyncWatermark(repo_id=REPO_ID, target="git", org_id=ORG_ID)
        db_session.add(wm)
        db_session.flush()

        assert wm.id is not None
        assert wm.org_id == ORG_ID
        assert wm.repo_id == REPO_ID
        assert wm.target == "git"
        assert wm.last_synced_at is None
        assert wm.updated_at is not None

    def test_create_watermark_with_timestamp(self, db_session):
        ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm = SyncWatermark(
            repo_id=REPO_ID, target="prs", org_id=ORG_ID, last_synced_at=ts
        )
        db_session.add(wm)
        db_session.flush()

        assert wm.last_synced_at == ts

    def test_unique_constraint_on_org_repo_target(self, db_session):
        wm1 = SyncWatermark(repo_id=REPO_ID, target="git", org_id=ORG_ID)
        db_session.add(wm1)
        db_session.flush()

        wm2 = SyncWatermark(repo_id=REPO_ID, target="git", org_id=ORG_ID)
        db_session.add(wm2)
        with pytest.raises(Exception):
            db_session.flush()

    def test_different_targets_allowed(self, db_session):
        wm1 = SyncWatermark(repo_id=REPO_ID, target="git", org_id=ORG_ID)
        wm2 = SyncWatermark(repo_id=REPO_ID, target="prs", org_id=ORG_ID)
        db_session.add_all([wm1, wm2])
        db_session.flush()

        count = db_session.query(SyncWatermark).count()
        assert count == 2


class TestGetWatermark:
    def test_returns_none_for_new_repo(self, db_session):
        result = get_watermark(db_session, ORG_ID, REPO_ID, "git")
        assert result is None

    def test_returns_timestamp_when_exists(self, db_session):
        ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm = SyncWatermark(
            repo_id=REPO_ID, target="git", org_id=ORG_ID, last_synced_at=ts
        )
        db_session.add(wm)
        db_session.flush()

        result = get_watermark(db_session, ORG_ID, REPO_ID, "git")
        assert result == ts

    def test_returns_none_for_different_target(self, db_session):
        ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm = SyncWatermark(
            repo_id=REPO_ID, target="git", org_id=ORG_ID, last_synced_at=ts
        )
        db_session.add(wm)
        db_session.flush()

        result = get_watermark(db_session, ORG_ID, REPO_ID, "prs")
        assert result is None


class TestSetWatermark:
    def test_creates_new_watermark(self, db_session):
        ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        set_watermark(db_session, ORG_ID, REPO_ID, "git", ts)

        result = get_watermark(db_session, ORG_ID, REPO_ID, "git")
        assert result is not None
        assert result.replace(tzinfo=None) == ts.replace(tzinfo=None)

    def test_upserts_existing_watermark(self, db_session):
        ts1 = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2025, 6, 2, 12, 0, 0, tzinfo=timezone.utc)

        set_watermark(db_session, ORG_ID, REPO_ID, "git", ts1)
        set_watermark(db_session, ORG_ID, REPO_ID, "git", ts2)

        result = get_watermark(db_session, ORG_ID, REPO_ID, "git")
        assert result is not None
        assert result.replace(tzinfo=None) == ts2.replace(tzinfo=None)

        count = db_session.query(SyncWatermark).count()
        assert count == 1

    def test_different_targets_independent(self, db_session):
        ts1 = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2025, 6, 2, 12, 0, 0, tzinfo=timezone.utc)

        set_watermark(db_session, ORG_ID, REPO_ID, "git", ts1)
        set_watermark(db_session, ORG_ID, REPO_ID, "prs", ts2)

        r1 = get_watermark(db_session, ORG_ID, REPO_ID, "git")
        r2 = get_watermark(db_session, ORG_ID, REPO_ID, "prs")
        assert r1 is not None and r1.replace(tzinfo=None) == ts1.replace(tzinfo=None)
        assert r2 is not None and r2.replace(tzinfo=None) == ts2.replace(tzinfo=None)


class TestRunSyncConfigWatermarks:
    @patch("dev_health_ops.storage.run_with_store")
    @patch("dev_health_ops.workers.tasks._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.tasks._resolve_env_credentials",
        return_value={"token": "ghp_test"},
    )
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_passes_since_from_watermark(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_post_sync,
        mock_run_with_store,
        db_session,
    ):
        from dev_health_ops.workers.tasks import run_sync_config

        config = _make_config(
            provider="github",
            sync_options={"owner": "my-org", "repo": "my-repo"},
            sync_targets=["git", "prs"],
        )
        db_session.add(config)
        db_session.flush()

        ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        set_watermark(db_session, ORG_ID, "my-org/my-repo", "git", ts)
        set_watermark(db_session, ORG_ID, "my-org/my-repo", "prs", ts)

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
        mock_run_with_store.return_value = None

        task = run_sync_config
        task.push_request(id="watermark-test-1")
        try:
            result = task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        assert result["status"] == "success"
        mock_run_with_store.assert_called_once()
        git_wm = get_watermark(db_session, ORG_ID, "my-org/my-repo", "git")
        prs_wm = get_watermark(db_session, ORG_ID, "my-org/my-repo", "prs")
        assert git_wm is not None
        assert prs_wm is not None

    @patch("dev_health_ops.storage.run_with_store")
    @patch("dev_health_ops.workers.tasks._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.tasks._resolve_env_credentials",
        return_value={"token": "ghp_test"},
    )
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_full_resync_skips_watermark_reading(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_post_sync,
        mock_run_with_store,
        db_session,
    ):
        from dev_health_ops.workers.tasks import run_sync_config

        config = _make_config(
            provider="github",
            sync_options={
                "owner": "my-org",
                "repo": "my-repo",
                "full_resync": True,
            },
            sync_targets=["git"],
        )
        db_session.add(config)
        db_session.flush()

        ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        set_watermark(db_session, ORG_ID, "my-org/my-repo", "git", ts)

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
        mock_run_with_store.return_value = None

        task = run_sync_config
        task.push_request(id="watermark-test-2")
        try:
            result = task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        assert result["status"] == "success"

    @patch("dev_health_ops.storage.run_with_store")
    @patch("dev_health_ops.workers.tasks._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.tasks._resolve_env_credentials",
        return_value={"token": "ghp_test"},
    )
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_watermarks_written_after_success(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_post_sync,
        mock_run_with_store,
        db_session,
    ):
        from dev_health_ops.workers.tasks import run_sync_config

        config = _make_config(
            provider="github",
            sync_options={"owner": "my-org", "repo": "my-repo"},
            sync_targets=["git", "prs"],
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
        mock_run_with_store.return_value = None

        task = run_sync_config
        task.push_request(id="watermark-test-3")
        try:
            result = task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        assert result["status"] == "success"
        git_wm = get_watermark(db_session, ORG_ID, "my-org/my-repo", "git")
        prs_wm = get_watermark(db_session, ORG_ID, "my-org/my-repo", "prs")
        assert git_wm is not None
        assert prs_wm is not None

    @patch("dev_health_ops.storage.run_with_store")
    @patch(
        "dev_health_ops.workers.tasks._resolve_env_credentials",
        return_value={"token": "ghp_test"},
    )
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_watermarks_not_written_on_failure(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_run_with_store,
        db_session,
    ):
        from dev_health_ops.workers.tasks import run_sync_config

        config = _make_config(
            provider="github",
            sync_options={"owner": "my-org", "repo": "my-repo"},
            sync_targets=["git"],
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
        mock_run_with_store.side_effect = RuntimeError("API failure")

        task = run_sync_config
        task.push_request(id="watermark-test-4")
        try:
            with pytest.raises((Retry, RuntimeError)):
                task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        git_wm = get_watermark(db_session, ORG_ID, "my-org/my-repo", "git")
        assert git_wm is None

    @patch("dev_health_ops.metrics.job_work_items.run_work_items_sync_job")
    @patch("dev_health_ops.workers.tasks._dispatch_post_sync_tasks")
    @patch("dev_health_ops.workers.tasks._resolve_env_credentials", return_value={})
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_no_watermark_when_repo_id_missing(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_post_sync,
        mock_run_work_items,
        db_session,
    ):
        from dev_health_ops.workers.tasks import run_sync_config

        config = _make_config(
            provider="jira",
            sync_options={"backfill_days": 1},
            sync_targets=["work-items"],
            name="jira-config",
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)

        task = run_sync_config
        task.push_request(id="watermark-test-5")
        try:
            result = task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        assert result["status"] == "success"
        count = db_session.query(SyncWatermark).count()
        assert count == 0

    @patch("dev_health_ops.storage.run_with_store")
    @patch("dev_health_ops.workers.tasks._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.tasks._resolve_env_credentials",
        return_value={"token": "ghp_test"},
    )
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_since_is_none_when_partial_watermarks(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_post_sync,
        mock_run_with_store,
        db_session,
    ):
        from dev_health_ops.workers.tasks import run_sync_config

        config = _make_config(
            provider="github",
            sync_options={"owner": "my-org", "repo": "my-repo"},
            sync_targets=["git", "prs"],
        )
        db_session.add(config)
        db_session.flush()

        ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        set_watermark(db_session, ORG_ID, "my-org/my-repo", "git", ts)

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
        mock_run_with_store.return_value = None

        task = run_sync_config
        task.push_request(id="watermark-test-6")
        try:
            result = task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        assert result["status"] == "success"
        git_wm = get_watermark(db_session, ORG_ID, "my-org/my-repo", "git")
        prs_wm = get_watermark(db_session, ORG_ID, "my-org/my-repo", "prs")
        assert git_wm is not None
        assert prs_wm is not None

    @patch("dev_health_ops.storage.run_with_store")
    @patch("dev_health_ops.workers.tasks._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.tasks._resolve_env_credentials",
        return_value={"token": "glpat_test"},
    )
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_gitlab_watermark_uses_project_id(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_post_sync,
        mock_run_with_store,
        db_session,
    ):
        from dev_health_ops.workers.tasks import run_sync_config

        config = _make_config(
            provider="gitlab",
            sync_options={
                "project_id": 42,
                "gitlab_url": "https://gitlab.com",
            },
            sync_targets=["git"],
            name="gitlab-config",
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
        mock_run_with_store.return_value = None

        task = run_sync_config
        task.push_request(id="watermark-test-7")
        try:
            result = task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        assert result["status"] == "success"
        git_wm = get_watermark(db_session, ORG_ID, "42", "git")
        assert git_wm is not None
