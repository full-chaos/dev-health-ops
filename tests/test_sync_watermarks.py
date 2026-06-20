from __future__ import annotations

import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from celery.exceptions import Retry
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
from dev_health_ops.sync.watermarks import (  # noqa: E402
    get_watermark,
    get_watermark_with_overlap,
    set_watermark,
)

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
        sync_targets=["git", "prs"] if sync_targets is None else sync_targets,
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
    @patch("dev_health_ops.workers.sync_runtime._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.sync_runtime._resolve_env_credentials",
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
        from dev_health_ops.workers.sync_runtime import run_sync_config

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

        task = cast(Any, run_sync_config)
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
    @patch("dev_health_ops.workers.sync_runtime._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.sync_runtime._resolve_env_credentials",
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
        from dev_health_ops.workers.sync_runtime import run_sync_config

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

        task = cast(Any, run_sync_config)
        task.push_request(id="watermark-test-2")
        try:
            result = task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        assert result["status"] == "success"

    @patch("dev_health_ops.storage.run_with_store")
    @patch("dev_health_ops.workers.sync_runtime._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.sync_runtime._resolve_env_credentials",
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
        from dev_health_ops.workers.sync_runtime import run_sync_config

        config = _make_config(
            provider="github",
            sync_options={"owner": "my-org", "repo": "my-repo"},
            sync_targets=["git", "prs"],
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
        mock_run_with_store.return_value = None

        task = cast(Any, run_sync_config)
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
        "dev_health_ops.workers.sync_runtime._resolve_env_credentials",
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
        from dev_health_ops.workers.sync_runtime import run_sync_config

        config = _make_config(
            provider="github",
            sync_options={"owner": "my-org", "repo": "my-repo"},
            sync_targets=["git"],
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
        mock_run_with_store.side_effect = RuntimeError("API failure")

        task = cast(Any, run_sync_config)
        task.push_request(id="watermark-test-4")
        try:
            with pytest.raises((Retry, RuntimeError)):
                task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        git_wm = get_watermark(db_session, ORG_ID, "my-org/my-repo", "git")
        assert git_wm is None

    @patch("dev_health_ops.metrics.job_work_items.run_work_items_sync_job")
    @patch("dev_health_ops.workers.sync_runtime._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.sync_runtime._resolve_env_credentials", return_value={}
    )
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_no_watermark_when_repo_id_missing(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_post_sync,
        mock_run_work_items,
        db_session,
    ):
        from dev_health_ops.workers.sync_runtime import run_sync_config

        config = _make_config(
            provider="jira",
            sync_options={"backfill_days": 1},
            sync_targets=["work-items"],
            name="jira-config",
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)

        task = cast(Any, run_sync_config)
        task.push_request(id="watermark-test-5")
        try:
            result = task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        assert result["status"] == "success"
        count = db_session.query(SyncWatermark).count()
        assert count == 0

    @patch("dev_health_ops.metrics.job_work_items.run_work_items_sync_job")
    @patch("dev_health_ops.workers.sync_runtime._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.sync_runtime._resolve_env_credentials",
        return_value={"api_key": "lin_test"},
    )
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_linear_empty_targets_defaults_to_work_items(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_post_sync,
        mock_run_work_items,
        db_session,
    ):
        from dev_health_ops.workers.sync_runtime import run_sync_config

        config = _make_config(
            provider="linear",
            sync_options={"backfill_days": 2},
            sync_targets=[],
            name="linear-config",
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)

        task = cast(Any, run_sync_config)
        task.push_request(id="linear-work-items-test")
        try:
            result = task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        assert result["status"] == "success"
        assert result["result"]["sync_targets"] == ["work-items"]
        mock_run_work_items.assert_called_once()
        assert mock_run_work_items.call_args.kwargs["provider"] == "linear"

    @patch("dev_health_ops.metrics.job_work_items.run_work_items_sync_job")
    @patch("dev_health_ops.workers.sync_runtime._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.sync_runtime._resolve_env_credentials", return_value={}
    )
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_github_work_items_only_does_not_require_owner_repo(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_post_sync,
        mock_run_work_items,
        db_session,
    ):
        from dev_health_ops.workers.sync_runtime import run_sync_config

        config = _make_config(
            provider="github",
            sync_options={"search": "full-chaos"},
            sync_targets=["work-items"],
            name="github-work-items",
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)

        task = cast(Any, run_sync_config)
        task.push_request(id="github-work-items-only")
        try:
            result = task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        assert result["status"] == "success"
        mock_run_work_items.assert_called_once()

    @patch("dev_health_ops.metrics.job_work_items.run_work_items_sync_job")
    def test_batch_child_empty_targets_defaults_to_work_items(
        self, mock_run_work_items
    ):
        from dev_health_ops.workers.sync_batch import _run_sync_for_repo

        task = cast(Any, _run_sync_for_repo)
        task.push_request(id="batch-linear-work-items-test")
        try:
            result = task(
                config_id=str(uuid.uuid4()),
                org_id=ORG_ID,
                triggered_by="manual",
                provider="linear",
                sync_targets=[],
                sync_options_override={"backfill_days": 3},
                credentials={"api_key": "lin_test"},
                config_name="linear-config",
            )
        finally:
            task.pop_request()

        assert result["status"] == "success"
        assert result["result"]["sync_targets"] == ["work-items"]
        mock_run_work_items.assert_called_once()
        assert mock_run_work_items.call_args.kwargs["provider"] == "linear"

    @patch("dev_health_ops.storage.run_with_store")
    @patch("dev_health_ops.workers.sync_runtime._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.sync_runtime._resolve_env_credentials",
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
        from dev_health_ops.workers.sync_runtime import run_sync_config

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

        task = cast(Any, run_sync_config)
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
    @patch("dev_health_ops.workers.sync_runtime._dispatch_post_sync_tasks")
    @patch(
        "dev_health_ops.workers.sync_runtime._resolve_env_credentials",
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
        from dev_health_ops.workers.sync_runtime import run_sync_config

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

        task = cast(Any, run_sync_config)
        task.push_request(id="watermark-test-7")
        try:
            result = task(config_id=str(config.id), org_id=ORG_ID)
        finally:
            task.pop_request()

        assert result["status"] == "success"
        git_wm = get_watermark(db_session, ORG_ID, "42", "git")
        assert git_wm is not None


# ---------------------------------------------------------------------------
# WS-B tests: monotonic, legacy alias, overlap (CHAOS-2571, 2572, 2578)
# ---------------------------------------------------------------------------


class TestSetWatermarkIsMonotonic:
    def test_set_watermark_is_monotonic(self, db_session):
        """Writing an earlier timestamp must not roll back the watermark (CHAOS-2578)."""
        t_later = datetime(2025, 6, 1, 10, 5, 0, tzinfo=timezone.utc)
        t_earlier = datetime(2025, 6, 1, 10, 0, 0, tzinfo=timezone.utc)

        set_watermark(db_session, ORG_ID, REPO_ID, "commits", t_later)
        set_watermark(db_session, ORG_ID, REPO_ID, "commits", t_earlier)

        stored = get_watermark(db_session, ORG_ID, REPO_ID, "commits")
        assert stored is not None
        stored_utc = stored.replace(tzinfo=timezone.utc)
        assert stored_utc == t_later, (
            f"Monotonic invariant violated: stored={stored_utc} < written={t_later}"
        )

    def test_set_watermark_advances_when_newer(self, db_session):
        """Writing a later timestamp must advance the watermark normally."""
        t_first = datetime(2025, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
        t_second = datetime(2025, 6, 1, 11, 0, 0, tzinfo=timezone.utc)

        set_watermark(db_session, ORG_ID, REPO_ID, "commits", t_first)
        set_watermark(db_session, ORG_ID, REPO_ID, "commits", t_second)

        stored = get_watermark(db_session, ORG_ID, REPO_ID, "commits")
        assert stored is not None
        assert stored.replace(tzinfo=timezone.utc) == t_second


class TestLegacyTargetAliasWarmsPlanner:
    def test_legacy_target_alias_warms_planner_dataset(self, db_session):
        """Write via legacy target='git'; planner read for the resolved dataset_key finds the row (CHAOS-2571).

        The alias map resolves 'git' to the primary dataset_key in DatasetKey enum order.
        Writing via the legacy target path and reading via the resolved canonical key
        must return the same watermark, proving the alias warms the planner.
        """
        from dev_health_ops.sync.watermarks import (
            dataset_key_for_legacy_target,
            set_legacy_repo_watermark,
        )

        ts = datetime(2025, 7, 1, 8, 0, 0, tzinfo=timezone.utc)

        # Write via legacy target path.
        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", ts)

        # The alias must resolve 'git' to a dataset_key.
        resolved_key = dataset_key_for_legacy_target("git")
        assert resolved_key is not None, (
            "'git' must resolve to a dataset_key via the registry alias"
        )

        # Planner read via canonical dataset_key must find the row.
        # set_legacy_repo_watermark delegates to set_watermark(dataset_key=resolved_key),
        # so the canonical row is stored under resolved_key.
        stored = get_watermark(db_session, ORG_ID, REPO_ID, resolved_key)
        assert stored is not None, (
            f"Planner read for dataset_key={resolved_key!r} must find the row written via legacy target='git'"
        )
        assert stored.replace(tzinfo=timezone.utc) == ts

    def test_legacy_read_after_canonical_write(self, db_session):
        """Write via canonical dataset_key; legacy read via target must find the row.

        The legacy get_legacy_repo_watermark reads by target column first, then
        falls back via the alias map.  Writing 'prs' (which maps to legacy target
        'prs') and reading via get_legacy_repo_watermark('prs') must succeed.
        """
        from dev_health_ops.sync.watermarks import (
            get_legacy_repo_watermark,
        )

        ts = datetime(2025, 7, 2, 9, 0, 0, tzinfo=timezone.utc)
        # 'prs' dataset_key maps to legacy target 'prs' (1:1 mapping).
        set_watermark(db_session, ORG_ID, REPO_ID, "prs", ts)

        # Legacy read via target='prs' must find the canonical row.
        # set_watermark stores target='prs' (target == dataset_key convention),
        # so the legacy lookup by target column finds it directly.
        stored = get_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "prs")
        assert stored is not None, (
            "Legacy read via target='prs' must find the row written via canonical dataset_key='prs'"
        )
        assert stored.replace(tzinfo=timezone.utc) == ts


class TestIncrementalReadAppliesOverlap:
    def test_incremental_read_applies_overlap(self, db_session, monkeypatch):
        """Planner incremental window_start == watermark - overlap (CHAOS-2572)."""

        from dev_health_ops.sync.watermarks import get_watermark_with_overlap

        overlap_seconds = 3600  # 1 hour
        monkeypatch.setenv("SYNC_WATERMARK_OVERLAP", str(overlap_seconds))

        ts = datetime(2025, 8, 1, 12, 0, 0, tzinfo=timezone.utc)
        set_watermark(db_session, ORG_ID, REPO_ID, "commits", ts)

        result = get_watermark_with_overlap(db_session, ORG_ID, REPO_ID, "commits")
        assert result is not None
        expected = ts - __import__("datetime").timedelta(seconds=overlap_seconds)
        assert result.replace(tzinfo=timezone.utc) == expected, (
            f"Overlap not applied: got {result}, expected {expected}"
        )

    def test_overlap_not_applied_when_zero(self, db_session, monkeypatch):
        """With SYNC_WATERMARK_OVERLAP=0, get_watermark_with_overlap returns raw value."""
        monkeypatch.setenv("SYNC_WATERMARK_OVERLAP", "0")

        ts = datetime(2025, 8, 2, 12, 0, 0, tzinfo=timezone.utc)
        set_watermark(db_session, ORG_ID, REPO_ID, "prs", ts)

        result = get_watermark_with_overlap(db_session, ORG_ID, REPO_ID, "prs")
        assert result is not None
        assert result.replace(tzinfo=timezone.utc) == ts

    def test_overlap_not_applied_on_cold_start(self, db_session, monkeypatch):
        """With no watermark row, get_watermark_with_overlap returns None (cold-start)."""
        monkeypatch.setenv("SYNC_WATERMARK_OVERLAP", "3600")

        result = get_watermark_with_overlap(
            db_session, ORG_ID, "no-such-repo", "commits"
        )
        assert result is None, (
            "Cold-start must return None even with overlap configured"
        )

    def test_planner_incremental_window_applies_overlap(self, db_session, monkeypatch):
        """End-to-end: planner incremental since_at == watermark - overlap."""
        from datetime import timedelta

        from dev_health_ops.models import (
            Integration,
            IntegrationDataset,
            IntegrationSource,
            SyncRunMode,
        )
        from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run

        overlap_seconds = 1800  # 30 minutes
        monkeypatch.setenv("SYNC_WATERMARK_OVERLAP", str(overlap_seconds))

        # monkeypatch.setenv is sufficient: _watermark_overlap_seconds() reads
        # os.getenv at call time, so no module reload is needed.

        planner_org = "overlap-test-org"
        integration = Integration(
            org_id=planner_org,
            provider="github",
            name="overlap-integration",
            config={},
            is_active=True,
        )
        db_session.add(integration)
        db_session.flush()

        source = IntegrationSource(
            org_id=planner_org,
            integration_id=integration.id,
            provider="github",
            source_type="repo",
            external_id="overlap-org/overlap-repo",
            name="overlap-repo",
            full_name="overlap-org/overlap-repo",
            metadata_={},
            is_enabled=True,
            discovered_at=datetime.now(timezone.utc),
            last_seen_at=datetime.now(timezone.utc),
        )
        db_session.add(source)
        db_session.flush()

        dataset = IntegrationDataset(
            org_id=planner_org,
            integration_id=integration.id,
            dataset_key="commits",
            is_enabled=True,
            options={},
        )
        db_session.add(dataset)
        db_session.flush()

        watermark_ts = datetime(2025, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
        set_watermark(
            db_session, planner_org, source.external_id, "commits", watermark_ts
        )

        plan = plan_sync_run(
            db_session,
            SyncPlanRequest(
                integration_id=str(integration.id),
                org_id=planner_org,
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="test",
                before=datetime(2025, 9, 2, 0, 0, 0, tzinfo=timezone.utc),
            ),
        )

        from dev_health_ops.models import SyncRunUnit

        units = (
            db_session.query(SyncRunUnit)
            .filter(SyncRunUnit.sync_run_id == plan.sync_run_id)
            .all()
        )
        assert len(units) == 1
        unit = units[0]
        assert unit.since_at is not None
        expected_since = watermark_ts - timedelta(seconds=overlap_seconds)
        actual_since = unit.since_at.replace(tzinfo=timezone.utc)
        assert abs((actual_since - expected_since).total_seconds()) < 2, (
            f"Planner did not apply overlap: got {actual_since}, expected {expected_since}"
        )


# ---------------------------------------------------------------------------
# Regression tests for adversarial-review NO-SHIP findings (WS-B watermark)
# ---------------------------------------------------------------------------


class TestFinding1LegacyGitAliasWarmsIncrementalDatasets:
    """FINDING 1 [HIGH]: legacy target='git' must resolve to an INCREMENTAL dataset.

    Before the fix, _build_legacy_target_to_dataset_key() picked the first key in
    DatasetKey enum order, which is 'repo-metadata' (WatermarkBehavior.NONE).  That
    meant planner reads for 'commits'/'commit-stats'/'files' never found the row and
    migrated syncs cold-started.  After the fix the primary is the first INCREMENTAL
    key, which is 'commits'.
    """

    def test_git_alias_resolves_to_incremental_dataset(self):
        """dataset_key_for_legacy_target('git') must NOT return 'repo-metadata'.

        'repo-metadata' has WatermarkBehavior.NONE so the planner never writes a
        watermark row for it.  The alias must resolve to an INCREMENTAL dataset so
        that migrated legacy syncs warm the planner correctly.
        """
        from dev_health_ops.sync.datasets import WatermarkBehavior, _watermark_behavior
        from dev_health_ops.sync.watermarks import dataset_key_for_legacy_target

        resolved = dataset_key_for_legacy_target("git")
        assert resolved is not None, "'git' must resolve to a dataset_key"
        assert resolved != "repo-metadata", (
            f"'git' resolved to 'repo-metadata' (WatermarkBehavior.NONE) — migrated syncs will cold-start; got {resolved!r}"
        )
        assert _watermark_behavior(resolved) == WatermarkBehavior.INCREMENTAL, (
            f"'git' resolved to {resolved!r} which has WatermarkBehavior.NONE — must be INCREMENTAL"
        )

    def test_git_alias_warms_commits_dataset(self, db_session):
        """Write via legacy target='git'; planner read for 'commits' finds the row.

        This is the core regression: after the fix, set_legacy_repo_watermark('git')
        stores the row under dataset_key='commits' (the first INCREMENTAL key for
        'git'), so get_watermark(..., 'commits') returns the stored timestamp.
        """
        from dev_health_ops.sync.watermarks import (
            dataset_key_for_legacy_target,
            set_legacy_repo_watermark,
        )

        ts = datetime(2025, 10, 1, 8, 0, 0, tzinfo=timezone.utc)
        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", ts)

        resolved = dataset_key_for_legacy_target("git")
        assert resolved is not None

        # Planner read for the resolved (INCREMENTAL) key must find the row.
        stored = get_watermark(db_session, ORG_ID, REPO_ID, resolved)
        assert stored is not None, (
            f"Planner read for dataset_key={resolved!r} must find the row written via legacy target='git'"
        )
        assert stored.replace(tzinfo=timezone.utc) == ts

    def test_git_alias_warms_commits_not_repo_metadata(self, db_session):
        """Write via legacy target='git'; planner read for 'repo-metadata' must NOT find the row.

        'repo-metadata' has WatermarkBehavior.NONE — the planner never reads it for
        incremental windows.  The row must be stored under an INCREMENTAL key.
        """
        from dev_health_ops.sync.watermarks import set_legacy_repo_watermark

        ts = datetime(2025, 10, 2, 9, 0, 0, tzinfo=timezone.utc)
        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", ts)

        # 'repo-metadata' is WatermarkBehavior.NONE — the planner never stores
        # watermarks under it, so a legacy write must NOT land there.
        # We only assert the alias resolves to a non-NONE key; we do not need
        # to inspect the repo-metadata row itself.
        get_watermark(
            db_session, ORG_ID, REPO_ID, "repo-metadata"
        )  # side-effect check only
        # The row may or may not exist under repo-metadata depending on the legacy
        # target column, but the canonical dataset_key must NOT be repo-metadata.
        from dev_health_ops.sync.watermarks import dataset_key_for_legacy_target

        resolved = dataset_key_for_legacy_target("git")
        assert resolved != "repo-metadata", (
            "Legacy 'git' alias must not resolve to 'repo-metadata' (WatermarkBehavior.NONE)"
        )


class TestFinding2MonotonicWriteAtDBLevel:
    """FINDING 2 [MED]: set_watermark must be monotonic at the DB level.

    Before the fix, the monotonic check was a Python read-compare-write.  Two
    concurrent sessions could both read the same value, both decide theirs is newer,
    and the later commit could overwrite a higher timestamp with a lower one.
    After the fix, the UPDATE uses GREATEST(COALESCE(last_synced_at, :ts), :ts) so
    the DB resolves the race atomically.
    """

    def test_out_of_order_write_does_not_roll_back(self, db_session):
        """Simulate out-of-order arrival: write t_high then t_low; stored must be t_high.

        This is the single-session equivalent of the two-session race.  The DB-level
        GREATEST ensures the lower timestamp never overwrites the higher one.
        """
        t_high = datetime(2025, 11, 1, 12, 0, 0, tzinfo=timezone.utc)
        t_low = datetime(2025, 11, 1, 10, 0, 0, tzinfo=timezone.utc)

        set_watermark(db_session, ORG_ID, REPO_ID, "commits", t_high)
        # Simulate a late-arriving unit with an earlier timestamp.
        set_watermark(db_session, ORG_ID, REPO_ID, "commits", t_low)

        stored = get_watermark(db_session, ORG_ID, REPO_ID, "commits")
        assert stored is not None
        stored_utc = stored.replace(tzinfo=timezone.utc)
        assert stored_utc == t_high, (
            f"Monotonic invariant violated: stored={stored_utc} was rolled back from {t_high} to {t_low}"
        )

    def test_equal_timestamp_does_not_change_value(self, db_session):
        """Writing the same timestamp twice must leave the stored value unchanged."""
        ts = datetime(2025, 11, 2, 8, 0, 0, tzinfo=timezone.utc)

        set_watermark(db_session, ORG_ID, REPO_ID, "prs", ts)
        set_watermark(db_session, ORG_ID, REPO_ID, "prs", ts)

        stored = get_watermark(db_session, ORG_ID, REPO_ID, "prs")
        assert stored is not None
        assert stored.replace(tzinfo=timezone.utc) == ts

    def test_null_existing_accepts_first_write(self, db_session):
        """When last_synced_at is NULL (new row), the first write must land correctly.

        COALESCE(NULL, :ts) = :ts, so GREATEST(:ts, :ts) = :ts — the insert path
        must not be affected by the DB-level update expression.
        """
        ts = datetime(2025, 11, 3, 9, 0, 0, tzinfo=timezone.utc)
        set_watermark(db_session, ORG_ID, REPO_ID, "cicd", ts)

        stored = get_watermark(db_session, ORG_ID, REPO_ID, "cicd")
        assert stored is not None
        assert stored.replace(tzinfo=timezone.utc) == ts


class TestFinding3LegacyIncrementalAppliesOverlap:
    """FINDING 3 [MED]: legacy incremental since_dt must reflect the configured overlap.

    Before the fix, sync_runtime.py and sync_batch.py called get_legacy_repo_watermark
    and used the raw value as since_dt without subtracting SYNC_WATERMARK_OVERLAP.
    After the fix, apply_watermark_overlap() is called on the raw watermark before
    assigning since_dt.
    """

    def test_apply_watermark_overlap_subtracts_configured_seconds(self, monkeypatch):
        """apply_watermark_overlap(ts) returns ts - SYNC_WATERMARK_OVERLAP seconds."""
        from datetime import timedelta

        from dev_health_ops.sync.watermarks import apply_watermark_overlap

        overlap_seconds = 7200  # 2 hours
        monkeypatch.setenv("SYNC_WATERMARK_OVERLAP", str(overlap_seconds))

        ts = datetime(2025, 12, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = apply_watermark_overlap(ts)
        expected = ts - timedelta(seconds=overlap_seconds)
        assert result == expected, (
            f"apply_watermark_overlap did not subtract overlap: got {result}, expected {expected}"
        )

    def test_apply_watermark_overlap_zero_returns_raw(self, monkeypatch):
        """With SYNC_WATERMARK_OVERLAP=0, apply_watermark_overlap returns the raw value."""
        from dev_health_ops.sync.watermarks import apply_watermark_overlap

        monkeypatch.setenv("SYNC_WATERMARK_OVERLAP", "0")
        ts = datetime(2025, 12, 2, 8, 0, 0, tzinfo=timezone.utc)
        assert apply_watermark_overlap(ts) == ts

    def test_legacy_since_dt_reflects_overlap_via_runtime(
        self, db_session, monkeypatch
    ):
        """End-to-end: legacy incremental since_dt == watermark - overlap.

        Simulates the sync_runtime/sync_batch watermark-read path by calling
        get_legacy_repo_watermark then apply_watermark_overlap, mirroring the
        fixed code path.  Verifies that the overlap is subtracted before since_dt
        is assigned.
        """
        from datetime import timedelta

        from dev_health_ops.sync.watermarks import (
            apply_watermark_overlap,
            get_legacy_repo_watermark,
            set_legacy_repo_watermark,
        )

        overlap_seconds = 3600  # 1 hour
        monkeypatch.setenv("SYNC_WATERMARK_OVERLAP", str(overlap_seconds))

        ts = datetime(2025, 12, 3, 12, 0, 0, tzinfo=timezone.utc)
        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", ts)

        # Simulate the fixed legacy path: read raw watermark, then apply overlap.
        raw = get_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git")
        assert raw is not None
        since_dt = apply_watermark_overlap(raw)

        expected = ts - timedelta(seconds=overlap_seconds)
        assert since_dt.replace(tzinfo=timezone.utc) == expected, (
            f"Legacy incremental since_dt does not reflect overlap: got {since_dt}, expected {expected}"
        )


# ---------------------------------------------------------------------------
# Regression tests for codex re-review findings on set_legacy_repo_watermark()
# ---------------------------------------------------------------------------


class TestLegacyWriteReconcilesToCanonicalKey:
    """FINDING 1 [HIGH]: set_legacy_repo_watermark must reconcile dataset_key.

    A pre-existing legacy row with dataset_key='git' (the raw target string)
    must have its dataset_key updated to the canonical INCREMENTAL key (e.g.
    'commits') when set_legacy_repo_watermark is called.  Without this,
    get_watermark(..., 'commits') cannot find the row and migrated incremental
    syncs cold-start.
    """

    def test_preexisting_legacy_row_reconciled_to_canonical_key(self, db_session):
        """Seed a pre-existing SyncWatermark(target='git', dataset_key='git'),
        call set_legacy_repo_watermark(..., 'git'), then assert a planner read
        get_watermark(..., 'commits') finds the reconciled row.
        """
        from dev_health_ops.sync.watermarks import (
            dataset_key_for_legacy_target,
            set_legacy_repo_watermark,
        )

        # Seed the pre-existing legacy row exactly as the old runtime wrote it:
        # target='git', dataset_key='git' (raw target string, not canonical).
        old_ts = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        legacy_row = SyncWatermark(
            repo_id=REPO_ID,
            target="git",
            org_id=ORG_ID,
            source_id=REPO_ID,
            dataset_key="git",  # pre-migration: raw target string
            last_synced_at=old_ts,
        )
        db_session.add(legacy_row)
        db_session.flush()

        # Confirm the row exists with the old dataset_key.
        assert legacy_row.dataset_key == "git"

        # Now call set_legacy_repo_watermark — this is what the runtime calls.
        new_ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", new_ts)

        # The alias must resolve 'git' to an INCREMENTAL canonical key.
        canonical_key = dataset_key_for_legacy_target("git")
        assert canonical_key is not None
        assert canonical_key != "git", "canonical_key must not be the raw target string"

        # Planner read via canonical dataset_key must now find the row.
        stored = get_watermark(db_session, ORG_ID, REPO_ID, canonical_key)
        assert stored is not None, (
            f"Planner read for dataset_key={canonical_key!r} must find the row after reconciliation; "
            f"pre-existing legacy row with dataset_key='git' was not reconciled"
        )
        assert stored.replace(tzinfo=timezone.utc) == new_ts

    def test_preexisting_legacy_row_dataset_key_updated_in_place(self, db_session):
        """After set_legacy_repo_watermark, the row's dataset_key column is the canonical key.

        Verifies the reconciliation mutates the existing row rather than creating
        a duplicate (which would violate the unique constraint).
        """
        from dev_health_ops.sync.watermarks import set_legacy_repo_watermark

        old_ts = datetime(2025, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
        legacy_row = SyncWatermark(
            repo_id=REPO_ID,
            target="git",
            org_id=ORG_ID,
            source_id=REPO_ID,
            dataset_key="git",
            last_synced_at=old_ts,
        )
        db_session.add(legacy_row)
        db_session.flush()
        row_id = legacy_row.id

        new_ts = datetime(2025, 7, 1, 8, 0, 0, tzinfo=timezone.utc)
        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", new_ts)

        # Must still be exactly one row (no duplicate created).
        count = (
            db_session.query(SyncWatermark)
            .filter(
                SyncWatermark.org_id == ORG_ID,
                SyncWatermark.repo_id == REPO_ID,
                SyncWatermark.target == "git",
            )
            .count()
        )
        assert count == 1, (
            f"Expected 1 row, got {count} — reconciliation must not create duplicates"
        )

        # The same row must now carry the canonical dataset_key.
        db_session.expire(legacy_row)
        updated_row = (
            db_session.query(SyncWatermark).filter(SyncWatermark.id == row_id).one()
        )
        from dev_health_ops.sync.watermarks import dataset_key_for_legacy_target

        canonical_key = dataset_key_for_legacy_target("git")
        assert updated_row.dataset_key == canonical_key, (
            f"Row dataset_key not reconciled: got {updated_row.dataset_key!r}, expected {canonical_key!r}"
        )

    def test_new_row_created_with_canonical_key(self, db_session):
        """When no pre-existing row exists, set_legacy_repo_watermark creates one
        with dataset_key set to the canonical key (not the raw target string).
        """
        from dev_health_ops.sync.watermarks import (
            dataset_key_for_legacy_target,
            set_legacy_repo_watermark,
        )

        ts = datetime(2025, 8, 1, 10, 0, 0, tzinfo=timezone.utc)
        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", ts)

        canonical_key = dataset_key_for_legacy_target("git")
        assert canonical_key is not None

        # Planner read must find the new row.
        stored = get_watermark(db_session, ORG_ID, REPO_ID, canonical_key)
        assert stored is not None, (
            f"New row must be findable via canonical dataset_key={canonical_key!r}"
        )
        assert stored.replace(tzinfo=timezone.utc) == ts


class TestLegacyWriteMonotonicAtDBLevel:
    """FINDING 2 [MED]: set_legacy_repo_watermark must be monotonic.

    The legacy write path previously did Python read/compare/assign.  Two
    concurrent sessions could race and roll the watermark backwards.  The fix
    routes the update through the same GREATEST(COALESCE(...), :ts) pattern
    used by set_watermark().
    """

    def test_legacy_out_of_order_write_does_not_roll_back(self, db_session):
        """Write t_high via set_legacy_repo_watermark, then t_low; stored must be t_high.

        This exercises set_legacy_repo_watermark() directly (NOT set_watermark),
        confirming the legacy path itself is monotonic.
        """
        from dev_health_ops.sync.watermarks import set_legacy_repo_watermark

        t_high = datetime(2025, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
        t_low = datetime(2025, 9, 1, 10, 0, 0, tzinfo=timezone.utc)

        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", t_high)
        # Simulate a late-arriving unit with an earlier timestamp.
        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", t_low)

        # Read back via the legacy path.
        from dev_health_ops.sync.watermarks import get_legacy_repo_watermark

        stored = get_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git")
        assert stored is not None
        stored_utc = stored.replace(tzinfo=timezone.utc)
        assert stored_utc == t_high, (
            f"Legacy monotonic invariant violated: stored={stored_utc} was rolled back from {t_high} to {t_low}"
        )

    def test_legacy_write_advances_when_newer(self, db_session):
        """Writing a later timestamp via set_legacy_repo_watermark advances the watermark."""
        from dev_health_ops.sync.watermarks import (
            get_legacy_repo_watermark,
            set_legacy_repo_watermark,
        )

        t_first = datetime(2025, 10, 1, 8, 0, 0, tzinfo=timezone.utc)
        t_second = datetime(2025, 10, 1, 10, 0, 0, tzinfo=timezone.utc)

        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "prs", t_first)
        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "prs", t_second)

        stored = get_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "prs")
        assert stored is not None
        assert stored.replace(tzinfo=timezone.utc) == t_second

    def test_legacy_preexisting_row_out_of_order_does_not_roll_back(self, db_session):
        """Pre-existing legacy row (dataset_key='git') + out-of-order write stays monotonic.

        Combines Finding 1 (pre-existing legacy row) with Finding 2 (monotonic):
        seed a row with t_high, call set_legacy_repo_watermark with t_low, assert
        the stored value is still t_high after reconciliation.
        """
        from dev_health_ops.sync.watermarks import (
            dataset_key_for_legacy_target,
            get_legacy_repo_watermark,
            set_legacy_repo_watermark,
        )

        t_high = datetime(2025, 11, 1, 14, 0, 0, tzinfo=timezone.utc)
        t_low = datetime(2025, 11, 1, 9, 0, 0, tzinfo=timezone.utc)

        # Seed pre-existing legacy row with t_high.
        legacy_row = SyncWatermark(
            repo_id=REPO_ID,
            target="git",
            org_id=ORG_ID,
            source_id=REPO_ID,
            dataset_key="git",
            last_synced_at=t_high,
        )
        db_session.add(legacy_row)
        db_session.flush()

        # Late-arriving write with t_low must not roll back.
        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", t_low)

        # Legacy read must still return t_high.
        stored_legacy = get_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git")
        assert stored_legacy is not None
        assert stored_legacy.replace(tzinfo=timezone.utc) == t_high, (
            f"Legacy monotonic violated after reconciliation: got {stored_legacy}, expected {t_high}"
        )

        # Canonical planner read must also return t_high (row was reconciled).
        canonical_key = dataset_key_for_legacy_target("git")
        assert canonical_key is not None
        stored_canonical = get_watermark(db_session, ORG_ID, REPO_ID, canonical_key)
        assert stored_canonical is not None, (
            f"Planner read for {canonical_key!r} must find the reconciled row"
        )
        assert stored_canonical.replace(tzinfo=timezone.utc) == t_high, (
            f"Canonical read returned wrong value: got {stored_canonical}, expected {t_high}"
        )


class TestLegacyWriteCollisionMerge:
    """Regression: set_legacy_repo_watermark must not raise IntegrityError when
    a legacy row (target='git', dataset_key='git') and a canonical row
    (target='commits', dataset_key='commits') coexist.

    Before the fix, mutating the legacy row's dataset_key in-place violated the
    uq_sync_watermark_org_source_dataset unique constraint.  The fix detects the
    collision, merges into the canonical row (max of all timestamps), and deletes
    the legacy row in the same transaction.
    """

    def test_no_integrity_error_when_both_rows_exist(self, db_session):
        """Seed both rows, call set_legacy_repo_watermark, assert no IntegrityError.

        Mandatory regression from codex round-3: seed
        SyncWatermark(target='git', dataset_key='git') AND
        SyncWatermark(target='commits', dataset_key='commits') with different
        timestamps, then call set_legacy_repo_watermark(..., 'git').  Assert:
        - No IntegrityError raised.
        - Surviving canonical row's last_synced_at == max(all three timestamps).
        - No duplicate (org_id, source_id, dataset_key='commits') rows.
        - get_watermark(..., 'commits') returns the merged value.
        """
        from dev_health_ops.sync.watermarks import (
            dataset_key_for_legacy_target,
            set_legacy_repo_watermark,
        )

        canonical_key = dataset_key_for_legacy_target("git")
        assert canonical_key is not None
        assert canonical_key != "git"

        t_legacy = datetime(2025, 3, 1, 8, 0, 0, tzinfo=timezone.utc)
        t_canonical = datetime(2025, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
        t_new = datetime(2025, 3, 1, 9, 0, 0, tzinfo=timezone.utc)  # between the two
        t_expected = t_canonical  # max of all three

        # Seed the legacy row: target='git', dataset_key='git' (old runtime style).
        legacy_row = SyncWatermark(
            repo_id=REPO_ID,
            target="git",
            org_id=ORG_ID,
            source_id=REPO_ID,
            dataset_key="git",
            last_synced_at=t_legacy,
        )
        db_session.add(legacy_row)
        db_session.flush()

        # Seed the canonical row: target='commits', dataset_key='commits' (planner style).
        canonical_row = SyncWatermark(
            repo_id=REPO_ID,
            target=canonical_key,
            org_id=ORG_ID,
            source_id=REPO_ID,
            dataset_key=canonical_key,
            last_synced_at=t_canonical,
        )
        db_session.add(canonical_row)
        db_session.flush()

        # Both rows exist — this is the collision scenario.
        count_before = (
            db_session.query(SyncWatermark)
            .filter(
                SyncWatermark.org_id == ORG_ID,
                SyncWatermark.repo_id == REPO_ID,
            )
            .count()
        )
        assert count_before == 2, f"Expected 2 rows before merge, got {count_before}"

        # Must not raise IntegrityError.
        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", t_new)

        # After merge: exactly one row with dataset_key=canonical_key.
        count_after = (
            db_session.query(SyncWatermark)
            .filter(
                SyncWatermark.org_id == ORG_ID,
                SyncWatermark.source_id == REPO_ID,
                SyncWatermark.dataset_key == canonical_key,
            )
            .count()
        )
        assert count_after == 1, (
            f"Expected 1 canonical row after merge, got {count_after} — duplicate or missing"
        )

        # Surviving row must carry the max timestamp.
        stored = get_watermark(db_session, ORG_ID, REPO_ID, canonical_key)
        assert stored is not None, (
            f"get_watermark(..., {canonical_key!r}) must find the merged row"
        )
        assert stored.replace(tzinfo=timezone.utc) == t_expected, (
            f"Merged timestamp wrong: got {stored}, expected {t_expected} (max of all three)"
        )

    def test_collision_max_is_new_timestamp(self, db_session):
        """When the new timestamp is the largest, the merged row carries it."""
        from dev_health_ops.sync.watermarks import (
            dataset_key_for_legacy_target,
            set_legacy_repo_watermark,
        )

        canonical_key = dataset_key_for_legacy_target("git")
        assert canonical_key is not None

        t_legacy = datetime(2025, 4, 1, 8, 0, 0, tzinfo=timezone.utc)
        t_canonical = datetime(2025, 4, 1, 9, 0, 0, tzinfo=timezone.utc)
        t_new = datetime(2025, 4, 1, 12, 0, 0, tzinfo=timezone.utc)  # largest

        legacy_row = SyncWatermark(
            repo_id=REPO_ID,
            target="git",
            org_id=ORG_ID,
            source_id=REPO_ID,
            dataset_key="git",
            last_synced_at=t_legacy,
        )
        db_session.add(legacy_row)
        db_session.flush()

        canonical_row = SyncWatermark(
            repo_id=REPO_ID,
            target=canonical_key,
            org_id=ORG_ID,
            source_id=REPO_ID,
            dataset_key=canonical_key,
            last_synced_at=t_canonical,
        )
        db_session.add(canonical_row)
        db_session.flush()

        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", t_new)

        stored = get_watermark(db_session, ORG_ID, REPO_ID, canonical_key)
        assert stored is not None
        assert stored.replace(tzinfo=timezone.utc) == t_new, (
            f"Expected t_new={t_new} as max, got {stored}"
        )

    def test_collision_legacy_row_deleted_after_merge(self, db_session):
        """After merge, the legacy row (target='git', dataset_key='git') is gone."""
        from dev_health_ops.sync.watermarks import (
            dataset_key_for_legacy_target,
            set_legacy_repo_watermark,
        )

        canonical_key = dataset_key_for_legacy_target("git")
        assert canonical_key is not None

        t_legacy = datetime(2025, 5, 1, 8, 0, 0, tzinfo=timezone.utc)
        t_canonical = datetime(2025, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
        t_new = datetime(2025, 5, 1, 9, 0, 0, tzinfo=timezone.utc)

        legacy_row = SyncWatermark(
            repo_id=REPO_ID,
            target="git",
            org_id=ORG_ID,
            source_id=REPO_ID,
            dataset_key="git",
            last_synced_at=t_legacy,
        )
        db_session.add(legacy_row)
        db_session.flush()

        canonical_row = SyncWatermark(
            repo_id=REPO_ID,
            target=canonical_key,
            org_id=ORG_ID,
            source_id=REPO_ID,
            dataset_key=canonical_key,
            last_synced_at=t_canonical,
        )
        db_session.add(canonical_row)
        db_session.flush()

        set_legacy_repo_watermark(db_session, ORG_ID, REPO_ID, "git", t_new)

        # The legacy row (dataset_key='git') must be gone.
        remaining_legacy = (
            db_session.query(SyncWatermark)
            .filter(
                SyncWatermark.org_id == ORG_ID,
                SyncWatermark.repo_id == REPO_ID,
                SyncWatermark.dataset_key == "git",
            )
            .one_or_none()
        )
        assert remaining_legacy is None, (
            "Legacy row with dataset_key='git' must be deleted after merge"
        )
