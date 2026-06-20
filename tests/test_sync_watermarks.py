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
