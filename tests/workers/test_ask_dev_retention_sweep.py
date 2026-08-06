"""CHAOS-3404: Ask Dev retention sweep must have a real production caller.

``DevPersistenceService.cleanup_expired`` existed with unit coverage but no
beat schedule, CLI command, or caller anywhere -- the 0/30-day retention
policy never executed in production. These tests prove:

* the wiring seam -- a registered Celery task + a beat entry that points at
  it (mirrors ``tests/test_release_impact_schedule.py``'s precedent), and
* the sweep helper actually purges expired rows end-to-end against a real
  (sqlite) database, aged via a direct SQL UPDATE of the persisted
  ``expires_at`` column -- never wall-clock waiting, never monkeypatching the
  service's clock (D6: DB-level timestamp aging only).
* a skipped/failed sweep is observable: the last-success gauge only advances
  on success, and a mid-sweep failure still surfaces (never a silent empty
  success).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import pytest_asyncio
from celery.schedules import crontab
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev.persistence import DevPersistenceService
from dev_health_ops.models.dev_persistence import (
    DevConversation,
    DevConversationTombstone,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization, User
from tests._helpers import closing_coroutine_runner, tables_of

_TABLES = tables_of(
    User,
    Organization,
    DevConversation,
    DevConversationTombstone,
)


@pytest_asyncio.fixture
async def persistence(tmp_path: Path):
    database = tmp_path / "ask-dev-retention-sweep.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database}")
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    async with maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="ask-dev", name="Ask Dev"),
                User(id=user_id, email="ask-dev@example.com"),
            ]
        )
        await session.commit()
    try:
        yield maker, org_id, user_id
    finally:
        await engine.dispose()


async def _seed_conversation(maker, *, org_id, user_id, retention_days: int) -> Any:
    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id,
            user_id=user_id,
            current_scope={},
            retention_days=retention_days,
        )
        await session.commit()
        return conversation.id


async def _age_conversation(maker, conversation_id, *, expires_at: datetime) -> None:
    """Age a persisted row's expiry directly in the DB (D6: no clock mocking)."""
    async with maker() as session:
        await session.execute(
            update(DevConversation)
            .where(DevConversation.id == conversation_id)
            .values(expires_at=expires_at)
        )
        await session.commit()


class TestTaskRegistration:
    def test_task_importable_and_callable(self) -> None:
        from dev_health_ops.workers.ask_dev_retention import (
            run_ask_dev_retention_cleanup,
        )

        assert callable(run_ask_dev_retention_cleanup)

    def test_task_exported_from_tasks_module(self) -> None:
        from dev_health_ops.workers import tasks

        assert "run_ask_dev_retention_cleanup" in tasks.__all__
        assert hasattr(tasks, "run_ask_dev_retention_cleanup")

    def test_task_is_celery_task_on_default_queue(self) -> None:
        from dev_health_ops.workers.ask_dev_retention import (
            run_ask_dev_retention_cleanup,
        )

        assert hasattr(run_ask_dev_retention_cleanup, "apply_async")
        assert hasattr(run_ask_dev_retention_cleanup, "delay")
        assert (
            run_ask_dev_retention_cleanup.name
            == "dev_health_ops.workers.tasks.run_ask_dev_retention_cleanup"
        )
        assert run_ask_dev_retention_cleanup.queue == "default"

    def test_task_drives_the_async_sweep_via_run_async(self) -> None:
        """The sync Celery task body must go through the shared run_async
        helper (not a bare asyncio.run()), matching every other async task."""
        from dev_health_ops.workers import ask_dev_retention

        sentinel = closing_coroutine_runner(
            return_value={"status": "completed", "purged": 0}
        )
        with patch.object(ask_dev_retention, "run_async", side_effect=sentinel):
            result = ask_dev_retention.run_ask_dev_retention_cleanup.run()
        assert result == {"status": "completed", "purged": 0}


class TestBeatSchedule:
    def test_beat_schedule_has_the_retention_sweep_entry(self) -> None:
        from dev_health_ops.workers.config import beat_schedule

        assert "ask-dev-retention-sweep" in beat_schedule
        entry = beat_schedule["ask-dev-retention-sweep"]
        assert (
            entry["task"]
            == "dev_health_ops.workers.tasks.run_ask_dev_retention_cleanup"
        )
        assert entry["options"]["queue"] == "default"

    def test_beat_schedule_uses_a_daily_crontab(self) -> None:
        from dev_health_ops.workers.config import beat_schedule

        schedule = beat_schedule["ask-dev-retention-sweep"]["schedule"]
        assert isinstance(schedule, crontab)

    def test_beat_entry_kwargs_are_a_subset_of_the_task_signature(self) -> None:
        """Signature-contract guard (team-lead design requirement, precedent:
        test_daily_metrics_kwargs_subset_of_task_signature in
        test_external_ingest_recompute_dispatch.py): whatever kwargs the
        beat entry (or any future dispatcher) passes to
        run_ask_dev_retention_cleanup must be real parameter names on the
        task's own ``.run`` signature -- a renamed/removed `limit` or
        `max_batches` kwarg would otherwise fail silently at call time
        (TypeError swallowed inside Celery's task dispatch, not surfaced
        until the sweep never runs)."""
        import inspect

        from dev_health_ops.workers.ask_dev_retention import (
            run_ask_dev_retention_cleanup,
        )
        from dev_health_ops.workers.config import beat_schedule

        params = set(inspect.signature(run_ask_dev_retention_cleanup.run).parameters)
        assert {"limit", "max_batches"} <= params

        entry_kwargs = beat_schedule["ask-dev-retention-sweep"].get("kwargs", {})
        assert set(entry_kwargs) <= params


class TestSweepPurgesDbLevelAgedRows:
    @pytest.mark.asyncio
    async def test_expired_30_day_conversation_is_purged(self, persistence) -> None:
        from dev_health_ops.workers.ask_dev_retention import (
            _run_ask_dev_retention_cleanup,
        )

        maker, org_id, user_id = persistence
        conversation_id = await _seed_conversation(
            maker, org_id=org_id, user_id=user_id, retention_days=30
        )
        # DB-level timestamp aging (D6): a direct SQL UPDATE of the persisted
        # row, not a mocked clock and not a real wall-clock sleep.
        await _age_conversation(
            maker,
            conversation_id,
            expires_at=datetime.now(UTC) - timedelta(days=1),
        )

        with patch(
            "dev_health_ops.metrics.prometheus.record_ask_dev_retention_sweep"
        ) as mock_record:
            result = await _run_ask_dev_retention_cleanup(session_factory=maker)

        assert result == {
            "status": "completed",
            "purged": 1,
            "batches": 1,
            "drained": True,
        }
        async with maker() as session:
            assert await session.get(DevConversation, conversation_id) is None
        mock_record.assert_called_once_with(status="completed", purged=1)

    @pytest.mark.asyncio
    async def test_not_yet_expired_conversation_is_left_alone(
        self, persistence
    ) -> None:
        from dev_health_ops.workers.ask_dev_retention import (
            _run_ask_dev_retention_cleanup,
        )

        maker, org_id, user_id = persistence
        conversation_id = await _seed_conversation(
            maker, org_id=org_id, user_id=user_id, retention_days=30
        )
        # Freshly created: expires_at is ~30 days out, not yet due.

        result = await _run_ask_dev_retention_cleanup(session_factory=maker)

        assert result["purged"] == 0
        async with maker() as session:
            assert await session.get(DevConversation, conversation_id) is not None

    @pytest.mark.asyncio
    async def test_drains_across_multiple_batches(self, persistence) -> None:
        """More expired rows than fit in one `limit`-sized batch still all
        get purged in one sweep, as long as it stays under max_batches."""
        from dev_health_ops.workers.ask_dev_retention import (
            _run_ask_dev_retention_cleanup,
        )

        maker, org_id, user_id = persistence
        conversation_ids = [
            await _seed_conversation(
                maker, org_id=org_id, user_id=user_id, retention_days=30
            )
            for _ in range(3)
        ]
        past = datetime.now(UTC) - timedelta(days=1)
        for conversation_id in conversation_ids:
            await _age_conversation(maker, conversation_id, expires_at=past)

        result = await _run_ask_dev_retention_cleanup(
            limit=1, max_batches=10, session_factory=maker
        )

        assert result["status"] == "completed"
        assert result["purged"] == 3
        assert result["batches"] >= 3
        assert result["drained"] is True
        async with maker() as session:
            for conversation_id in conversation_ids:
                assert await session.get(DevConversation, conversation_id) is None

    @pytest.mark.asyncio
    async def test_batch_cap_reports_undrained_instead_of_looping_forever(
        self, persistence
    ) -> None:
        """A backlog bigger than limit*max_batches must be reported, not
        silently dropped -- the next scheduled tick finishes the drain."""
        from dev_health_ops.workers.ask_dev_retention import (
            _run_ask_dev_retention_cleanup,
        )

        maker, org_id, user_id = persistence
        conversation_ids = [
            await _seed_conversation(
                maker, org_id=org_id, user_id=user_id, retention_days=30
            )
            for _ in range(3)
        ]
        past = datetime.now(UTC) - timedelta(days=1)
        for conversation_id in conversation_ids:
            await _age_conversation(maker, conversation_id, expires_at=past)

        with patch(
            "dev_health_ops.metrics.prometheus.record_ask_dev_retention_sweep"
        ) as mock_record:
            result = await _run_ask_dev_retention_cleanup(
                limit=1, max_batches=2, session_factory=maker
            )

        assert result["status"] == "partial"
        assert result["purged"] == 2
        assert result["batches"] == 2
        assert result["drained"] is False
        # Codex adversarial-review round 1 (CHAOS-3404, HIGH, confirmed): a
        # capped-but-error-free run must record "partial", never "completed"
        # -- advancing the last-success gauge here would let retention debt
        # persist while the dead-man alert reports a healthy sweep.
        mock_record.assert_called_once_with(status="partial", purged=2)
        # The third row is still there -- a real remaining backlog, not lost.
        async with maker() as session:
            remaining = [
                cid
                for cid in conversation_ids
                if await session.get(DevConversation, cid) is not None
            ]
        assert len(remaining) == 1

    @pytest.mark.asyncio
    async def test_a_falsely_short_batch_does_not_report_completed(
        self, persistence
    ) -> None:
        """Codex adversarial-review round 2 (CHAOS-3404, HIGH, confirmed): a
        batch returning fewer than `limit` rows is not proof the backlog is
        empty -- cleanup_expired selects with FOR UPDATE SKIP LOCKED, so a
        genuinely concurrent invocation holding the remaining expired rows'
        locks can make a batch look short even though real work remains.
        This simulates exactly that short-read shape (0 selected, 0 purged,
        as a fully-lock-contended SKIP LOCKED read would report) while a
        genuinely expired, unpurged conversation is still present in the
        DB, and proves the definitive post-loop count_expired() check
        catches it: the sweep must report "partial", never "completed",
        and must not lose the still-pending row."""
        from dev_health_ops.api.dev.persistence import (
            CleanupResult,
            DevPersistenceService,
        )
        from dev_health_ops.workers.ask_dev_retention import (
            _run_ask_dev_retention_cleanup,
        )

        maker, org_id, user_id = persistence
        conversation_id = await _seed_conversation(
            maker, org_id=org_id, user_id=user_id, retention_days=30
        )
        await _age_conversation(
            maker, conversation_id, expires_at=datetime.now(UTC) - timedelta(days=1)
        )

        async def _fake_lock_contended_batch(self, *, limit):
            return CleanupResult(reason="retention_expired", selected=0, purged=0)

        with (
            patch.object(
                DevPersistenceService, "cleanup_expired", _fake_lock_contended_batch
            ),
            patch(
                "dev_health_ops.metrics.prometheus.record_ask_dev_retention_sweep"
            ) as mock_record,
        ):
            result = await _run_ask_dev_retention_cleanup(
                limit=1, session_factory=maker
            )

        assert result["status"] == "partial"
        assert result["drained"] is False
        assert result["purged"] == 0
        mock_record.assert_called_once_with(status="partial", purged=0)
        # Not lost -- still there for the next, uncontended tick to collect.
        async with maker() as session:
            assert await session.get(DevConversation, conversation_id) is not None


class TestSweepFailureIsObservableNotSilent:
    @pytest.mark.asyncio
    async def test_a_zeroth_batch_failure_raises_and_records_failed_status(
        self,
    ) -> None:
        """The connection-never-opens case: nothing committed, purged=0."""
        from dev_health_ops.workers.ask_dev_retention import (
            _run_ask_dev_retention_cleanup,
        )

        class _BoomSession:
            async def __aenter__(self):
                raise RuntimeError("postgres unavailable")

            async def __aexit__(self, *exc_info):
                return False

        def _broken_factory():
            return _BoomSession()

        with patch(
            "dev_health_ops.metrics.prometheus.record_ask_dev_retention_sweep"
        ) as mock_record:
            with pytest.raises(RuntimeError, match="postgres unavailable"):
                await _run_ask_dev_retention_cleanup(session_factory=_broken_factory)

        # Never a silent empty success: status=failed was recorded, and the
        # last-success gauge (asserted via record_ask_dev_retention_sweep's
        # own contract, unit-tested separately below) was never touched.
        mock_record.assert_called_once_with(status="failed", purged=0)

    @pytest.mark.asyncio
    async def test_a_second_batch_failure_preserves_the_first_batchs_purge(
        self, persistence
    ) -> None:
        """Codex adversarial-review round 1 (CHAOS-3404, MEDIUM, confirmed):
        the prior failure test never exercised a real committed batch before
        the failure. This drives an actual first batch to a real commit
        (row genuinely gone, durably, independent of the second failure),
        then fails the second batch, and asserts the failure metric reports
        the REAL committed count -- not 0, not silently dropped -- and that
        DevPersistenceService.cleanup_expired was the thing actually called
        (a mutation swapping it for a bare DELETE that skips the tombstone
        must not be able to pass this test)."""
        from dev_health_ops.api.dev.persistence import DevPersistenceService
        from dev_health_ops.workers.ask_dev_retention import (
            _run_ask_dev_retention_cleanup,
        )

        maker, org_id, user_id = persistence
        conversation_ids = [
            await _seed_conversation(
                maker, org_id=org_id, user_id=user_id, retention_days=30
            )
            for _ in range(2)
        ]
        past = datetime.now(UTC) - timedelta(days=1)
        for conversation_id in conversation_ids:
            await _age_conversation(maker, conversation_id, expires_at=past)

        real_calls: list[int] = []
        real_cleanup_expired = DevPersistenceService.cleanup_expired

        call_count = 0

        async def _spy_cleanup_expired(self, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("connection dropped mid-second-batch")
            result = await real_cleanup_expired(self, *args, **kwargs)
            real_calls.append(result.purged)
            return result

        with (
            patch.object(
                DevPersistenceService, "cleanup_expired", _spy_cleanup_expired
            ),
            patch(
                "dev_health_ops.metrics.prometheus.record_ask_dev_retention_sweep"
            ) as mock_record,
        ):
            with pytest.raises(RuntimeError, match="connection dropped"):
                await _run_ask_dev_retention_cleanup(
                    limit=1, max_batches=5, session_factory=maker
                )

        # cleanup_expired (the real service method, not a bare DELETE) was
        # genuinely invoked, and batch one genuinely purged one row.
        assert call_count == 2
        assert real_calls == [1]
        # That first purge is durable -- committed before the second batch
        # ever started -- independent of the second batch's failure.
        async with maker() as session:
            remaining = [
                cid
                for cid in conversation_ids
                if await session.get(DevConversation, cid) is not None
            ]
        assert len(remaining) == 1
        # The failure metric reports the REAL committed count, not 0.
        mock_record.assert_called_once_with(status="failed", purged=1)


class TestRecordAskDevRetentionSweepGaugeContract:
    """Unit contract for the metrics helper itself: the receipt that makes a
    *skipped* sweep detectable (not just a failed one) is that the
    last-success gauge only ever advances on status="completed"."""

    def test_completed_status_advances_the_last_success_gauge(self) -> None:
        from dev_health_ops.metrics import prometheus

        with (
            patch.object(
                prometheus.ASK_DEV_RETENTION_SWEEP_TOTAL, "labels"
            ) as mock_total,
            patch.object(
                prometheus.ASK_DEV_RETENTION_SWEEP_LAST_SUCCESS_TIMESTAMP, "set"
            ) as mock_gauge_set,
            patch.object(
                prometheus.ASK_DEV_RETENTION_SWEEP_PURGED_TOTAL, "inc"
            ) as mock_purged_inc,
        ):
            prometheus.record_ask_dev_retention_sweep(
                status="completed", purged=5, timestamp=1234.0
            )

        mock_total.assert_called_once_with(status="completed")
        mock_purged_inc.assert_called_once_with(5)
        mock_gauge_set.assert_called_once_with(1234.0)

    def test_partial_status_records_purged_but_never_advances_the_gauge(
        self,
    ) -> None:
        from dev_health_ops.metrics import prometheus

        with (
            patch.object(
                prometheus.ASK_DEV_RETENTION_SWEEP_TOTAL, "labels"
            ) as mock_total,
            patch.object(
                prometheus.ASK_DEV_RETENTION_SWEEP_LAST_SUCCESS_TIMESTAMP, "set"
            ) as mock_gauge_set,
            patch.object(
                prometheus.ASK_DEV_RETENTION_SWEEP_PURGED_TOTAL, "inc"
            ) as mock_purged_inc,
        ):
            prometheus.record_ask_dev_retention_sweep(status="partial", purged=3)

        mock_total.assert_called_once_with(status="partial")
        mock_purged_inc.assert_called_once_with(3)
        mock_gauge_set.assert_not_called()

    def test_failed_status_never_advances_the_last_success_gauge(self) -> None:
        from dev_health_ops.metrics import prometheus

        with (
            patch.object(
                prometheus.ASK_DEV_RETENTION_SWEEP_TOTAL, "labels"
            ) as mock_total,
            patch.object(
                prometheus.ASK_DEV_RETENTION_SWEEP_LAST_SUCCESS_TIMESTAMP, "set"
            ) as mock_gauge_set,
        ):
            prometheus.record_ask_dev_retention_sweep(status="failed")

        mock_total.assert_called_once_with(status="failed")
        mock_gauge_set.assert_not_called()
