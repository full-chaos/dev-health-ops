"""Schedulers honour the selected timezone (CHAOS-2689).

The sync / report / metrics dispatchers must evaluate cron schedules in the
schedule's **selected** timezone, not UTC. Previously the persisted ``timezone``
was ignored and every cron was interpreted as UTC, so any non-UTC schedule fired
on the wrong wall-clock slots and looked like it "never ran". These tests pin:

1. ``cron_next_run`` math: a non-UTC cron resolves to the local wall-clock slot
   (with correct DST offset), naive bases are treated as UTC, and unknown/empty
   timezones fall back to UTC instead of crashing.
2. Wiring: each dispatcher forwards the job's stored timezone into the cron
   computation (a regression that drops the argument is caught).
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models.git import Base

# Importing these at module load registers their tables on the shared ``Base``
# metadata so ``create_all`` builds them for the in-memory SQLite fixture.
from dev_health_ops.models.reports import SavedReport
from dev_health_ops.models.settings import (
    ScheduledJob,
    SyncConfiguration,
)
from dev_health_ops.utils.datetime import validate_timezone_name
from dev_health_ops.workers.task_utils import cron_next_run

LA = "America/Los_Angeles"


class TestCronNextRun:
    """Direct proof of the timezone-aware cron math (real ``croniter``)."""

    def test_non_utc_cron_evaluates_on_local_wall_clock(self):
        # base 2026-06-26 12:00Z == 05:00 PDT. The next LA midnight is
        # 2026-06-27 00:00 PDT == 2026-06-27 07:00Z (PDT = UTC-7 in June).
        base = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
        assert cron_next_run("0 0 * * *", base, LA) == datetime(
            2026, 6, 27, 7, 0, tzinfo=timezone.utc
        )

    def test_utc_cron_matches_utc_slot(self):
        base = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
        assert cron_next_run("0 0 * * *", base, "UTC") == datetime(
            2026, 6, 27, 0, 0, tzinfo=timezone.utc
        )

    def test_dst_offset_tracks_the_season(self):
        # January: PST = UTC-8, so LA midnight resolves to 08:00Z.
        winter = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)
        assert cron_next_run("0 0 * * *", winter, LA) == datetime(
            2026, 1, 16, 8, 0, tzinfo=timezone.utc
        )

    def test_result_is_aware_utc(self):
        base = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
        assert cron_next_run("0 */6 * * *", base, LA).tzinfo is timezone.utc

    def test_naive_base_is_treated_as_utc(self):
        naive = datetime(2026, 6, 26, 12, 0)
        aware = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
        assert cron_next_run("0 0 * * *", naive, LA) == cron_next_run(
            "0 0 * * *", aware, LA
        )

    @pytest.mark.parametrize("tz_name", ["", None])
    def test_missing_timezone_defaults_to_utc(self, tz_name):
        # Empty / None is a legacy/unset value and is intentionally treated as UTC.
        base = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
        assert cron_next_run("0 0 * * *", base, tz_name) == cron_next_run(
            "0 0 * * *", base, "UTC"
        )

    def test_invalid_timezone_is_defense_in_depth_utc_fallback(self):
        # Invalid timezones are rejected at the schedule write path (see
        # TestTimezoneValidation); if a corrupt/legacy row still reaches the
        # dispatcher, the helper must NOT crash -- it falls back to UTC so other
        # due jobs keep dispatching.
        base = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
        assert cron_next_run("0 0 * * *", base, "Not/AZone") == cron_next_run(
            "0 0 * * *", base, "UTC"
        )

    def test_dst_fall_back_does_not_emit_repeated_hour_twice(self):
        # 2026-11-01 in America/Los_Angeles: 02:00 PDT falls back to 01:00 PST,
        # so 01:30 occurs twice (08:30Z PDT, then 09:30Z PST). A '30 1 * * *'
        # schedule must fire the slot ONCE: the first occurrence resolves to the
        # earlier offset, and advancing from there jumps to the NEXT DAY -- never
        # the same-day second fold (which would double-fire the job).
        before = datetime(
            2026, 11, 1, 6, 0, tzinfo=timezone.utc
        )  # 2026-10-31 23:00 PDT
        first = cron_next_run("30 1 * * *", before, LA)
        assert first == datetime(2026, 11, 1, 8, 30, tzinfo=timezone.utc)
        # Advancing the marker from the first fire skips the repeated 09:30Z slot.
        nxt = cron_next_run("30 1 * * *", first, LA)
        assert nxt == datetime(2026, 11, 2, 9, 30, tzinfo=timezone.utc)

    def test_dst_spring_forward_nonexistent_time_resolves_once(self):
        # 2026-03-08 in America/Los_Angeles: 02:00 -> 03:00, so 02:30 does not
        # exist. A '30 2 * * *' schedule must still resolve to a single instant
        # without raising.
        before = datetime(2026, 3, 8, 9, 0, tzinfo=timezone.utc)  # 2026-03-08 01:00 PST
        result = cron_next_run("30 2 * * *", before, LA)
        assert result.tzinfo is timezone.utc
        assert result == datetime(2026, 3, 8, 10, 30, tzinfo=timezone.utc)


@contextmanager
def _session_ctx(session):
    yield session


class TestDispatchersForwardSelectedTimezone:
    """Each dispatcher must pass the job's stored timezone into the cron eval.

    The spy returns a far-future occurrence so dispatch short-circuits as
    "not due" before any enqueue/planner work, while still capturing the
    ``tz_name`` the dispatcher forwarded.
    """

    @pytest.fixture
    def db_session(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        with Session(engine) as session:
            yield session
        engine.dispose()

    @staticmethod
    def _call(task) -> dict:
        task.push_request(id=str(uuid.uuid4()))
        try:
            return task()
        finally:
            task.pop_request()

    def test_sync_dispatch_forwards_timezone(self, monkeypatch, db_session):
        from dev_health_ops.workers import sync_scheduler

        now = datetime.now(timezone.utc)
        config = SyncConfiguration(
            name="linear",
            provider="linear",
            org_id="default",
            sync_targets=["work-items"],
            sync_options={"schedule_cron": "0 */6 * * *", "timezone": LA},
            is_active=True,
        )
        config.last_sync_at = now - timedelta(hours=2)
        db_session.add(config)
        db_session.flush()
        job = ScheduledJob(
            name=f"sync-config-{config.id}",
            job_type="sync",
            schedule_cron="0 */6 * * *",
            org_id="default",
            provider="linear",
            sync_config_id=config.id,
            tz=LA,
        )
        db_session.add(job)
        db_session.flush()

        seen: dict[str, str | None] = {}

        def spy(cron_expr, base, tz_name=None):
            seen["tz"] = tz_name
            return now + timedelta(hours=99)  # far future => not due

        monkeypatch.setattr(sync_scheduler, "cron_next_run", spy)
        monkeypatch.setattr(
            "dev_health_ops.db.get_postgres_session_sync",
            lambda: _session_ctx(db_session),
        )
        monkeypatch.setattr(
            sync_scheduler, "organization_exists_sync", lambda _s, _o: True
        )

        result = self._call(sync_scheduler.dispatch_scheduled_syncs)
        assert result["dispatched"] == []
        assert seen["tz"] == LA

    def test_metrics_dispatch_forwards_timezone(self, monkeypatch, db_session):
        from dev_health_ops.workers import metrics_daily

        now = datetime.now(timezone.utc)
        job = ScheduledJob(
            name="metrics-default",
            job_type="metrics",
            schedule_cron="0 0 * * *",
            org_id="default",
            job_config={"org_id": "default"},
            tz=LA,
        )
        db_session.add(job)
        db_session.flush()

        seen: dict[str, str | None] = {}

        def spy(cron_expr, base, tz_name=None):
            seen["tz"] = tz_name
            return now + timedelta(hours=99)

        monkeypatch.setattr(metrics_daily, "cron_next_run", spy)
        monkeypatch.setattr(
            "dev_health_ops.db.get_postgres_session_sync",
            lambda: _session_ctx(db_session),
        )
        monkeypatch.setattr(
            metrics_daily, "organization_exists_sync", lambda _s, _o: True
        )

        result = self._call(metrics_daily.dispatch_scheduled_metrics)
        assert result["dispatched"] == []
        assert seen["tz"] == LA

    def test_report_dispatch_forwards_timezone(self, monkeypatch, db_session):
        from dev_health_ops.workers import report_scheduler

        now = datetime.now(timezone.utc)
        job = ScheduledJob(
            name="report-1",
            job_type="report",
            schedule_cron="0 0 * * *",
            org_id="default",
            tz=LA,
        )
        db_session.add(job)
        db_session.flush()
        report = SavedReport(
            org_id="default",
            name="weekly",
            report_plan={},
            schedule_id=job.id,
            is_active=True,
        )
        db_session.add(report)
        db_session.flush()

        seen: dict[str, str | None] = {}

        def spy(cron_expr, base, tz_name=None):
            seen["tz"] = tz_name
            return now + timedelta(hours=99)

        monkeypatch.setattr(report_scheduler, "cron_next_run", spy)
        monkeypatch.setattr(
            "dev_health_ops.db.get_postgres_session_sync",
            lambda: _session_ctx(db_session),
        )
        monkeypatch.setattr(
            report_scheduler, "organization_exists_sync", lambda _s, _o: True
        )

        result = self._call(report_scheduler.dispatch_scheduled_reports)
        assert result["dispatched"] == []
        assert seen["tz"] == LA


class TestTimezoneValidation:
    """``validate_timezone_name`` rejects bad zones at the schedule write path."""

    @pytest.mark.parametrize("good", ["America/Los_Angeles", "UTC", "Europe/Berlin"])
    def test_accepts_valid_zone(self, good):
        validate_timezone_name(good)  # must not raise

    @pytest.mark.parametrize("empty", ["", None])
    def test_allows_empty_as_utc_default(self, empty):
        validate_timezone_name(empty)  # must not raise

    @pytest.mark.parametrize(
        "bad", ["Not/AZone", "America/Nowhere", "garbage", "Totally/Bogus"]
    )
    def test_rejects_invalid_zone(self, bad):
        with pytest.raises(ValueError, match="Invalid timezone"):
            validate_timezone_name(bad)


class TestSyncDispatchDstFold:
    """The real sync due/marker path fires a DST fall-back fold slot only once."""

    @pytest.fixture
    def db_session(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        with Session(engine) as session:
            yield session
        engine.dispose()

    def test_fall_back_slot_dispatches_once(self, monkeypatch, db_session):
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        from dev_health_ops.workers import sync_scheduler

        # 2026-11-01 America/Los_Angeles fall-back: 01:30 occurs at 08:30Z (PDT)
        # and again at 09:30Z (PST).
        first_fold = datetime(2026, 11, 1, 8, 30, tzinfo=timezone.utc)
        second_fold = datetime(2026, 11, 1, 9, 30, tzinfo=timezone.utc)

        config = SyncConfiguration(
            name="linear",
            provider="linear",
            org_id="default",
            sync_targets=["work-items"],
            sync_options={"schedule_cron": "30 1 * * *", "timezone": LA},
            is_active=True,
        )
        config.last_sync_at = datetime(2026, 11, 1, 6, 0, tzinfo=timezone.utc)
        db_session.add(config)
        db_session.flush()
        job = ScheduledJob(
            name=f"sync-config-{config.id}",
            job_type="sync",
            schedule_cron="30 1 * * *",
            org_id="default",
            provider="linear",
            sync_config_id=config.id,
            tz=LA,
        )
        db_session.add(job)
        db_session.flush()

        dispatch_mock = MagicMock()
        monkeypatch.setattr(
            sync_scheduler, "organization_exists_sync", lambda *_a: True
        )
        monkeypatch.setattr(
            "dev_health_ops.sync.trigger_routing.planner_request_for_config_if_routed",
            lambda *a, **k: object(),
        )
        monkeypatch.setattr(
            "dev_health_ops.sync.planner.plan_sync_run",
            lambda *a, **k: SimpleNamespace(sync_run_id="run-1"),
        )
        monkeypatch.setattr(
            "dev_health_ops.workers.sync_units.dispatch_sync_run", dispatch_mock
        )

        # First fold instant: due -> dispatched exactly once.
        assert (
            sync_scheduler._maybe_dispatch_config(db_session, config, first_fold)
            is True
        )
        assert dispatch_mock.apply_async.call_count == 1
        # The marker advanced past the repeated hour to the next day's slot.
        marker = job.next_run_at
        assert marker is not None
        if marker.tzinfo is None:
            marker = marker.replace(tzinfo=timezone.utc)
        assert marker == datetime(2026, 11, 2, 9, 30, tzinfo=timezone.utc)

        # Second fold instant (same wall-clock 01:30): must NOT re-dispatch.
        assert (
            sync_scheduler._maybe_dispatch_config(db_session, config, second_fold)
            is False
        )
        assert dispatch_mock.apply_async.call_count == 1
