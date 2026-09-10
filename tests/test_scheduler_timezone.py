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

from datetime import datetime, timezone

import pytest

# Importing these at module load registers their tables on the shared ``Base``
# metadata so ``create_all`` builds them for the in-memory SQLite fixture.
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


# CHAOS-3093 (2026-09-09): TestDispatchersForwardSelectedTimezone tested
# workers/sync_scheduler.py's dispatch_scheduled_syncs, deleted outright --
# internal/scheduler/sync's coordinator/loop (Go, already live per
# config.py's beat_schedule comment history) owns this cadence now, and its
# own timezone-forwarding invariant is pinned by
# internal/scheduler/sync/scheduler_test.go's
# TestEvaluatePreservesPythonTimezoneFallbackSemantics. See
# tests/workers/test_celery_dead_code_contract.py.
#
# TestSyncDispatchDstFold (a single method, test_fall_back_slot_dispatches_
# once) tested sync_scheduler.py's _maybe_dispatch_config, also deleted. The
# same invariant -- a DST fall-back wall-clock slot fires exactly once -- is
# pinned on the Go side by a different mechanism (deterministic occurrence
# identity rather than a stored next-run marker):
# internal/scheduler/sync/transaction_test.go's
# TestOccurrenceIdentityIsDeterministicForConfigAndCronOccurrence.


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
