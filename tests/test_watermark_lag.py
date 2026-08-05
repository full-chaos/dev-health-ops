"""Watermark-vs-now lag computation (CHAOS-3430).

A HEAVY dataset syncing under the CHAOS-3412 incremental window ratchet
finalizes every capped tick as an ordinary SUCCESS while its watermark may
still trail ``now`` by weeks.  These tests pin the rule that decides when
that trailing state is worth surfacing as "catching up", and the
resolution precedence used to read the watermark for a (source, dataset)
pair.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.sync.watermark_lag import (
    DEFAULT_HEAVY_MAX_WINDOW_DAYS,
    HEAVY_MAX_WINDOW_DAYS_ENV,
    compute_watermark_lag,
    heavy_max_window_days,
    resolve_watermark,
)

NOW = datetime(2026, 8, 5, 12, 0, 0, tzinfo=timezone.utc)


def _lag(cost_class: str, trailing_days: float, **kwargs):
    return compute_watermark_lag(
        cost_class=cost_class,
        watermark_at=NOW - timedelta(days=trailing_days),
        now=NOW,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Flagging rule
# ---------------------------------------------------------------------------


def test_heavy_dataset_trailing_beyond_cap_is_flagged_catching_up():
    """Cold-start ratchet: first capped tick stamps a watermark ~83 days back."""
    lag = _lag("heavy", 83)

    assert lag.catching_up is True
    assert lag.lag_seconds == int(timedelta(days=83).total_seconds())
    assert lag.watermark_at == NOW - timedelta(days=83)
    assert lag.window_cap_days == DEFAULT_HEAVY_MAX_WINDOW_DAYS
    # 83 days of arrears at a 7-day cap needs ceil(83/7) == 12 more ticks.
    assert lag.ticks_behind == 12


def test_heavy_dataset_caught_up_is_not_flagged():
    lag = _lag("heavy", 0.25)

    assert lag.catching_up is False
    assert lag.ticks_behind is None
    assert lag.lag_seconds == int(timedelta(days=0.25).total_seconds())


def test_heavy_dataset_exactly_at_cap_is_not_flagged():
    """A watermark exactly one cap-window back is one healthy tick, not arrears."""
    lag = _lag("heavy", DEFAULT_HEAVY_MAX_WINDOW_DAYS)

    assert lag.catching_up is False
    assert lag.ticks_behind is None


def test_heavy_dataset_one_second_past_cap_is_flagged():
    lag = compute_watermark_lag(
        cost_class="heavy",
        watermark_at=NOW - timedelta(days=DEFAULT_HEAVY_MAX_WINDOW_DAYS, seconds=1),
        now=NOW,
    )

    assert lag.catching_up is True
    assert lag.ticks_behind == 2


@pytest.mark.parametrize("cost_class", ["light", "medium", "standard", "unknown", ""])
def test_non_heavy_dataset_is_never_flagged(cost_class: str):
    """Only HEAVY families ratchet; a trailing LIGHT watermark is not catch-up."""
    lag = _lag(cost_class, 400)

    assert lag.catching_up is False
    assert lag.ticks_behind is None
    # Lag is still reported honestly — only the catch-up verdict is withheld.
    assert lag.lag_seconds == int(timedelta(days=400).total_seconds())


def test_missing_watermark_reports_no_lag_and_no_flag():
    """No watermark row yet: nothing has been read, so nothing is 'behind'."""
    lag = compute_watermark_lag(cost_class="heavy", watermark_at=None, now=NOW)

    assert lag.watermark_at is None
    assert lag.lag_seconds is None
    assert lag.catching_up is False
    assert lag.ticks_behind is None


def test_watermark_ahead_of_now_clamps_lag_to_zero():
    lag = compute_watermark_lag(
        cost_class="heavy", watermark_at=NOW + timedelta(days=3), now=NOW
    )

    assert lag.lag_seconds == 0
    assert lag.catching_up is False


def test_naive_watermark_is_read_as_utc():
    """SQLite round-trips drop tzinfo; a naive stamp must not blow up or skew."""
    lag = compute_watermark_lag(
        cost_class="heavy",
        watermark_at=(NOW - timedelta(days=30)).replace(tzinfo=None),
        now=NOW,
    )

    assert lag.lag_seconds == int(timedelta(days=30).total_seconds())
    assert lag.catching_up is True


# ---------------------------------------------------------------------------
# Cap resolution
# ---------------------------------------------------------------------------


def test_cap_defaults_to_seven_days(monkeypatch):
    monkeypatch.delenv(HEAVY_MAX_WINDOW_DAYS_ENV, raising=False)
    assert heavy_max_window_days() == 7
    assert DEFAULT_HEAVY_MAX_WINDOW_DAYS == 7


def test_cap_reads_env_override(monkeypatch):
    monkeypatch.setenv(HEAVY_MAX_WINDOW_DAYS_ENV, "30")
    assert heavy_max_window_days() == 30
    # A 20-day arrears is inside a 30-day cap: no longer catching up.
    assert _lag("heavy", 20).catching_up is False
    assert _lag("heavy", 40).catching_up is True


@pytest.mark.parametrize("raw", ["0", "-3", "nonsense", ""])
def test_cap_rejects_unusable_env_values(monkeypatch, raw: str):
    """A zero/negative/garbage cap would divide by zero or flag everything."""
    monkeypatch.setenv(HEAVY_MAX_WINDOW_DAYS_ENV, raw)
    assert heavy_max_window_days() == DEFAULT_HEAVY_MAX_WINDOW_DAYS


def test_cap_tracks_the_planner_resolver_including_the_overlap_widening(monkeypatch):
    """The verdict must use the cap the planner ACTUALLY sizes windows with.

    ``_effective_heavy_max_window_days`` widens the cap when
    ``SYNC_WATERMARK_OVERLAP`` meets or exceeds it, because a window ending at
    or before its own start watermark can never advance.  A duplicated env read
    here would judge lag against the narrower configured value and report every
    dataset inside the widened window as behind.  This runs the production
    resolver as the oracle, so the two cannot drift.
    """
    from dev_health_ops.sync.planner import _effective_heavy_max_window_days

    # Overlap (10 days) exceeds the configured cap (7) -> planner widens.
    monkeypatch.setenv(HEAVY_MAX_WINDOW_DAYS_ENV, "7")
    monkeypatch.setenv("SYNC_WATERMARK_OVERLAP", str(10 * 86_400))

    planner_cap = _effective_heavy_max_window_days()
    assert planner_cap > 7, "precondition: planner must widen this configuration"
    assert heavy_max_window_days() == planner_cap

    # A dataset inside the WIDENED window is not catching up, even though it
    # trails by more than the configured 7 days.
    inside = compute_watermark_lag(
        cost_class="heavy",
        watermark_at=NOW - timedelta(days=planner_cap - 1),
        now=NOW,
    )
    assert inside.catching_up is False
    assert inside.window_cap_days == planner_cap


def test_cap_matches_the_planner_when_no_widening_applies(monkeypatch):
    from dev_health_ops.sync.planner import _effective_heavy_max_window_days

    monkeypatch.setenv(HEAVY_MAX_WINDOW_DAYS_ENV, "21")
    monkeypatch.delenv("SYNC_WATERMARK_OVERLAP", raising=False)

    assert heavy_max_window_days() == _effective_heavy_max_window_days() == 21


def test_ticks_behind_accounts_for_the_watermark_overlap(monkeypatch):
    """Net advance per tick is cap MINUS overlap, not the cap (CHAOS-3430 F2).

    The capped window spans ``[W - overlap, W - overlap + cap]``, so a successful
    tick moves the watermark forward by ``cap - overlap``. Dividing arrears by
    the raw cap understates catch-up time by the ratio between the two — with a
    6-day overlap on a 7-day cap that is a 7x understatement.
    """
    monkeypatch.setenv(HEAVY_MAX_WINDOW_DAYS_ENV, "7")
    monkeypatch.setenv("SYNC_WATERMARK_OVERLAP", str(6 * 86_400))

    lag = _lag("heavy", 83)

    assert lag.catching_up is True
    # Net advance is 7 - 6 = 1 day per tick, so 83 days of arrears needs 83
    # ticks -- NOT ceil(83/7) == 12.
    assert lag.ticks_behind == 83


def test_ticks_behind_uses_net_advance_under_the_widened_cap(monkeypatch):
    """The clamp widens the cap, but net advance stays ~1 day.

    When overlap >= cap the planner widens the cap to floor(overlap_days)+1 so
    each tick advances at all. Net advance is then a single day, and the tick
    estimate must reflect that rather than the widened cap.
    """
    from dev_health_ops.sync.planner import _effective_heavy_max_window_days

    monkeypatch.setenv(HEAVY_MAX_WINDOW_DAYS_ENV, "7")
    monkeypatch.setenv("SYNC_WATERMARK_OVERLAP", str(10 * 86_400))

    assert _effective_heavy_max_window_days() == 11  # floor(10) + 1
    lag = _lag("heavy", 40)

    # Net advance = 11 - 10 = 1 day, so 40 days of arrears needs 40 ticks.
    assert lag.ticks_behind == 40
    assert lag.window_cap_days == 11


def test_ticks_behind_matches_the_cap_when_no_overlap_is_configured(monkeypatch):
    """Control: with no overlap, net advance IS the cap and nothing changes."""
    monkeypatch.setenv(HEAVY_MAX_WINDOW_DAYS_ENV, "7")
    monkeypatch.delenv("SYNC_WATERMARK_OVERLAP", raising=False)

    assert _lag("heavy", 83).ticks_behind == 12


def test_explicit_cap_argument_overrides_env(monkeypatch):
    monkeypatch.setenv(HEAVY_MAX_WINDOW_DAYS_ENV, "7")
    lag = _lag("heavy", 10, window_cap_days=14)

    assert lag.window_cap_days == 14
    assert lag.catching_up is False


# ---------------------------------------------------------------------------
# Watermark resolution precedence (mirrors sync.watermarks.get_watermark)
# ---------------------------------------------------------------------------


class _Row:
    def __init__(self, source_id, repo_id, target, dataset_key, last_synced_at):
        self.source_id = source_id
        self.repo_id = repo_id
        self.target = target
        self.dataset_key = dataset_key
        self.last_synced_at = last_synced_at


def test_resolution_prefers_canonical_row():
    canonical = _Row("owner/repo", "owner/repo", "commits", "commits", NOW)
    legacy = _Row("owner/repo", "owner/repo", "git", "git", NOW - timedelta(days=40))

    assert resolve_watermark([legacy, canonical], "owner/repo", "commits") == NOW


def test_resolution_falls_back_to_raw_legacy_row():
    """`commits` with no canonical row warms from the raw `target='git'` row."""
    legacy = _Row("owner/repo", "owner/repo", "git", "git", NOW - timedelta(days=40))

    assert resolve_watermark([legacy], "owner/repo", "commits") == NOW - timedelta(
        days=40
    )


def test_resolution_ignores_other_sources():
    other = _Row("other/repo", "other/repo", "commits", "commits", NOW)

    assert resolve_watermark([other], "owner/repo", "commits") is None


def test_resolution_returns_none_when_no_row_matches():
    assert resolve_watermark([], "owner/repo", "commits") is None
