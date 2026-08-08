"""CHAOS-3602: the TTL registry must describe what ClickHouse ACTUALLY
enforces, parsed from the migration source itself -- never a hand-copied
number that can silently drift once a migration's TTL changes.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from dev_health_ops.fixtures import ttl_registry as ttl_registry_module
from dev_health_ops.fixtures.ttl_registry import (
    KNOWN_TTL_TABLES,
    MAX_PINNED_NOW_STALENESS_DAYS,
    RESTORE_SIDE_SLACK_DAYS,
    TTL_SAFETY_MARGIN_DAYS,
    PinnedNowStaleError,
    SnapshotExpiredError,
    assert_pinned_now_not_too_stale,
    assert_snapshot_not_expired,
    assert_ttl_vocabulary_is_consistent,
    clickhouse_ttl_retentions,
    compute_drift_days,
    drift_days_context,
    max_generated_age_days,
    max_safe_backdate_days,
    snapshot_expiry,
    snapshot_shelf_life_days,
)


class TestParsesTheRealMigrations:
    """The table that actually caused CHAOS-3602 must be discovered
    correctly against the checked-in migration, not a fixture string."""

    def test_feature_flag_event_is_discovered_with_its_real_ttl(self) -> None:
        retentions = clickhouse_ttl_retentions()
        assert "feature_flag_event" in retentions
        retention = retentions["feature_flag_event"]
        assert retention.column == "event_ts"
        assert retention.retention_days == 90

    def test_every_known_ttl_table_is_discovered(self) -> None:
        """Pins the full current set so a migration adding/removing a TTL
        is a visible, deliberate test change -- not a silent gap. Checked
        against the SAME `KNOWN_TTL_TABLES` constant the runtime fail-closed
        checks use, not a second hand-copied literal that could drift from
        it independently."""
        retentions = clickhouse_ttl_retentions()
        assert retentions.keys() >= KNOWN_TTL_TABLES

    def test_a_table_with_no_ttl_has_no_safe_backdate_limit(self) -> None:
        assert max_safe_backdate_days("projects") is None

    def test_safe_backdate_is_retention_minus_margin(self) -> None:
        assert (
            max_safe_backdate_days("feature_flag_event") == 90 - TTL_SAFETY_MARGIN_DAYS
        )


class TestGeneratorCeilingHasRealSlackBelowTheGuardThreshold:
    """CHAOS-3602 follow-up bug, caught live on the first re-mint attempt
    after the fix: generation and the mint guard both used
    `max_safe_backdate_days` -- the SAME value -- as their boundary. A row
    generated at exactly that ceiling, checked by the guard a few seconds
    later (ordinary elapsed time within one mint run), was already "at or
    past" the guard's own threshold purely because `now()` had ticked
    forward between generation and the check. The guard fired on its own
    mint's freshly-generated data: 8 `feature_flag_event` rows "at or past
    83 days old" when generation had targeted precisely 83 as its ceiling.

    `max_generated_age_days` must be a genuinely SEPARATE, smaller number
    -- not the same threshold reused for both roles.
    """

    def test_generator_ceiling_is_strictly_below_the_guard_threshold(self) -> None:
        for table in clickhouse_ttl_retentions():
            guard_threshold = max_safe_backdate_days(table)
            generator_ceiling = max_generated_age_days(table)
            assert guard_threshold is not None
            assert generator_ceiling is not None
            assert generator_ceiling < guard_threshold, (
                f"{table}: generator ceiling ({generator_ceiling}) must stay "
                f"strictly below the guard's own threshold "
                f"({guard_threshold}) -- equal values give zero slack for "
                "the ordinary time elapsed between generation and the "
                "guard's check within the same mint run"
            )

    def test_the_slack_is_at_least_a_full_day(self) -> None:
        """A gap of a fraction of a day would still be fragile against a
        slow mint run; require at least a full day of real headroom."""
        for table in clickhouse_ttl_retentions():
            guard_threshold = max_safe_backdate_days(table)
            generator_ceiling = max_generated_age_days(table)
            assert guard_threshold is not None
            assert generator_ceiling is not None
            assert guard_threshold - generator_ceiling >= 1


class TestParserHandlesBothTtlSyntaxForms:
    """Migration 034 uses two different TTL spellings across its own
    tables: `TTL toDateTime(event_ts) + INTERVAL 90 DAY` (a column wrapped
    in a cast) and `TTL day + INTERVAL 365 DAY` (a bare column already of a
    temporal type). Both must parse -- proven against synthetic source, not
    just observed by accident against the real files.
    """

    def _parse_text(self, tmp_path: Path, text: str) -> dict:
        migrations_dir = tmp_path / "clickhouse"
        migrations_dir.mkdir()
        (migrations_dir / "999_synthetic.sql").write_text(text)
        original = ttl_registry_module._MIGRATIONS_DIR
        ttl_registry_module._MIGRATIONS_DIR = migrations_dir
        try:
            clickhouse_ttl_retentions.cache_clear()
            return clickhouse_ttl_retentions()
        finally:
            ttl_registry_module._MIGRATIONS_DIR = original
            clickhouse_ttl_retentions.cache_clear()

    def test_wrapped_column_form(self, tmp_path: Path) -> None:
        retentions = self._parse_text(
            tmp_path,
            "CREATE TABLE IF NOT EXISTS synth_wrapped (\n"
            "    occurred_at DateTime64(3, 'UTC')\n"
            ") ENGINE = MergeTree()\n"
            "TTL toDateTime(occurred_at) + INTERVAL 42 DAY DELETE;",
        )
        assert retentions["synth_wrapped"].column == "occurred_at"
        assert retentions["synth_wrapped"].retention_days == 42

    def test_bare_column_form(self, tmp_path: Path) -> None:
        retentions = self._parse_text(
            tmp_path,
            "CREATE TABLE IF NOT EXISTS synth_bare (\n"
            "    day Date\n"
            ") ENGINE = MergeTree()\n"
            "TTL day + INTERVAL 99 DAY DELETE;",
        )
        assert retentions["synth_bare"].column == "day"
        assert retentions["synth_bare"].retention_days == 99

    def test_a_table_with_no_ttl_clause_is_not_registered(self, tmp_path: Path) -> None:
        retentions = self._parse_text(
            tmp_path,
            "CREATE TABLE IF NOT EXISTS synth_no_ttl (\n"
            "    id String\n"
            ") ENGINE = MergeTree()\n"
            "ORDER BY id;",
        )
        assert "synth_no_ttl" not in retentions


class TestRegistryTracksSourceChanges:
    """The whole point of parsing rather than hand-copying: change the TTL
    in the (synthetic) migration source and the registry MUST reflect the
    new number immediately, proving there is no cached/hardcoded literal
    that could drift out of sync with a real migration edit.
    """

    def test_changing_the_migration_changes_the_registry(self, tmp_path: Path) -> None:
        migrations_dir = tmp_path / "clickhouse"
        migrations_dir.mkdir()
        path = migrations_dir / "999_synthetic.sql"
        path.write_text(
            "CREATE TABLE IF NOT EXISTS synth (\n"
            "    event_ts DateTime64(3, 'UTC')\n"
            ") ENGINE = MergeTree()\n"
            "TTL toDateTime(event_ts) + INTERVAL 30 DAY DELETE;"
        )
        original = ttl_registry_module._MIGRATIONS_DIR
        ttl_registry_module._MIGRATIONS_DIR = migrations_dir
        try:
            clickhouse_ttl_retentions.cache_clear()
            assert clickhouse_ttl_retentions()["synth"].retention_days == 30

            path.write_text(
                "CREATE TABLE IF NOT EXISTS synth (\n"
                "    event_ts DateTime64(3, 'UTC')\n"
                ") ENGINE = MergeTree()\n"
                "TTL toDateTime(event_ts) + INTERVAL 15 DAY DELETE;"
            )
            clickhouse_ttl_retentions.cache_clear()
            assert clickhouse_ttl_retentions()["synth"].retention_days == 15, (
                "the registry did not track a real migration edit -- it is "
                "reading a stale/cached value instead of the source"
            )
        finally:
            ttl_registry_module._MIGRATIONS_DIR = original
            clickhouse_ttl_retentions.cache_clear()


class TestVocabularyConsistency:
    """Codex round-3 finding (HIGH, confirmed): checking a parsed registry
    against KNOWN_TTL_TABLES alone only catches a table falling OUT of an
    otherwise-working registry -- a table that never enters EITHER the
    registry or KNOWN_TTL_TABLES in the first place (an unmatched TTL
    syntax variant) satisfies that check trivially. The registry's own
    "completeness" was being checked against a vocabulary that itself never
    learned about the new table -- pure circularity.

    `assert_ttl_vocabulary_is_consistent` breaks that circularity with a
    THIRD, independent source: a deliberately looser sweep of the same
    migration files.
    """

    def _swap_migrations_dir(self, tmp_path: Path, text: str):
        migrations_dir = tmp_path / "clickhouse"
        migrations_dir.mkdir()
        (migrations_dir / "999_synthetic.sql").write_text(text)
        original = ttl_registry_module._MIGRATIONS_DIR
        ttl_registry_module._MIGRATIONS_DIR = migrations_dir
        clickhouse_ttl_retentions.cache_clear()
        return original

    def _restore_migrations_dir(self, original) -> None:
        ttl_registry_module._MIGRATIONS_DIR = original
        clickhouse_ttl_retentions.cache_clear()

    def test_the_real_repo_is_internally_consistent(self) -> None:
        """Sanity: the actual migrations directory, as it stands today,
        must not trip this check -- otherwise every other test in this
        module (and every guard that calls this first) would be
        unreachable."""
        assert_ttl_vocabulary_is_consistent()  # must not raise

    def test_a_fifth_ttl_table_the_precise_parser_misses_fails_loudly(
        self, tmp_path: Path
    ) -> None:
        """The exact codex round-3 repro: `TTL ... + INTERVAL 4 WEEK` is
        real ClickHouse syntax, but `_TTL_RE` requires the literal word
        "DAY" and never matches it -- so `clickhouse_ttl_retentions` never
        sees this table, and (having never been seen) neither does
        KNOWN_TTL_TABLES. The coarse sweep, which only requires the bare
        substrings "TTL" and "INTERVAL", still finds it -- and that
        disagreement must fail loudly rather than pass vacuously.
        """
        original = self._swap_migrations_dir(
            tmp_path,
            "CREATE TABLE IF NOT EXISTS fifth_ttl_table (\n"
            "    occurred_at DateTime64(3, 'UTC')\n"
            ") ENGINE = MergeTree()\n"
            "TTL occurred_at + INTERVAL 4 WEEK;",
        )
        try:
            # Confirms the precise parser really does miss it -- the
            # premise of this whole test.
            assert "fifth_ttl_table" not in clickhouse_ttl_retentions()

            with pytest.raises(RuntimeError, match="fifth_ttl_table"):
                assert_ttl_vocabulary_is_consistent()
        finally:
            self._restore_migrations_dir(original)

    def test_a_registry_extra_the_coarse_sweep_does_not_see_fails_too(
        self, monkeypatch
    ) -> None:
        """The reverse imbalance: the precise parser reports a table the
        coarse sweep does not -- should be impossible in practice (the
        coarse sweep is a strict superset by construction), but the check
        must be genuinely bidirectional, not one-sided.
        """
        real = clickhouse_ttl_retentions()
        fake_extra = dict(real)
        fake_extra["totally_fictional_table"] = ttl_registry_module.TtlRetention(
            table="totally_fictional_table", column="ts", retention_days=10
        )
        monkeypatch.setattr(
            ttl_registry_module, "clickhouse_ttl_retentions", lambda: fake_extra
        )

        with pytest.raises(RuntimeError, match="totally_fictional_table"):
            assert_ttl_vocabulary_is_consistent()

    def test_a_coarse_table_missing_from_known_ttl_tables_fails(
        self, monkeypatch
    ) -> None:
        """A table the coarse sweep (and, by extension, the precise parser)
        finds, but that KNOWN_TTL_TABLES has not been updated to include --
        the hand-maintained vocabulary itself must be kept honest too."""
        monkeypatch.setattr(
            ttl_registry_module,
            "KNOWN_TTL_TABLES",
            frozenset({"feature_flag_event"}),
        )
        with pytest.raises(RuntimeError, match="not yet added to KNOWN_TTL_TABLES"):
            assert_ttl_vocabulary_is_consistent()

    def test_both_scanners_cover_py_migrations_not_just_sql(
        self, tmp_path: Path
    ) -> None:
        """Standing repo trap (reference_ops_clickhouse_python_migrations):
        this repo's ClickHouse migrations are BOTH .sql AND .py, and a .py
        migration carries its DDL as a Python string -- grepping/globbing
        *.sql alone is a known way to reach a wrong-schema conclusion here.
        A fifth TTL'd table declared ONLY in a .py migration must be found
        by BOTH the precise parser and the coarse sweep, or the exact
        vacuous-pass this test class exists to close reproduces one
        directory over (a .py-only table, invisible to both scanners,
        would satisfy `assert_ttl_vocabulary_is_consistent` just as
        trivially as the WEEK-unit case above). No real .py migration
        declares a TTL today (checked: `grep -l TTL
        migrations/clickhouse/*.py` finds only a docstring mentioning the
        word, `grep -c INTERVAL` on it is 0), so this cannot be demonstrated
        against the real tree -- it would pass whether or not .py were
        scanned. This feeds the parser a .py-sourced TTL directly, so the
        claim is OBSERVED rather than asserted.
        """
        migrations_dir = tmp_path / "clickhouse"
        migrations_dir.mkdir()
        (migrations_dir / "999_added_via_python.py").write_text(
            'SQL = """\n'
            "CREATE TABLE py_sourced_ttl_table (\n"
            "    occurred_at DateTime64(3, 'UTC')\n"
            ") ENGINE = MergeTree()\n"
            "ORDER BY occurred_at\n"
            "TTL toDateTime(occurred_at) + INTERVAL 45 DAY DELETE;\n"
            '"""\n'
        )
        original = ttl_registry_module._MIGRATIONS_DIR
        ttl_registry_module._MIGRATIONS_DIR = migrations_dir
        clickhouse_ttl_retentions.cache_clear()
        try:
            precise = clickhouse_ttl_retentions()
            assert "py_sourced_ttl_table" in precise, (
                "the precise parser did not find a TTL declared inside a "
                ".py migration -- it is scanning *.sql only"
            )
            assert precise["py_sourced_ttl_table"].retention_days == 45

            coarse = ttl_registry_module._coarse_ttl_table_sweep()
            assert "py_sourced_ttl_table" in coarse, (
                "the coarse sweep did not find a TTL declared inside a .py "
                "migration -- it is scanning *.sql only, which would let a "
                "fifth TTL'd table added via a .py migration alone "
                "reproduce the exact vacuous-pass this sweep exists to "
                "close, one directory over"
            )
        finally:
            ttl_registry_module._MIGRATIONS_DIR = original
            clickhouse_ttl_retentions.cache_clear()


class TestComputeDriftDays:
    def test_no_drift_when_pinned_now_is_real_now(self) -> None:
        now = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
        assert compute_drift_days(now, now) == 0

    def test_a_future_pinned_now_needs_no_compensation(self) -> None:
        real_now = datetime(2026, 8, 8, tzinfo=timezone.utc)
        pinned_now = real_now + timedelta(days=5)
        assert compute_drift_days(pinned_now, real_now) == 0

    def test_three_days_stale_matches_the_incident(self) -> None:
        """The exact scenario that blocked a real mint: pinned_now =
        2026-08-05, real now = 2026-08-08."""
        pinned_now = datetime(2026, 8, 5, tzinfo=timezone.utc)
        real_now = datetime(2026, 8, 8, tzinfo=timezone.utc)
        assert compute_drift_days(pinned_now, real_now) == 3

    def test_a_partial_day_of_drift_rounds_up(self) -> None:
        pinned_now = datetime(2026, 8, 5, 0, 0, tzinfo=timezone.utc)
        real_now = datetime(2026, 8, 6, 1, 0, tzinfo=timezone.utc)  # 1 day + 1 hour
        assert compute_drift_days(pinned_now, real_now) == 2


class TestDriftThreadedIntoTheClamp:
    """CHAOS-3602 follow-up, live incident: generation clamped ages against
    `max_safe_backdate_days` alone, ignorant of how stale `pinned_now` had
    become. A world minted with `pinned_now` 3 days behind real time
    produced rows the mint guard correctly flagged as past its own
    threshold -- the exact 3-day-drift case reproduced here.
    """

    def test_zero_drift_leaves_the_ceiling_unchanged(self) -> None:
        guard_threshold = max_safe_backdate_days("feature_flag_event")
        assert guard_threshold is not None
        with drift_days_context(0):
            assert (
                max_generated_age_days("feature_flag_event")
                == guard_threshold - ttl_registry_module.GENERATOR_SLACK_DAYS
            )

    def test_three_day_drift_shaves_three_more_days_off_the_ceiling(self) -> None:
        """The live incident, reproduced deterministically: with no drift
        threaded, generation's own ceiling was left exactly 3 days too high
        for a pinned_now that was 3 days stale."""
        with drift_days_context(0):
            undrifted_ceiling = max_generated_age_days("feature_flag_event")
        assert undrifted_ceiling is not None
        with drift_days_context(3):
            drifted_ceiling = max_generated_age_days("feature_flag_event")
        assert drifted_ceiling == undrifted_ceiling - 3

    def test_drift_never_pushes_the_ceiling_negative(self) -> None:
        with drift_days_context(10_000):
            assert max_generated_age_days("feature_flag_event") == 0

    def test_drift_context_is_scoped_and_reverts(self) -> None:
        baseline = max_generated_age_days("feature_flag_event")
        assert baseline is not None
        with drift_days_context(5):
            assert max_generated_age_days("feature_flag_event") == baseline - 5
        assert max_generated_age_days("feature_flag_event") == baseline

    def test_a_table_with_no_ttl_ignores_drift_entirely(self) -> None:
        with drift_days_context(5):
            assert max_generated_age_days("projects") is None


class TestPinnedNowStalenessCeiling:
    """A mint must refuse rather than silently squeeze the generatable-
    history window to compensate for an unbounded amount of pinned_now
    drift."""

    def test_drift_inside_the_ceiling_returns_the_drift_and_does_not_raise(
        self,
    ) -> None:
        pinned_now = datetime(2026, 8, 1, tzinfo=timezone.utc)
        real_now = pinned_now + timedelta(days=MAX_PINNED_NOW_STALENESS_DAYS - 1)
        assert (
            assert_pinned_now_not_too_stale(pinned_now, real_now)
            == MAX_PINNED_NOW_STALENESS_DAYS - 1
        )

    def test_drift_past_the_ceiling_is_refused_with_a_named_fix(self) -> None:
        pinned_now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        real_now = pinned_now + timedelta(days=MAX_PINNED_NOW_STALENESS_DAYS + 1)
        with pytest.raises(PinnedNowStaleError) as exc_info:
            assert_pinned_now_not_too_stale(pinned_now, real_now)
        message = str(exc_info.value)
        assert "pinned_now" in message
        assert "re-pinning is a deliberate decision" in message

    def test_drift_exactly_at_the_ceiling_is_still_allowed(self) -> None:
        pinned_now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        real_now = pinned_now + timedelta(days=MAX_PINNED_NOW_STALENESS_DAYS)
        assert (
            assert_pinned_now_not_too_stale(pinned_now, real_now)
            == MAX_PINNED_NOW_STALENESS_DAYS
        )


class TestSnapshotShelfLife:
    """A minted snapshot's dates are frozen relative to pinned_now, but
    ClickHouse's TTL enforcement always runs on real time -- once enough
    real time passes, the snapshot's oldest rows are due for a live TTL
    merge regardless of what any boot does."""

    def test_shelf_life_is_margin_minus_restore_slack(self) -> None:
        assert (
            snapshot_shelf_life_days()
            == TTL_SAFETY_MARGIN_DAYS - RESTORE_SIDE_SLACK_DAYS
        )

    def test_expiry_is_pinned_now_plus_shelf_life(self) -> None:
        pinned_now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert snapshot_expiry(pinned_now) == pinned_now + timedelta(
            days=snapshot_shelf_life_days()
        )

    def test_restoring_before_expiry_is_allowed(self) -> None:
        pinned_now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        real_now = snapshot_expiry(pinned_now) - timedelta(days=1)
        assert_snapshot_not_expired(pinned_now, real_now)  # must not raise

    def test_restoring_at_or_past_expiry_is_refused_with_a_named_fix(self) -> None:
        pinned_now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        real_now = snapshot_expiry(pinned_now)
        with pytest.raises(SnapshotExpiredError) as exc_info:
            assert_snapshot_not_expired(pinned_now, real_now)
        message = str(exc_info.value)
        assert "expired" in message
        assert "re-mint required" in message
