"""CHAOS-3602: the TTL registry must describe what ClickHouse ACTUALLY
enforces, parsed from the migration source itself -- never a hand-copied
number that can silently drift once a migration's TTL changes.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dev_health_ops.fixtures import ttl_registry as ttl_registry_module
from dev_health_ops.fixtures.ttl_registry import (
    KNOWN_TTL_TABLES,
    TTL_SAFETY_MARGIN_DAYS,
    assert_ttl_vocabulary_is_consistent,
    clickhouse_ttl_retentions,
    max_safe_backdate_days,
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
            ttl_registry_module._coarse_ttl_table_sweep.cache_clear()
            return clickhouse_ttl_retentions()
        finally:
            ttl_registry_module._MIGRATIONS_DIR = original
            clickhouse_ttl_retentions.cache_clear()
            ttl_registry_module._coarse_ttl_table_sweep.cache_clear()

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
            ttl_registry_module._coarse_ttl_table_sweep.cache_clear()


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
        ttl_registry_module._coarse_ttl_table_sweep.cache_clear()
        return original

    def _restore_migrations_dir(self, original) -> None:
        ttl_registry_module._MIGRATIONS_DIR = original
        clickhouse_ttl_retentions.cache_clear()
        ttl_registry_module._coarse_ttl_table_sweep.cache_clear()

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
        ttl_registry_module._coarse_ttl_table_sweep.cache_clear()
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
            ttl_registry_module._coarse_ttl_table_sweep.cache_clear()


class TestCoarseSweepDoesNotMatchProse:
    """Codex round-4 finding (HIGH, confirmed LIVE against the real tree):
    the original coarse table-name regex had no line anchor and no
    trailing ``(``, so it matched prose mid-sentence -- exactly the class
    PR #1602 review D already hardened the PRECISE parser against.
    Reproduced directly: running the old regex against
    055_work_item_daily_rollups_replacing_merge_tree.py's own docstring
    yielded phantom tables ``'to'`` (from "SHOW CREATE TABLE to get the
    live DDL") and ``'statement'`` (from "the table name in a CREATE TABLE
    statement."). Both were silenced only because that specific chunk
    happens to lack the substring "INTERVAL" -- one unrelated docstring
    edit anywhere near either sentence would have made
    ``assert_ttl_vocabulary_is_consistent`` abort ALL fixture generation
    over a table that does not exist.
    """

    def test_the_055_docstring_produces_no_phantom_tables_today(self) -> None:
        """Pins the CURRENT real-tree state: this exact phantom must not
        appear, so a regression is caught immediately."""
        coarse = ttl_registry_module._coarse_ttl_table_sweep()
        assert "to" not in coarse
        assert "statement" not in coarse

    def test_the_055_docstring_stays_safe_even_if_interval_appears_nearby(
        self, tmp_path: Path
    ) -> None:
        """The adversarial case the real tree currently avoids only by
        accident (that docstring paragraph happens to lack "INTERVAL"):
        inject the exact 055 prose PLUS the word "interval" into a
        synthetic migration and confirm the coarse sweep still finds no
        phantom table -- proving the fix is STRUCTURAL (the anchored,
        shared ``_CREATE_TABLE_RE``), not merely "this file doesn't
        currently trip it".
        """
        migrations_dir = tmp_path / "clickhouse"
        migrations_dir.mkdir()
        (migrations_dir / "999_synthetic.py").write_text(
            '"""\n'
            "    1. SHOW CREATE TABLE to get the live DDL (preserves columns,\n"
            "       partitioning, the existing ORDER BY, settings, codecs, TTL,\n"
            "       and the retention interval).\n"
            '"""\n'
            "def regex_for_table_name():\n"
            '    """Regex for the table name in a CREATE TABLE statement."""\n'
        )
        original = ttl_registry_module._MIGRATIONS_DIR
        ttl_registry_module._MIGRATIONS_DIR = migrations_dir
        clickhouse_ttl_retentions.cache_clear()
        ttl_registry_module._coarse_ttl_table_sweep.cache_clear()
        try:
            coarse = ttl_registry_module._coarse_ttl_table_sweep()
            assert coarse == frozenset(), (
                f"expected no phantom tables, got {sorted(coarse)}"
            )
        finally:
            ttl_registry_module._MIGRATIONS_DIR = original
            clickhouse_ttl_retentions.cache_clear()
            ttl_registry_module._coarse_ttl_table_sweep.cache_clear()


class TestCoarseTtlDetectionIsWordBoundaried:
    """Codex round-4 finding (MED): naive substring checks ("TTL" in text,
    "INTERVAL" in text) match INSIDE other identifiers. Confirmed a real
    occurrence in this repo's migrations: ``toIntervalDay`` (used in
    008_grafana_panel_views.sql's view definitions) contains "Interval" as
    a substring. It does not currently misfire only because that specific
    statement is a ``CREATE OR REPLACE VIEW`` (not a table) and lacks "TTL"
    too -- the word-boundary fix must hold regardless of that coincidence.
    """

    def test_tointervaday_next_to_a_real_ttl_keyword_does_not_satisfy_the_pattern(
        self,
    ) -> None:
        """A genuine "TTL" keyword co-occurring with `toIntervalDay` (which
        contains "Interval" as a bare substring, not a keyword) must NOT be
        treated as a TTL clause -- this is the scenario a naive substring
        check gets wrong; matching this string is exactly the failure mode
        being guarded against.
        """
        assert not ttl_registry_module._COARSE_TTL_RE.search(
            "-- TTL note: see toIntervalDay(30) for the window function"
        )

    def test_a_hypothetical_throttled_table_next_to_a_real_interval_does_not_match(
        self,
    ) -> None:
        """A table NAME containing "TTL" as a substring (not a keyword),
        co-occurring with a genuine "INTERVAL" elsewhere, must NOT be
        treated as a TTL clause.
        """
        assert not ttl_registry_module._COARSE_TTL_RE.search(
            "CREATE TABLE throttled_requests (x String) ENGINE = MergeTree() "
            "-- retry INTERVAL is configured elsewhere"
        )

    def test_a_genuine_ttl_clause_with_any_time_unit_still_matches(self) -> None:
        for unit in ("DAY", "WEEK", "MONTH", "HOUR"):
            assert ttl_registry_module._COARSE_TTL_RE.search(
                f"TTL occurred_at + INTERVAL 4 {unit}"
            ), f"failed to match a bare TTL clause using {unit}"


class TestAlterTableModifyTtlCoverage:
    """Codex round-4 finding (MED, confirmed dormant): ``ALTER TABLE x
    MODIFY TTL ...`` -- the standard way to add or change retention on an
    EXISTING table -- was invisible to all three sources (precise parser,
    coarse sweep, KNOWN_TTL_TABLES): a CREATE-time-only blind spot shared
    by all of them. No real migration uses this syntax today, so it cannot
    be demonstrated against the real tree; proven synthetically instead.
    """

    def test_the_real_tree_has_no_alter_table_modify_ttl_today(self) -> None:
        """Documents the premise this test class depends on: if this ever
        becomes false, the synthetic tests below stop being the only
        coverage and this repo's own migrations should be re-examined."""
        for path in sorted(ttl_registry_module._MIGRATIONS_DIR.glob("*")):
            if path.suffix not in {".sql", ".py"}:
                continue
            assert not ttl_registry_module._ALTER_TABLE_MODIFY_TTL_RE.search(
                path.read_text(encoding="utf-8")
            ), (
                f"{path.name} has an ALTER TABLE MODIFY TTL -- update this test's premise"
            )

    def test_an_alter_table_modify_ttl_is_found_by_the_coarse_sweep(
        self, tmp_path: Path
    ) -> None:
        migrations_dir = tmp_path / "clickhouse"
        migrations_dir.mkdir()
        (migrations_dir / "999_synthetic.sql").write_text(
            "ALTER TABLE existing_table MODIFY TTL created_at + INTERVAL 60 DAY;"
        )
        original = ttl_registry_module._MIGRATIONS_DIR
        ttl_registry_module._MIGRATIONS_DIR = migrations_dir
        clickhouse_ttl_retentions.cache_clear()
        ttl_registry_module._coarse_ttl_table_sweep.cache_clear()
        try:
            coarse = ttl_registry_module._coarse_ttl_table_sweep()
            assert "existing_table" in coarse, (
                "the coarse sweep did not find a table whose TTL was added "
                "via ALTER TABLE ... MODIFY TTL -- the CREATE-only blind "
                "spot is still open"
            )
        finally:
            ttl_registry_module._MIGRATIONS_DIR = original
            clickhouse_ttl_retentions.cache_clear()
            ttl_registry_module._coarse_ttl_table_sweep.cache_clear()


class TestMultiTableChunkAttribution:
    """Codex round-4 finding (MED, confirmed dormant): the original
    ``.search()``-based version always paired the FIRST CREATE TABLE with
    the FIRST TTL clause in a ``;``-delimited chunk -- wrong if a LATER
    CREATE TABLE in the SAME chunk (no semicolons at all, as several real
    ``.py`` migrations have -- 027, 028, 067 each have zero, checked
    directly) owns the TTL clause. No real ``.py`` migration currently
    carries a TTL at all, so this is proven synthetically.
    """

    def test_a_ttl_in_the_second_table_of_a_no_semicolon_chunk_is_attributed_correctly(
        self, tmp_path: Path
    ) -> None:
        migrations_dir = tmp_path / "clickhouse"
        migrations_dir.mkdir()
        (migrations_dir / "999_synthetic.py").write_text(
            "SQL = '''\n"
            "CREATE TABLE first_table (\n"
            "    id String\n"
            ") ENGINE = MergeTree() ORDER BY id\n"
            "CREATE TABLE second_table (\n"
            "    occurred_at DateTime64(3, 'UTC')\n"
            ") ENGINE = MergeTree()\n"
            "TTL toDateTime(occurred_at) + INTERVAL 20 DAY DELETE\n"
            "'''\n"
        )
        original = ttl_registry_module._MIGRATIONS_DIR
        ttl_registry_module._MIGRATIONS_DIR = migrations_dir
        clickhouse_ttl_retentions.cache_clear()
        ttl_registry_module._coarse_ttl_table_sweep.cache_clear()
        try:
            retentions = clickhouse_ttl_retentions()
            assert "second_table" in retentions, (
                "the TTL clause belonging to the SECOND CREATE TABLE in a "
                "semicolon-free chunk was not attributed to it"
            )
            assert "first_table" not in retentions, (
                "the TTL was misattributed to the FIRST CREATE TABLE in "
                "the chunk instead of the one it actually follows"
            )
            assert retentions["second_table"].retention_days == 20

            coarse = ttl_registry_module._coarse_ttl_table_sweep()
            assert "second_table" in coarse
            assert "first_table" not in coarse
        finally:
            ttl_registry_module._MIGRATIONS_DIR = original
            clickhouse_ttl_retentions.cache_clear()
            ttl_registry_module._coarse_ttl_table_sweep.cache_clear()
