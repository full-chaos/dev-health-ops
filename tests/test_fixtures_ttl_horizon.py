"""CHAOS-3432/3544: generated fixture history must stay clear of every TTL.

The defect: ClickHouse tables carry ``TTL <col> + INTERVAL n DAY DELETE``,
and the generators wrote history to the edge of the tightest one. The oldest
rows sat exactly on the boundary at generation time, so the database deleted
them on a restore days later -- the committed snapshot bytes unchanged, the
restored table not. Measured against the committed snapshot on 2026-08-07:
1045 rows, 1 already expired, 24 more due within seven days.

These guards keep the class closed rather than the instance fixed.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest

from dev_health_ops.fixtures.ttl_horizon import (
    TTL_SAFETY_MARGIN,
    clickhouse_ttl_horizons,
    max_generated_history_days,
    tightest_ttl_days,
)


def test_ttl_horizons_are_derived_from_the_real_schema() -> None:
    """The horizons come from the migrations, not from a list someone
    maintains by hand.

    A hardcoded list is how a future migration silently rejoins the decay
    class: the list still looks complete, and nothing connects the new table
    to the failure it eventually causes. This asserts the derivation finds
    the clauses that actually exist.
    """

    horizons = clickhouse_ttl_horizons()

    assert horizons, (
        "no TTL clauses found -- this helper exists BECAUSE the schema has "
        "them, so an empty result means the pattern stopped matching (a "
        "migration reworded its TTL), not that the risk disappeared"
    )
    assert "034_feature_flag_user_impact_tables.sql" in horizons, (
        "the 90-day feature_flag_event TTL is the horizon that actually bit "
        "-- if it is no longer found, this guard is measuring nothing"
    )
    assert tightest_ttl_days() == min(horizons.values())


def test_generated_history_stops_a_full_margin_inside_the_tightest_ttl() -> None:
    """The arithmetic that defines the snapshot's shelf life."""

    assert max_generated_history_days() == (
        tightest_ttl_days() - TTL_SAFETY_MARGIN.days
    )
    assert max_generated_history_days() > 0, (
        "the margin has grown past the tightest TTL, leaving no room to "
        "generate history at all"
    )


def test_feature_flag_events_land_inside_the_ttl_horizon() -> None:
    """The producer itself, executed -- not its constants re-read.

    Runs the real generator and checks the oldest event it emits. This is
    what would have caught the original defect: with ``randint(7, 90)``
    against a 90-day TTL the oldest event lands exactly on the boundary, and
    this assertion fails by the full margin.
    """

    from dev_health_ops.fixtures.generator import SyntheticDataGenerator

    random.seed(20260805)
    generator = SyntheticDataGenerator()
    flags = generator.generate_feature_flags(count=25, org_id="org")
    events = generator.generate_feature_flag_events(flags, org_id="org")

    assert events, "the generator produced no events, so this guard measured nothing"

    # The generator anchors on its own `now`; measure against that rather
    # than a pinned date, so this cannot pass by disagreeing about "today".
    now = max(event.event_ts for event in events)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    horizon = now - timedelta(days=max_generated_history_days())
    oldest = min(event.event_ts for event in events)
    if oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=UTC)

    assert oldest >= horizon, (
        "generated feature-flag history reaches "
        f"{(now - oldest).days} days back, past the {max_generated_history_days()}-"
        f"day limit that keeps it a full {TTL_SAFETY_MARGIN.days}-day margin "
        f"inside the {tightest_ttl_days()}-day TTL. Rows this old are deleted "
        "by ClickHouse on restore, which breaks the content oracle with no "
        "code change anywhere."
    )


class TestSnapshotShelfLife:
    """The typed expiry preflight (CHAOS-3432/3544).

    The real cost of TTL decay was never the drift itself -- it was that the
    drift presented as ``feature_flag_event: source=32c53f52 target=0160527f``
    and got attributed to generator nondeterminism for months. A named
    staleness error is the difference between a five-minute re-mint and a
    night of archaeology for the same cause.
    """

    @staticmethod
    def _document(*, age_days: int | None, shelf_life: int | None = None) -> dict:
        document: dict = {}
        if age_days is not None:
            minted = datetime.now(UTC) - timedelta(days=age_days)
            document["minted_at"] = minted.isoformat()
        if shelf_life is not None:
            document["shelf_life_days"] = shelf_life
        return document

    def test_a_fresh_snapshot_passes(self) -> None:
        from dev_health_ops.fixtures.world_snapshot import (
            _assert_snapshot_within_shelf_life,
        )

        _assert_snapshot_within_shelf_life(
            self._document(age_days=1, shelf_life=TTL_SAFETY_MARGIN.days)
        )

    def test_a_stale_snapshot_fails_by_NAME_not_by_hash_mismatch(self) -> None:
        """The whole point: the failure says what is wrong and what to do."""

        from dev_health_ops.fixtures.world_snapshot import (
            SnapshotExpiredError,
            _assert_snapshot_within_shelf_life,
        )

        with pytest.raises(SnapshotExpiredError) as raised:
            _assert_snapshot_within_shelf_life(
                self._document(
                    age_days=TTL_SAFETY_MARGIN.days + 1,
                    shelf_life=TTL_SAFETY_MARGIN.days,
                )
            )

        message = str(raised.value)
        assert "SNAPSHOT EXPIRED" in message
        assert "re-mint required" in message, (
            "the error must name the REMEDY, not just the condition -- the "
            "failure this replaces was actionable only after archaeology"
        )

    def test_a_snapshot_exactly_at_its_shelf_life_is_still_accepted(self) -> None:
        """Boundary: the margin is the guarantee, so the last day inside it
        must still restore. An off-by-one here would fail a snapshot that is
        still perfectly valid and send someone re-minting for nothing."""

        from dev_health_ops.fixtures.world_snapshot import (
            _assert_snapshot_within_shelf_life,
        )

        _assert_snapshot_within_shelf_life(
            self._document(
                age_days=TTL_SAFETY_MARGIN.days, shelf_life=TTL_SAFETY_MARGIN.days
            )
        )

    def test_a_pre_field_snapshot_is_not_rejected(self) -> None:
        """Snapshots minted before this field existed cannot be checked.

        Refusing them would break restores that are still valid; silently
        treating them as fresh would be a false pass. It warns and proceeds,
        and the content oracle remains the backstop.
        """

        from dev_health_ops.fixtures.world_snapshot import (
            _assert_snapshot_within_shelf_life,
        )

        _assert_snapshot_within_shelf_life({})

    def test_the_preflight_runs_BEFORE_the_content_oracle(self) -> None:
        """Ordering is the whole value.

        Both would fail on a stale snapshot. Only the staleness error says
        why. If the oracle ran first, this change would add a second
        confusing failure rather than replacing one.
        """

        import inspect

        from dev_health_ops.fixtures import world_snapshot

        source = inspect.getsource(world_snapshot.restore_world)
        assert source.index("_assert_snapshot_within_shelf_life") < source.index(
            "_assert_content_identity"
        ), "the staleness preflight must precede the content oracle"


class TestShelfLifeAccountsForPinnedNowDrift:
    """The recorded shelf life must be the REAL one, not the nominal margin.

    Generators place history relative to the world's ``pinned_now``, but
    ClickHouse evaluates TTLs against the wall clock. Every day between
    ``pinned_now`` and the mint is margin already spent.

    Measured on the first re-mint under CHAOS-3544: 60-day history cap, a
    ``pinned_now`` of 2026-08-05, minted 2026-08-07 -- oldest row 62 days old
    against a 90-day TTL, so 28 days of real shelf life against a nominal 30.
    Recording 30 would leave a two-day window where rows decay while the
    preflight still reports the snapshot fresh: the exact cryptic failure the
    preflight exists to replace, reintroduced by an optimistic constant.
    """

    @staticmethod
    def _manifest(pinned_now: datetime):
        class _M:
            world = {"pinned_now": pinned_now.isoformat()}

        return _M()

    def test_days_since_pinned_now_are_deducted(self) -> None:
        from dev_health_ops.fixtures.world_snapshot import _shelf_life_days

        spent = 2
        recorded = _shelf_life_days(
            self._manifest(datetime.now(UTC) - timedelta(days=spent))
        )
        assert recorded == TTL_SAFETY_MARGIN.days - spent, (
            "the gap between pinned_now and the mint is margin already spent "
            "and must be deducted, or the snapshot claims a shelf life it "
            "does not have"
        )

    def test_a_world_minted_on_its_pinned_now_gets_the_full_margin(self) -> None:
        from dev_health_ops.fixtures.world_snapshot import _shelf_life_days

        assert (
            _shelf_life_days(self._manifest(datetime.now(UTC)))
            == TTL_SAFETY_MARGIN.days
        )

    def test_a_world_older_than_the_margin_reports_no_shelf_life(self) -> None:
        """It must not go negative and read as "fresh" through a comparison."""

        from dev_health_ops.fixtures.world_snapshot import _shelf_life_days

        assert (
            _shelf_life_days(
                self._manifest(
                    datetime.now(UTC) - timedelta(days=TTL_SAFETY_MARGIN.days + 10)
                )
            )
            == 0
        )


class TestParseSurfaceCoversPyMigrations:
    """The parser must see .py migrations, not only .sql.

    This repo's ClickHouse migrations are BOTH (72 .sql, 10 .py today), and a
    .py migration carries its DDL as SQL strings. A parser that globs *.sql
    alone would miss a TTL arriving that way -- and this helper exists
    precisely so that a future TTL table cannot silently rejoin the decay
    class.

    No .py migration declares a TTL today, so the coverage cannot be
    demonstrated against the real tree: it would pass whether or not .py were
    parsed. These feed the parser a .py-sourced TTL directly, so the claim is
    OBSERVED rather than asserted.
    """

    @staticmethod
    def _write(directory, name: str, body: str) -> None:
        (directory / name).write_text(body, encoding="utf-8")

    def test_a_ttl_declared_in_a_py_migration_is_found(self, tmp_path) -> None:
        self._write(
            tmp_path,
            "099_added_via_python.py",
            'SQL = """\n'
            "CREATE TABLE late_arrival (ts DateTime) ENGINE = MergeTree()\n"
            "ORDER BY ts\n"
            "TTL toDateTime(ts) + INTERVAL 45 DAY DELETE;\n"
            '"""\n',
        )

        horizons = clickhouse_ttl_horizons(tmp_path)

        assert horizons == {"099_added_via_python.py": 45}, (
            "a TTL declared inside a .py migration must be found -- missing "
            "it is how a future table rejoins the decay class in silence, "
            "which is the exact failure this helper exists to prevent"
        )

    def test_a_py_sourced_ttl_can_become_the_binding_horizon(self, tmp_path) -> None:
        """The consequence that matters: it must be able to TIGHTEN the cap.

        Finding the clause but not letting it bind would leave generators
        writing history past a horizon the parser can see.
        """

        self._write(
            tmp_path,
            "001_loose.sql",
            "TTL toDateTime(a) + INTERVAL 200 DAY DELETE;",
        )
        self._write(
            tmp_path,
            "099_tight.py",
            'SQL = "TTL toDateTime(b) + INTERVAL 45 DAY DELETE;"',
        )

        assert tightest_ttl_days(tmp_path) == 45
        assert max_generated_history_days(tmp_path) == 45 - TTL_SAFETY_MARGIN.days

    def test_the_real_tree_actually_contains_py_migrations(self) -> None:
        """Rule 4: if the .py migrations ever vanish, the controls above are
        still green but no longer describe this repo -- so the coverage claim
        would quietly stop meaning anything."""

        from dev_health_ops.fixtures.ttl_horizon import _migrations_dir

        assert list(_migrations_dir().glob("*.py")), (
            "no .py ClickHouse migrations found -- the .py parse surface was "
            "added because this repo has them; if that changed, revisit "
            "whether these controls still describe reality"
        )


class TestPerTableCeilingCompatibleWithShelfLife:
    """CHAOS-3602 port, codex finding (HIGH, confirmed): a per-table
    generator ceiling that ignores THIS module's TTL_SAFETY_MARGIN can drift
    out of sync with the shelf-life contract ``_assert_snapshot_within_
    shelf_life`` actually enforces at restore. Measured live before this
    fix: ttl_registry.py's own (looser) margin let ``telemetry_signal_
    bucket`` clamp to 80 days against a 90-day TTL -- add the full 30-day
    advertised shelf life and a restore at that boundary finds rows 110
    days old, 20 days PAST the horizon, already gone to a TTL merge.

    The invariant that must hold for every TTL'd table: a row generated at
    the ceiling, restored at the full advertised shelf life later, must
    still be STRICTLY inside its own TTL horizon -- codex round-2 (HIGH,
    confirmed): landing EXACTLY on the horizon is not safe, since this
    project's own live pre-dump guard already treats a row `<= now() - N
    DAY` old as unsafe and ClickHouse's TTL deletion is asynchronous.
    """

    def test_every_known_ttl_table_ceiling_survives_the_full_shelf_life(self) -> None:
        from dev_health_ops.fixtures.ttl_horizon import max_generated_age_days_for_table
        from dev_health_ops.fixtures.ttl_registry import clickhouse_ttl_retentions

        for table, retention in clickhouse_ttl_retentions().items():
            ceiling = max_generated_age_days_for_table(table)
            assert ceiling is not None
            age_at_full_shelf_life = ceiling + TTL_SAFETY_MARGIN.days
            assert age_at_full_shelf_life < retention.retention_days, (
                f"{table}: a row generated at the {ceiling}-day ceiling and "
                f"restored {TTL_SAFETY_MARGIN.days} days later (the full "
                f"advertised shelf life) would be {age_at_full_shelf_life} "
                f"days old -- at or past its {retention.retention_days}-day "
                "TTL is not strictly inside it. The restore contract "
                "promises a shelf life this ceiling cannot actually honor "
                "with real margin to spare"
            )

    def test_ceiling_uses_this_modules_margin_not_a_different_one(self) -> None:
        """Pins the exact arithmetic so a future edit that quietly reuses a
        different (e.g. ttl_registry.py's own) margin is caught."""
        from dev_health_ops.fixtures.ttl_horizon import (
            PER_TABLE_CEILING_HEADROOM_DAYS,
            max_generated_age_days_for_table,
        )

        assert max_generated_age_days_for_table("feature_flag_event") == (
            90 - TTL_SAFETY_MARGIN.days - PER_TABLE_CEILING_HEADROOM_DAYS
        )

    def test_a_table_with_no_ttl_has_no_ceiling(self) -> None:
        from dev_health_ops.fixtures.ttl_horizon import max_generated_age_days_for_table

        assert max_generated_age_days_for_table("projects") is None

    def test_an_empty_registry_fails_closed(self, monkeypatch) -> None:
        """Same fail-closed rule as the mint-time guard: a broken registry
        must not silently disable this ceiling's protection. Now routed
        through `assert_ttl_vocabulary_is_consistent` (codex round-3) --
        the independent coarse sweep, reading the REAL migrations
        unaffected by this patch, is what actually surfaces the
        disagreement."""
        from dev_health_ops.fixtures import ttl_horizon as ttl_horizon_module

        monkeypatch.setattr(
            "dev_health_ops.fixtures.ttl_registry.clickhouse_ttl_retentions",
            lambda: {},
        )
        with pytest.raises(RuntimeError, match="inconsistent"):
            ttl_horizon_module.max_generated_age_days_for_table("feature_flag_event")

    def test_a_partial_registry_fails_closed_too(self, monkeypatch) -> None:
        """Codex round-2 finding (HIGH, confirmed): a NON-empty registry
        missing just ONE known TTL'd table used to be indistinguishable
        from "genuinely no TTL" for that table -- reproduced directly with
        a fake client that would have reported 999999 violating rows for
        `telemetry_signal_bucket` had it ever been queried. Every table in
        KNOWN_TTL_TABLES must be present, not merely the registry being
        non-empty.
        """
        from dev_health_ops.fixtures import ttl_horizon as ttl_horizon_module
        from dev_health_ops.fixtures.ttl_registry import (
            KNOWN_TTL_TABLES,
            clickhouse_ttl_retentions,
        )

        real = clickhouse_ttl_retentions()
        partial = {
            table: retention
            for table, retention in real.items()
            if table != "telemetry_signal_bucket"
        }
        assert "telemetry_signal_bucket" in KNOWN_TTL_TABLES
        monkeypatch.setattr(
            "dev_health_ops.fixtures.ttl_registry.clickhouse_ttl_retentions",
            lambda: partial,
        )
        with pytest.raises(RuntimeError, match="telemetry_signal_bucket"):
            ttl_horizon_module.max_generated_age_days_for_table("feature_flag_event")
