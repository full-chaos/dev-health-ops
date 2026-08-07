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
