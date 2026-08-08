from datetime import datetime, timezone

import pytest

from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.fixtures.ttl_registry import max_safe_backdate_days
from dev_health_ops.metrics.schemas import (
    FeatureFlagEventRecord,
    FeatureFlagRecord,
)


@pytest.fixture
def generator() -> SyntheticDataGenerator:
    return SyntheticDataGenerator(repo_name="acme/demo-app", seed=42)


@pytest.fixture
def flags(generator: SyntheticDataGenerator) -> list[FeatureFlagRecord]:
    return generator.generate_feature_flags(count=10, org_id="test-org")


class TestGenerateFeatureFlags:
    def test_returns_nonempty(self, flags: list[FeatureFlagRecord]) -> None:
        assert len(flags) > 0

    def test_count_matches_request(self, generator: SyntheticDataGenerator) -> None:
        flags = generator.generate_feature_flags(count=5)
        assert len(flags) == 5

    def test_flag_keys_are_realistic(self, flags: list[FeatureFlagRecord]) -> None:
        for flag in flags:
            assert flag.flag_key
            assert "-" in flag.flag_key or flag.flag_key.isalpha()

    def test_repo_id_set(self, flags: list[FeatureFlagRecord]) -> None:
        for flag in flags:
            assert flag.repo_id is not None

    def test_providers_valid(self, flags: list[FeatureFlagRecord]) -> None:
        valid = {"launchdarkly", "github"}
        for flag in flags:
            assert flag.provider in valid

    def test_some_archived(self, flags: list[FeatureFlagRecord]) -> None:
        archived = [f for f in flags if f.archived_at is not None]
        active = [f for f in flags if f.archived_at is None]
        assert len(archived) > 0
        assert len(active) > 0


class TestGenerateFeatureFlagEvents:
    def test_returns_nonempty(
        self,
        generator: SyntheticDataGenerator,
        flags: list[FeatureFlagRecord],
    ) -> None:
        events = generator.generate_feature_flag_events(flags)
        assert len(events) > 0

    def test_first_event_per_flag_is_create(
        self,
        generator: SyntheticDataGenerator,
        flags: list[FeatureFlagRecord],
    ) -> None:
        events = generator.generate_feature_flag_events(flags)
        first_per_flag: dict[str, FeatureFlagEventRecord] = {}
        for evt in events:
            if (
                evt.flag_key not in first_per_flag
                or evt.event_ts < first_per_flag[evt.flag_key].event_ts
            ):
                first_per_flag[evt.flag_key] = evt
        for flag_key, evt in first_per_flag.items():
            assert evt.event_type == "create", (
                f"First event for {flag_key} should be 'create', got '{evt.event_type}'"
            )

    def test_events_chronologically_ordered(
        self,
        generator: SyntheticDataGenerator,
        flags: list[FeatureFlagRecord],
    ) -> None:
        events = generator.generate_feature_flag_events(flags)
        for i in range(1, len(events)):
            assert events[i].event_ts >= events[i - 1].event_ts

    def test_dedupe_keys_unique(
        self,
        generator: SyntheticDataGenerator,
        flags: list[FeatureFlagRecord],
    ) -> None:
        events = generator.generate_feature_flag_events(flags)
        keys = [e.dedupe_key for e in events]
        assert len(keys) == len(set(keys))

    def test_valid_event_types(
        self,
        generator: SyntheticDataGenerator,
        flags: list[FeatureFlagRecord],
    ) -> None:
        valid = {"create", "toggle", "update", "rule", "rollout"}
        events = generator.generate_feature_flag_events(flags)
        for evt in events:
            assert evt.event_type in valid


class TestGenerateFeatureFlagLinks:
    def test_returns_nonempty(
        self,
        generator: SyntheticDataGenerator,
        flags: list[FeatureFlagRecord],
    ) -> None:
        links = generator.generate_feature_flag_links(flags)
        assert len(links) > 0

    def test_confidence_levels_mixed(
        self,
        generator: SyntheticDataGenerator,
        flags: list[FeatureFlagRecord],
    ) -> None:
        links = generator.generate_feature_flag_links(flags)
        confidences = {link.confidence for link in links}
        assert len(confidences) > 0

    def test_target_types_valid(
        self,
        generator: SyntheticDataGenerator,
        flags: list[FeatureFlagRecord],
    ) -> None:
        links = generator.generate_feature_flag_links(flags)
        valid = {"issue", "pr"}
        for link in links:
            assert link.target_type in valid


class TestGenerateTelemetrySignalBuckets:
    def test_returns_nonempty(self, generator: SyntheticDataGenerator) -> None:
        buckets = generator.generate_telemetry_signal_buckets(days=3)
        assert len(buckets) > 0

    def test_bucket_duration_is_one_hour(
        self, generator: SyntheticDataGenerator
    ) -> None:
        buckets = generator.generate_telemetry_signal_buckets(days=1)
        for b in buckets[:20]:
            delta = b.bucket_end - b.bucket_start
            assert delta.total_seconds() == 3600

    def test_signal_types_valid(self, generator: SyntheticDataGenerator) -> None:
        valid = {
            "friction.rage_click",
            "friction.dead_click",
            "error.unhandled",
            "error.api_500",
            "adoption.feature_used",
        }
        buckets = generator.generate_telemetry_signal_buckets(days=2)
        for b in buckets:
            assert b.signal_type in valid

    def test_signal_count_positive(self, generator: SyntheticDataGenerator) -> None:
        buckets = generator.generate_telemetry_signal_buckets(days=2)
        for b in buckets:
            assert b.signal_count >= 1
            assert b.session_count >= 100


class TestGenerateReleaseImpactDaily:
    def test_returns_nonempty(self, generator: SyntheticDataGenerator) -> None:
        records = generator.generate_release_impact_daily(days=7)
        assert len(records) > 0

    def test_coverage_ratio_in_range(self, generator: SyntheticDataGenerator) -> None:
        records = generator.generate_release_impact_daily(days=5)
        for r in records:
            assert r.coverage_ratio is not None
            assert 0.0 <= r.coverage_ratio <= 1.0

    def test_confidence_in_range(self, generator: SyntheticDataGenerator) -> None:
        records = generator.generate_release_impact_daily(days=5)
        for r in records:
            assert r.release_impact_confidence_score is not None
            assert 0.0 <= r.release_impact_confidence_score <= 1.0

    def test_repo_id_set(self, generator: SyntheticDataGenerator) -> None:
        records = generator.generate_release_impact_daily(days=3)
        for r in records:
            assert r.repo_id is not None

    def test_environments_valid(self, generator: SyntheticDataGenerator) -> None:
        records = generator.generate_release_impact_daily(days=5)
        valid = {"production", "staging"}
        for r in records:
            assert r.environment in valid


class TestFeatureFlagDatesStayInsideTheirTtlMargin:
    """CHAOS-3602: feature_flag_event carries `TTL toDateTime(event_ts) +
    INTERVAL 90 DAY DELETE`. A flag backdated to exactly `now - 90 days`
    (the old, unguarded `random.randint(7, 90)` range's own upper bound)
    becomes due for silent TTL deletion the moment `now` advances past mint
    time -- caught live when a mint's content oracle found a restored row
    one short of what `fixtures world` had generated minutes earlier. Every
    generated `created_at` (and therefore every "create" event's
    `event_ts`) must stay a safe margin inside the table's own horizon.
    """

    def test_no_flag_is_created_at_or_past_the_ttl_margin(self) -> None:
        limit = max_safe_backdate_days("feature_flag_event")
        assert limit is not None
        now = datetime.now(timezone.utc)

        oldest_seen_days = 0.0
        # Large sample: the OLD `random.randint(7, 90)` range would almost
        # certainly produce something exceeding `limit` (83) within this
        # many draws -- P(a single draw never exceeds 83) = 76/84, so
        # across 300 independently-seeded draws the chance old code passes
        # this test by luck alone is astronomically small.
        for i in range(300):
            generator = SyntheticDataGenerator(repo_name="acme/demo-app", seed=i)
            flags = generator.generate_feature_flags(count=15, org_id="test-org")
            for flag in flags:
                assert flag.created_at is not None
                age_days = (now - flag.created_at).total_seconds() / 86400
                oldest_seen_days = max(oldest_seen_days, age_days)
                assert age_days <= limit + 0.01, (
                    f"flag {flag.flag_key} created_at is {age_days:.2f} days "
                    f"old, past the safe margin ({limit} days) inside "
                    "feature_flag_event's own TTL horizon"
                )
        # Sanity: the test actually exercised dates near the boundary, not
        # only close-to-zero ones that would trivially pass either way.
        assert oldest_seen_days > limit - 5

    def test_no_feature_flag_event_is_past_the_ttl_margin(self) -> None:
        """The "create" event inherits the flag's own created_at as its
        event_ts -- this is the exact row that disappeared in the real
        incident, so pin the guarantee at the event level too."""
        limit = max_safe_backdate_days("feature_flag_event")
        assert limit is not None
        now = datetime.now(timezone.utc)

        for i in range(50):
            generator = SyntheticDataGenerator(repo_name="acme/demo-app", seed=i)
            flags = generator.generate_feature_flags(count=15, org_id="test-org")
            events = generator.generate_feature_flag_events(flags, org_id="test-org")
            for evt in events:
                age_days = (now - evt.event_ts).total_seconds() / 86400
                assert age_days <= limit + 0.01, (
                    f"event {evt.dedupe_key} event_ts is {age_days:.2f} days "
                    f"old, past the safe margin ({limit} days)"
                )


class TestDaysBasedGeneratorsRespectTheirTtlMargin:
    """telemetry_signal_bucket and release_impact_daily both take a
    caller-supplied `days` window rather than an internal hardcoded range,
    but must still refuse to backdate past their own TTL horizons -- the
    same defense-in-depth CHAOS-3602 fix applied consistently, not just to
    the one table that actually broke."""

    def test_telemetry_signal_buckets_clamp_an_excessive_days_argument(
        self, generator: SyntheticDataGenerator
    ) -> None:
        limit = max_safe_backdate_days("telemetry_signal_bucket")
        assert limit is not None

        buckets = generator.generate_telemetry_signal_buckets(days=1000)
        now = datetime.now(timezone.utc)
        oldest = min(b.bucket_start for b in buckets)
        age_days = (now - oldest).total_seconds() / 86400
        assert age_days <= limit + 0.5, (
            f"oldest telemetry bucket is {age_days:.1f} days old, past the "
            f"safe margin ({limit} days) inside telemetry_signal_bucket's TTL"
        )

    def test_release_impact_daily_clamps_an_excessive_days_argument(
        self, generator: SyntheticDataGenerator
    ) -> None:
        limit = max_safe_backdate_days("release_impact_daily")
        assert limit is not None

        records = generator.generate_release_impact_daily(days=1000)
        now = datetime.now(timezone.utc).date()
        oldest_day = min(r.day for r in records)
        age_days = (now - oldest_day).days
        assert age_days <= limit, (
            f"oldest release_impact_daily row is {age_days} days old, past "
            f"the safe margin ({limit} days) inside its own TTL"
        )
