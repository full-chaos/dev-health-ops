import pytest

from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.metrics.schemas import (
    FeatureFlagEventRecord,
    FeatureFlagLinkRecord,
    FeatureFlagRecord,
    ReleaseImpactDailyRecord,
    TelemetrySignalBucketRecord,
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
            assert 0.0 <= r.coverage_ratio <= 1.0

    def test_confidence_in_range(self, generator: SyntheticDataGenerator) -> None:
        records = generator.generate_release_impact_daily(days=5)
        for r in records:
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
