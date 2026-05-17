"""Work item interactions, feature flags, telemetry, and release impact generators."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from dev_health_ops.fixtures.generators.base import BaseGeneratorMixin
from dev_health_ops.metrics.schemas import (
    FeatureFlagEventRecord,
    FeatureFlagLinkRecord,
    FeatureFlagRecord,
    ReleaseImpactDailyRecord,
    TelemetrySignalBucketRecord,
)
from dev_health_ops.models.work_items import WorkItem, WorkItemInteractionEvent


class InteractionsGeneratorMixin(BaseGeneratorMixin):
    """Generates work item interactions, feature flags & related events, telemetry, and release impact records."""

    _FLAG_KEYS = [
        "new-checkout",
        "dark-mode",
        "payment-v2",
        "onboarding-wizard",
        "search-reindex",
        "beta-dashboard",
        "ai-suggestions",
        "mobile-nav-redesign",
        "rate-limit-bypass",
        "feature-experiment-1",
        "feature-experiment-2",
        "feature-experiment-3",
        "gradual-rollout-auth",
        "ssr-hydration-fix",
        "pricing-tier-toggle",
        "notifications-v3",
        "analytics-pipeline-v2",
        "canary-deploy-gate",
        "maintenance-banner",
        "ab-test-signup-flow",
    ]

    _SIGNAL_TYPES = [
        "friction.rage_click",
        "friction.dead_click",
        "error.unhandled",
        "error.api_500",
        "adoption.feature_used",
    ]

    def generate_work_item_interactions(
        self, work_items: list[WorkItem]
    ) -> list[WorkItemInteractionEvent]:
        """Generate 0-5 comment interaction events per work item."""
        interactions = []
        last_synced = datetime.now(timezone.utc)
        now = datetime.now(timezone.utc)

        for item in work_items:
            num_interactions = random.randint(0, 5)
            if num_interactions == 0:
                continue

            end_time = item.completed_at or now
            if end_time <= item.created_at:
                end_time = item.created_at + timedelta(hours=1)

            duration_seconds = int((end_time - item.created_at).total_seconds())

            for _ in range(num_interactions):
                offset_seconds = (
                    random.randint(0, duration_seconds) if duration_seconds > 0 else 0
                )
                occurred_at = item.created_at + timedelta(seconds=offset_seconds)
                actor_name, actor_email = random.choice(self.repo_authors)

                interactions.append(
                    WorkItemInteractionEvent(
                        work_item_id=item.work_item_id,
                        provider=item.provider,
                        interaction_type="comment",
                        occurred_at=occurred_at,
                        actor=actor_email,
                        body_length=random.randint(20, 500),
                        last_synced=last_synced,
                    )
                )

        return interactions

    def generate_feature_flags(
        self,
        count: int = 15,
        *,
        org_id: str = "",
    ) -> list[FeatureFlagRecord]:
        """Generate synthetic feature flag registry entries."""
        flags: list[FeatureFlagRecord] = []
        now = datetime.now(timezone.utc)
        providers = ["launchdarkly", "launchdarkly", "launchdarkly", "github"]
        flag_types = ["boolean", "boolean", "boolean", "multivariate"]
        environments = ["production", "staging"]

        keys = list(self._FLAG_KEYS)
        random.shuffle(keys)
        keys = keys[:count]

        for i, key in enumerate(keys):
            created_offset_days = random.randint(7, 90)
            created_at = now - timedelta(days=created_offset_days)

            archived_at = None
            if random.random() < 0.20:
                archived_at = created_at + timedelta(
                    days=random.randint(5, created_offset_days)
                )

            flags.append(
                FeatureFlagRecord(
                    provider=random.choice(providers),
                    flag_key=key,
                    project_key=self.repo_name.split("/")[-1],
                    repo_id=self.repo_id,
                    environment=random.choice(environments),
                    flag_type=random.choice(flag_types),
                    created_at=created_at,
                    archived_at=archived_at,
                    last_synced=now,
                    org_id=org_id,
                )
            )

        return flags

    def generate_feature_flag_events(
        self,
        flags: list[FeatureFlagRecord],
        events_per_flag: int = 5,
        *,
        org_id: str = "",
    ) -> list[FeatureFlagEventRecord]:
        """Generate lifecycle events for each flag."""
        events: list[FeatureFlagEventRecord] = []
        now = datetime.now(timezone.utc)
        event_types = ["toggle", "update", "rule", "rollout"]

        for flag in flags:
            flag_created = flag.created_at or (now - timedelta(days=30))

            events.append(
                FeatureFlagEventRecord(
                    event_type="create",
                    flag_key=flag.flag_key,
                    environment=flag.environment,
                    repo_id=flag.repo_id,
                    actor_type="user",
                    prev_state=None,
                    next_state="off",
                    event_ts=flag_created,
                    ingested_at=flag_created + timedelta(seconds=random.randint(1, 60)),
                    source_event_id=None,
                    dedupe_key=f"synthetic:{flag.flag_key}:create:0",
                    org_id=org_id,
                )
            )

            span_seconds = max(1, int((now - flag_created).total_seconds()))
            for i in range(1, events_per_flag):
                evt_type = random.choice(event_types)
                offset = random.randint(1, span_seconds)
                event_ts = flag_created + timedelta(seconds=offset)

                prev_state = random.choice(["off", "on", "10%", "50%"])
                if evt_type == "toggle":
                    next_state = "on" if prev_state == "off" else "off"
                elif evt_type == "rollout":
                    next_state = random.choice(["10%", "25%", "50%", "100%"])
                else:
                    next_state = random.choice(["on", "off", "25%", "75%"])

                events.append(
                    FeatureFlagEventRecord(
                        event_type=evt_type,
                        flag_key=flag.flag_key,
                        environment=flag.environment,
                        repo_id=flag.repo_id,
                        actor_type=random.choice(["user", "automation"]),
                        prev_state=prev_state,
                        next_state=next_state,
                        event_ts=event_ts,
                        ingested_at=event_ts
                        + timedelta(seconds=random.randint(1, 120)),
                        source_event_id=None,
                        dedupe_key=f"synthetic:{flag.flag_key}:{evt_type}:{i}",
                        org_id=org_id,
                    )
                )

        events.sort(key=lambda e: e.event_ts)
        return events

    def generate_feature_flag_links(
        self,
        flags: list[FeatureFlagRecord],
        *,
        org_id: str = "",
        issue_ids: list[str] | None = None,
        pr_numbers: list[int] | None = None,
        release_refs: list[str] | None = None,
    ) -> list[FeatureFlagLinkRecord]:
        """Generate flag-to-work-item links."""
        links: list[FeatureFlagLinkRecord] = []
        now = datetime.now(timezone.utc)

        targets: list[tuple[str, str]] = []
        if issue_ids:
            for iid in issue_ids:
                targets.append(("issue", iid))
        if pr_numbers:
            for prn in pr_numbers:
                targets.append(("pr", f"{self.repo_id}#pr{prn}"))
        if release_refs:
            for release_ref in release_refs:
                targets.append(("release", release_ref))

        if not targets:
            for i in range(min(len(flags), 10)):
                targets.append(("issue", f"{self.repo_name}-{100 + i}"))
            for i in range(min(len(flags), 5)):
                targets.append(("pr", f"{self.repo_id}#pr{i + 1}"))
            for release_ref in self._default_release_refs(max(len(flags), 7)):
                targets.append(("release", release_ref))

        confidence_profiles = [
            (1.0, "native", "api_link"),
            (0.8, "explicit_text", "commit_message"),
            (0.3, "heuristic", "name_match"),
        ]

        for flag in flags:
            num_links = random.randint(0, min(3, len(targets)))
            if num_links == 0:
                continue

            chosen_targets = random.sample(targets, num_links)
            for target_type, target_id in chosen_targets:
                confidence, link_source, evidence_type = random.choice(
                    confidence_profiles
                )
                flag_created = flag.created_at or (now - timedelta(days=30))

                links.append(
                    FeatureFlagLinkRecord(
                        flag_key=flag.flag_key,
                        target_type=target_type,
                        target_id=target_id,
                        provider=flag.provider,
                        link_source=link_source,
                        link_type=(
                            "controls"
                            if target_type == "pr"
                            else "rollout"
                            if target_type == "release"
                            else "tracks"
                        ),
                        evidence_type=evidence_type,
                        confidence=confidence,
                        valid_from=flag_created,
                        valid_to=flag.archived_at,
                        last_synced=now,
                        org_id=org_id,
                    )
                )

        return links

    def generate_telemetry_signal_buckets(
        self,
        days: int = 30,
        *,
        org_id: str = "",
        release_refs: list[str] | None = None,
    ) -> list[TelemetrySignalBucketRecord]:
        """Generate hourly telemetry signal buckets."""
        buckets: list[TelemetrySignalBucketRecord] = []
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=days)

        if not release_refs:
            release_refs = self._default_release_refs(days)

        environments = ["production", "staging"]
        endpoint_groups = ["/api/checkout", "/api/auth", "/api/search", None]

        current = start.replace(minute=0, second=0, microsecond=0)
        bucket_idx = 0
        while current < now:
            bucket_end = current + timedelta(hours=1)

            active_signals = random.sample(
                self._SIGNAL_TYPES,
                k=random.randint(1, min(3, len(self._SIGNAL_TYPES))),
            )

            for signal_type in active_signals:
                session_count = random.randint(100, 5000)

                if "error" in signal_type:
                    signal_count = int(session_count * random.uniform(0.001, 0.05))
                elif "friction" in signal_type:
                    signal_count = int(session_count * random.uniform(0.005, 0.08))
                else:
                    signal_count = int(session_count * random.uniform(0.1, 0.6))

                signal_count = max(1, signal_count)

                buckets.append(
                    TelemetrySignalBucketRecord(
                        signal_type=signal_type,
                        signal_count=signal_count,
                        session_count=session_count,
                        unique_pseudonymous_count=int(session_count * 0.7),
                        endpoint_group=random.choice(endpoint_groups),
                        environment=random.choice(environments),
                        repo_id=self.repo_id,
                        release_ref=random.choice(release_refs),
                        bucket_start=current,
                        bucket_end=bucket_end,
                        ingested_at=bucket_end
                        + timedelta(seconds=random.randint(5, 300)),
                        is_sampled=random.random() < 0.1,
                        schema_version="1",
                        dedupe_key=f"synthetic:telemetry:{bucket_idx}:{signal_type}",
                        org_id=org_id,
                    )
                )
                bucket_idx += 1

            current = bucket_end

        return buckets

    def generate_release_impact_daily(
        self,
        days: int = 30,
        *,
        org_id: str = "",
        release_refs: list[str] | None = None,
    ) -> list[ReleaseImpactDailyRecord]:
        """Generate daily release impact metrics."""
        records: list[ReleaseImpactDailyRecord] = []
        now = datetime.now(timezone.utc)
        end_date = now.date()
        computed_at = now

        if not release_refs:
            release_refs = self._default_release_refs(days)

        environments = ["production", "staging"]

        for i in range(days):
            day = end_date - timedelta(days=i)
            for release_ref in release_refs:
                for env in environments:
                    friction_delta = random.uniform(-0.05, 0.20)
                    error_delta = random.uniform(-0.05, 0.20)
                    coverage = random.uniform(0.4, 0.95)
                    confidence = random.uniform(0.3, 1.0)

                    records.append(
                        ReleaseImpactDailyRecord(
                            day=day,
                            release_ref=release_ref,
                            environment=env,
                            repo_id=self.repo_id,
                            release_user_friction_delta=friction_delta,
                            release_post_friction_rate=random.uniform(0.01, 0.15),
                            release_error_rate_delta=error_delta,
                            release_post_error_rate=random.uniform(0.001, 0.05),
                            time_to_first_user_issue_after_release=random.uniform(
                                0.5, 48.0
                            ),
                            release_impact_confidence_score=confidence,
                            release_impact_coverage_ratio=coverage,
                            flag_exposure_rate=random.uniform(0.1, 0.9),
                            flag_activation_rate=random.uniform(0.05, 0.8),
                            flag_reliability_guardrail=random.uniform(0.8, 1.0),
                            flag_friction_delta=random.uniform(-0.03, 0.10),
                            flag_rollout_half_life=random.uniform(1.0, 72.0),
                            flag_churn_rate=random.uniform(0.0, 0.3),
                            issue_to_release_impact_link_rate=random.uniform(0.2, 0.9),
                            rollback_or_disable_after_impact_spike=1
                            if random.random() < 0.1
                            else 0,
                            coverage_ratio=coverage,
                            missing_required_fields_count=random.randint(0, 2),
                            instrumentation_change_flag=random.random() < 0.05,
                            data_completeness=random.uniform(0.7, 1.0),
                            concurrent_deploy_count=random.randint(0, 3),
                            computed_at=computed_at,
                            org_id=org_id,
                        )
                    )

        return records
