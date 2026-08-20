# Rejected and Deferred Metrics: Feature Flag & User Impact

This document outlines the metrics evaluated during the Feature Flag + User Impact initiative that were either rejected for production use or deferred to future phases.

## Rejected Metrics (Do Not Ship)

The following metrics were evaluated but failed to meet the platform's standards for signal quality, privacy, or safety.

### 1. Release Impact by Author
- **Definition**: Aggregating `release_user_friction_delta` and `release_error_rate_delta` by the primary author of the deployment or associated commits.
- **Rejection Reason**: High misuse risk (person-to-person ranking).
- **Misuse Scenario**: Engineering managers using "Impact Scores" in performance reviews, leading to developers avoiding high-risk but necessary changes or "gaming" the metric by splitting risky changes into smaller, unattributed chunks.
- **Mitigation if Reconsidered**: Aggregate at the Team or Repository level only. Ensure individual attribution is never exposed in the UI or API.

### 2. Unattributed Telemetry Spike Rate
- **Definition**: The frequency of significant telemetry shifts (friction/errors) that cannot be linked to any release or feature flag within the attribution window.
- **Rejection Reason**: Low signal quality / Attribution too weak.
- **Misuse Scenario**: Teams spending hours "debugging the platform" to find a missing link for a spike that was actually caused by an external factor (e.g., a third-party API outage) that the system isn't instrumented to track.
- **Mitigation if Reconsidered**: Only display when `release_impact_coverage_ratio` is > 90% and concurrent deploy count is zero, providing a "clean room" for attribution.

### 3. Low-Traffic Flag Activation
- **Definition**: `flag_activation_rate` for flags with fewer than 100 exposed sessions.
- **Rejection Reason**: Insufficient denominator.
- **Misuse Scenario**: A product manager sees a 50% activation rate on a new feature and decides to kill the old version, not realizing the 50% represents only 2 out of 4 users.
- **Mitigation if Reconsidered**: Strict enforcement of `DisplayGate` logic. Suppress any metric where the denominator is below the `min_sample` threshold defined in the PRD.

!!! warning "Privacy Concern"
    Any metric requiring `unique_user_id` linkage was rejected for Phase 1 to maintain the platform's "pseudonymous by default" stance. User-based metrics are deferred until the User Identity service is fully audited.

## Deferred Metrics (Future Phase)

These metrics are conceptually sound but require infrastructure or data sources not available in the current phase.

### 1. User-Based Exposure Rate
- **What's missing**: `unique_pseudonymous_count` in telemetry buckets. Current metrics rely on session-based denominators which can overcount repeat users.
- **Estimated effort**: Medium. Requires telemetry schema updates and processor logic to handle cardinality estimation (e.g., HyperLogLog).
- **Recommended phase**: Phase 5 (User Identity & Persistence).

### 2. Flag Activation Rate (Full Implementation)
- **What's missing**: A standardized "success action" contract. Currently, the system doesn't know which telemetry signal constitutes "success" for a specific flag without manual configuration.
- **Estimated effort**: High. Requires a UI for mapping telemetry events to specific feature flags and a more flexible work graph edge type.
- **Recommended phase**: Phase 6 (Product Analytics Integration).

### 3. Flag Rollout Half-Life
- **What's missing**: Ingestion of provider-specific rollout events (e.g., LaunchDarkly percentage changes or GitLab incremental rollout steps).
- **Estimated effort**: Medium. Requires connector updates for each supported provider to fetch audit logs/events.
- **Recommended phase**: Phase 5 (Provider Deep Integration).

## Rollout Recommendations

### 1. GA Rollout Plan
1.  **Internal Dogfooding**: Enable for the `dev-health` development team to monitor our own releases.
2.  **Private Beta**: Enable for 3-5 high-traffic repositories with mature telemetry instrumentation.
3.  **General Availability**: Enable for all customers, starting with `release_impact` metrics, followed by `flag_impact` as connectors are configured.

### 2. Feature Flag for the Feature Flags
Use the existing billing and tiering system to gate these analytics.
- **Flag**: `billing.release_analytics`
- **Behavior**: Gates the "Release Impact" and "Flag Insights" tabs in the web UI.

### 3. Data Retention
To balance analytical depth with storage costs:
- **Raw Telemetry**: 90 days (allows for recomputation of recent deltas).
- **Derived Impact Records**: 365 days (allows for year-over-year trend analysis).

### 4. Monitoring during Rollout
- **Coverage Ratios**: Monitor `release_impact_coverage_ratio` across the fleet. If it stays below 50%, investigate instrumentation gaps.
- **Confidence Distributions**: Watch for a high percentage of "Heuristic" provenance. This indicates that teams aren't using native linking (tags/PR refs).
- **User Feedback**: Specifically monitor for "Language Policy" friction—do users find the "appears/suggests" terminology helpful or frustrating?

### 5. Escape Hatches
- **Global Toggle**: `RELEASE_IMPACT_ENABLED` environment variable to kill all impact processing if ClickHouse load spikes.
- **Per-Repo Exclusion**: A `disabled_metrics` list in the repository configuration to suppress specific signals that are known to be noisy.
