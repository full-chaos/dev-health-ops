# Feature Flag + User Impact: Stable Metric Registry

_Effective: 2026-04-15_
_Source: [Promotion Gates](feature-flag-metric-promotion.md) (CHAOS-831)_
_Rejection log: CHAOS-832_

## Overview

This registry lists all metrics that have passed promotion gates and are approved for production display. Metrics are classified as **stable** (fully validated) or **provisional** (shipping with beta label, pending further validation).

Rejected metrics are documented in the [promotion gates](feature-flag-metric-promotion.md) document and tracked in CHAOS-832.

---

## Stable Metrics

These metrics are fully implemented, have clear denominators, achievable confidence floors, and no misuse risk. They are approved for unconditional display (subject to standard visibility gates).

### `release_user_friction_delta`

| Property | Value |
|----------|-------|
| **Label** | Release Friction Delta |
| **Unit** | ratio (relative change) |
| **Formula** | `(mean(friction/session, post) - mean(friction/session, baseline)) / mean(friction/session, baseline)` |
| **Baseline window** | 7 days pre-deploy |
| **Post window** | 24-72 hours post-deploy |
| **Min sample** | 300 sessions in both windows |
| **Denominator** | `session_count` from `telemetry_signal_bucket` |
| **Display gate** | Standard (show >= 0.70 coverage, warn >= 0.50, suppress < 0.50) |
| **Companion** | `release_post_friction_rate` (absolute rate) |

**Caveats**: Observational only — does not prove causation. Concurrent deploys dilute attribution (see `concurrent_deploy_count`). Hedging language required in all UX copy.

### `release_error_rate_delta`

| Property | Value |
|----------|-------|
| **Label** | Release Error Rate Delta |
| **Unit** | ratio (relative change) |
| **Formula** | `(mean(error/session, post) - mean(error/session, baseline)) / mean(error/session, baseline)` |
| **Baseline window** | 7 days pre-deploy |
| **Post window** | 24-72 hours post-deploy |
| **Min sample** | 1000 events across both windows |
| **Denominator** | `session_count` from `telemetry_signal_bucket` |
| **Display gate** | Standard |
| **Companion** | `release_post_error_rate` (absolute rate) |

**Caveats**: Higher min sample means smaller teams will frequently see "warn" or "suppress" gates. This is by design — low-sample error rates are unreliable. Same observational/hedging caveats as friction delta.

### `release_impact_confidence_score`

| Property | Value |
|----------|-------|
| **Label** | Impact Confidence Score |
| **Unit** | 0.0 to 1.0 |
| **Formula** | `0.35 * coverage_ratio + 0.35 * sample_sufficiency + 0.30 * confounder_penalty` |
| **Scope** | Per release × environment |
| **Display** | Accompanies all impact metrics; drives visibility gates |

**Caveats**: Meta-metric, not a business KPI. Weights (0.35/0.35/0.30) were calibrated during prototype phase. Do not use for cross-team comparison.

### `release_impact_coverage_ratio`

| Property | Value |
|----------|-------|
| **Label** | Impact Coverage Ratio |
| **Unit** | 0.0 to 1.0 |
| **Formula** | `releases_with_telemetry / total_releases_on_day` |
| **Scope** | Per day × org |
| **Display** | Shown alongside every impact metric |

**Caveats**: Data quality indicator. Low coverage means telemetry instrumentation is incomplete, not that releases are problematic.

---

## Provisional Metrics (Beta)

These metrics have schema support and clear definitions but are not yet fully computed. They ship with a **beta** label in the UI and are subject to the conditions listed below for promotion to stable.

### `time_to_first_user_issue_after_release`

| Property | Value |
|----------|-------|
| **Label** | Time to First User Issue |
| **Unit** | hours |
| **Formula** | `min(issue.created_at) - deployment.completed_at` (work-graph-linked issues) |
| **Min sample** | 1 linked issue |
| **Current state** | Telemetry proxy implemented (friction spike timing). Full work-graph version pending. |
| **Promotion condition** | Implement work-graph-linked issue lookup. Validate against 3+ repos with known incident timelines. |

### `flag_exposure_rate`

| Property | Value |
|----------|-------|
| **Label** | Flag Exposure Rate |
| **Unit** | ratio |
| **Formula** | `exposed_sessions / eligible_sessions` |
| **Min sample** | 200 eligible sessions |
| **Current state** | Schema field exists. Computation stubbed (`None`). |
| **Promotion condition** | Implement computation from flag evaluation events joined with session telemetry. |

### `flag_activation_rate`

| Property | Value |
|----------|-------|
| **Label** | Flag Activation Rate |
| **Unit** | ratio |
| **Formula** | `activated_sessions / exposed_sessions` |
| **Min sample** | 100 exposed sessions |
| **Current state** | Schema field exists. Computation stubbed (`None`). |
| **Promotion condition** | Define per-flag success action contract in `feature_flag_link` config. Implement computation. |

### `flag_reliability_guardrail`

| Property | Value |
|----------|-------|
| **Label** | Flag Reliability Guardrail |
| **Unit** | ratio |
| **Formula** | `error_free_sessions / total_sessions` (exposed cohort) |
| **Min sample** | 300 sessions |
| **Current state** | Schema field exists. Computation stubbed (`None`). |
| **Promotion condition** | Implement exposed-cohort error-free session computation from flag evaluation + telemetry join. |

### `flag_friction_delta`

| Property | Value |
|----------|-------|
| **Label** | Flag Friction Delta |
| **Unit** | ratio |
| **Formula** | Same as `release_user_friction_delta` but scoped to flag exposure window |
| **Min sample** | 200 sessions |
| **Current state** | Schema field exists. Computation stubbed (`None`). Contamination logic exists in `confidence.py` but not wired. |
| **Promotion condition** | Wire `compute_cohort_contamination()` to exclude multi-flag sessions. Implement flag-scoped delta. |

### `issue_to_release_impact_link_rate`

| Property | Value |
|----------|-------|
| **Label** | Issue-to-Release Link Rate |
| **Unit** | 0.0 to 1.0 |
| **Formula** | `completed_items_with_coverage / completed_items` (rolling 30d) |
| **Min sample** | 50 work items |
| **Current state** | Schema field exists. Computation stubbed (`None`). |
| **Promotion condition** | Implement work graph edge traversal (issue → PR → deployment → release). |

### `rollback_or_disable_after_impact_spike`

| Property | Value |
|----------|-------|
| **Label** | Rollback/Disable After Impact |
| **Unit** | count |
| **Formula** | Count of flag disable/rollback events within 72h of deploy |
| **Current state** | Schema field exists. Computation stubbed (`None`). |
| **Promotion condition** | Implement flag event join with deployment window. Clarify "impact spike" trigger definition. |

---

## Rejected Metrics

The following metrics failed critical promotion gates and are **not shipped**. See [promotion gates](feature-flag-metric-promotion.md) for full rationale.

| Metric | Rejection Reason |
|--------|-----------------|
| `flag_rollout_half_life` | Provider-specific denominator (LaunchDarkly vs GitLab rollout mechanics) makes cross-provider comparison invalid. No unified rollout stage abstraction exists. |
| `flag_churn_rate` | Misuse risk — "volatility" framing pressures against healthy flag iteration. Interpretation ambiguity — high churn conflates healthy iteration with instability. No user guide entry. |

Rejected metrics are tracked in CHAOS-832 for potential future reconsideration if underlying issues are resolved.

---

## Display Contract

All metrics in this registry follow the standard visibility gate system:

| Coverage | Gate | UX Behavior |
|----------|------|-------------|
| >= 0.70 | `show` | Metric displayed normally |
| 0.50 - 0.69 | `warn` | Metric shown with warning icon and data quality note |
| < 0.50 | `suppress` | Metric hidden; "insufficient data" placeholder shown |

Provisional metrics additionally display a **beta** badge and tooltip explaining the metric is under validation.

## Language Policy

All metric labels and descriptions in the UI must use approved hedging language:

- **Allowed**: appears, suggests, leans, is consistent with
- **Forbidden**: caused, proved, determined, is/was (when stating impact), detected
