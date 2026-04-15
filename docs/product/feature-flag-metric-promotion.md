# Feature Flag Metric Promotion Gates

_Decision date: 2026-04-15_
_Tracking: CHAOS-831 (Phase 4: Promotion Gates)_
_Rejection log: CHAOS-832_

## Purpose

This document defines the acceptance criteria used to evaluate each candidate metric from the [Feature Flag + User Impact PRD](feature-flag-user-impact-prd.md) (lines 229-253). Metrics that pass all gates are promoted to the **stable** registry. Metrics that pass most gates ship as **provisional** (beta). Metrics that fail critical gates are **rejected** with documented rationale.

## Promotion Gates

### Gate 1: Implementability

**Question**: Is the metric fully computed by `release_impact.py` or a companion module?

| Verdict | Criteria |
|---------|----------|
| PASS | Metric is computed end-to-end and written to `ReleaseImpactDailyRecord` via ClickHouse sink |
| PARTIAL | Schema field exists in `ReleaseImpactDailyRecord` but computation returns `None` (stubbed) |
| FAIL | No schema field, no computation path |

**Evidence source**: `src/dev_health_ops/metrics/release_impact.py`, `src/dev_health_ops/metrics/schemas.py`

### Gate 2: Confidence Floor

**Question**: Can this metric achieve `coverage >= 0.70` (the "show" display gate) in typical deployments?

| Verdict | Criteria |
|---------|----------|
| PASS | Min sample is achievable for teams with >= 50 daily sessions; coverage threshold is realistic |
| WARN | Min sample requires high-traffic environments (>= 1000 events); many teams will see "warn" or "suppress" |
| FAIL | Min sample is unrealistic for most deployments; metric will almost always be suppressed |

**Reference**: `confidence.py` display gates — show >= 0.70, warn >= 0.50, suppress < 0.50

### Gate 3: Misuse Risk

**Question**: Could this metric be used punitively against individuals?

| Verdict | Criteria |
|---------|----------|
| PASS | Metric is scoped to release/environment/team; no individual attribution possible |
| WARN | Metric could be narrowed to individual via filtering (e.g., single-person team) |
| FAIL | Metric inherently identifies or ranks individuals |

**Reference**: Platform mission — "accessibility over extraction, learning not judgment"

### Gate 4: Signal Quality

**Question**: Does the metric have a clear, testable denominator? Is `min_sample` achievable?

| Verdict | Criteria |
|---------|----------|
| PASS | Denominator is well-defined (session_count, eligible_sessions, etc.), min_sample is documented and testable |
| WARN | Denominator depends on external data quality (e.g., work graph linkage) |
| FAIL | No clear denominator or min_sample is undefined |

### Gate 5: Interpretation Clarity

**Question**: Can a non-technical user understand what this metric means from the user guide?

| Verdict | Criteria |
|---------|----------|
| PASS | Metric has plain-language description in user guide with approved hedging language |
| WARN | Metric is documented but requires statistical literacy to interpret |
| FAIL | No user-facing documentation; metric is internal/diagnostic only |

## Verdict Matrix

Each metric receives a per-gate verdict. The overall promotion decision follows:

| Overall | Rule |
|---------|------|
| **PROMOTE** (stable) | All gates PASS, or all PASS with at most one WARN on non-critical gate |
| **PROVISIONAL** (beta) | Implementability is PARTIAL, or 2+ WARN verdicts, but no FAIL on Gate 3 (misuse) |
| **REJECT** | Any FAIL on Gate 1 (not implemented at all), Gate 3 (misuse risk), or 3+ FAIL verdicts |

## Evaluation Results

### Release Metrics

#### `release_user_friction_delta` — PROMOTE

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PASS | Fully computed via `_compute_delta()` with `friction.%` signal pattern. Written to `release_user_friction_delta` field. |
| Confidence floor | PASS | Min sample 300 sessions is achievable for teams with moderate traffic. Display gate system handles low-coverage gracefully. |
| Misuse risk | PASS | Scoped to release × environment. No individual attribution. |
| Signal quality | PASS | Denominator is `session_count` from `telemetry_signal_bucket`. Min sample (300) is documented and enforced in code (`_MIN_SESSIONS_FRICTION`). |
| Interpretation clarity | PASS | User guide provides plain-language description with approved hedging ("appears elevated"). |

#### `release_error_rate_delta` — PROMOTE

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PASS | Fully computed via `_compute_delta()` with `error.%` signal pattern. Written to `release_error_rate_delta` field. |
| Confidence floor | WARN | Min sample 1000 events is high; smaller teams may frequently see "suppress" gate. However, the metric degrades gracefully (returns `None` when insufficient). |
| Misuse risk | PASS | Scoped to release × environment. |
| Signal quality | PASS | Denominator is `session_count`. Min sample (1000) enforced via `_MIN_EVENTS_ERROR`. |
| Interpretation clarity | PASS | User guide documents with hedging language ("suggests a potential regression"). |

#### `time_to_first_user_issue_after_release` — PROVISIONAL

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PARTIAL | `_time_to_first_friction_spike()` computes time-to-first-friction-signal (telemetry-based proxy), but the PRD specifies time-to-first-*user-reported-issue* via work graph linkage. Current implementation is a telemetry approximation, not the full work-graph-linked version. |
| Confidence floor | WARN | Requires at least 1 linked issue — many releases will have zero linked issues, producing `None`. |
| Misuse risk | PASS | Scoped to release × environment. |
| Signal quality | WARN | Denominator is implicit (1 linked issue). The "linked issue" definition depends on work graph edge quality which varies by provider. |
| Interpretation clarity | PASS | User guide explains clearly ("hours between deployment and first linked user reported issue"). |

**Condition for promotion**: Implement full work-graph-linked version (issue `created_at` - `deployment.completed_at`) instead of telemetry proxy. Track via CHAOS-832.

#### `release_impact_confidence_score` — PROMOTE

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PASS | Fully computed via `_compute_confidence()` with weighted factors (coverage 0.35, sample 0.35, confound 0.30). Also has dedicated `compute_impact_confidence()` in `confidence.py`. |
| Confidence floor | PASS | Meta-metric — always computable when any release data exists. |
| Misuse risk | PASS | Diagnostic metric, not a performance indicator. |
| Signal quality | PASS | Factors are well-defined: coverage_ratio, sample_size, concurrent_deploy_count. |
| Interpretation clarity | PASS | User guide explains visibility gates (show/warn/suppress). Labeled as "not a business KPI" in PRD. |

#### `release_impact_coverage_ratio` — PROMOTE

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PASS | Computed as `releases_with_telemetry / total_releases_on_day` in `_compute_day()`. Written to `coverage_ratio` field. |
| Confidence floor | PASS | Always computable when deployment data exists. |
| Misuse risk | PASS | Data quality indicator, no individual attribution. |
| Signal quality | PASS | Clear denominator (total releases on day). |
| Interpretation clarity | PASS | Displayed alongside every impact metric per PRD. User guide documents the visibility gate table. |

### Flag Metrics

#### `flag_exposure_rate` — PROVISIONAL

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PARTIAL | Schema field exists in `ReleaseImpactDailyRecord` (`flag_exposure_rate`) but computation returns `None`. Requires flag evaluation event ingestion (session-level flag exposure tracking). |
| Confidence floor | PASS | Min sample 200 eligible sessions is reasonable for flagged features. |
| Misuse risk | PASS | Scoped to flag × environment. Session-based denominator avoids user identification. |
| Signal quality | PASS | Denominator (`eligible_sessions`) is well-defined. MVP uses session-based counting. |
| Interpretation clarity | PASS | User guide explains ("verifies rollout rules are reaching intended audience"). |

**Condition for promotion**: Implement computation from `feature_flag_events` + `telemetry_signal_bucket` join. Track via CHAOS-832.

#### `flag_activation_rate` — PROVISIONAL

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PARTIAL | Schema field exists (`flag_activation_rate`) but returns `None`. Requires per-flag success action contract definition. |
| Confidence floor | PASS | Min sample 100 exposed sessions is achievable. |
| Misuse risk | PASS | Session-scoped, no individual attribution. |
| Signal quality | WARN | "Activated" depends on per-flag success action definition — requires configuration contract that doesn't exist yet. |
| Interpretation clarity | PASS | User guide explains ("measures effectiveness in driving desired user behavior"). |

**Condition for promotion**: Define success action contract in `feature_flag_link` config. Implement computation. Track via CHAOS-832.

#### `flag_reliability_guardrail` — PROVISIONAL

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PARTIAL | Schema field exists (`flag_reliability_guardrail`) but returns `None`. Computation requires session-level flag exposure join with error signals. |
| Confidence floor | PASS | Min sample 300 sessions is reasonable. |
| Misuse risk | PASS | Guardrail metric, scoped to flag cohort. |
| Signal quality | PASS | Clear denominator (total_sessions for exposed cohort). Numerator well-defined (zero error/crash signals). |
| Interpretation clarity | PASS | User guide explains ("safety check ensures flag isn't introducing silent failures"). |

**Condition for promotion**: Implement exposed-cohort error-free session computation. Track via CHAOS-832.

#### `flag_friction_delta` — PROVISIONAL

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PARTIAL | Schema field exists (`flag_friction_delta`) but returns `None`. Same formula as release friction delta but scoped to flag exposure window. |
| Confidence floor | PASS | Min sample 200 sessions is achievable. |
| Misuse risk | PASS | Scoped to flag × environment. |
| Signal quality | WARN | Requires session-level flag evaluation signal and contamination exclusion (multiple concurrent flags). Contamination logic exists in `confidence.py` but isn't wired to computation. |
| Interpretation clarity | PASS | Follows same interpretation pattern as release friction delta. |

**Condition for promotion**: Wire contamination exclusion from `compute_cohort_contamination()`. Implement flag-scoped delta computation. Track via CHAOS-832.

#### `flag_rollout_half_life` — REJECT

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PARTIAL | Schema field exists (`flag_rollout_half_life`) but returns `None`. |
| Confidence floor | WARN | Requires 2 rollout events minimum — many flags use binary toggles with no incremental rollout, producing no data. |
| Misuse risk | PASS | Operational metric, no individual attribution. |
| Signal quality | FAIL | Denominator is provider-specific (LaunchDarkly percentage events vs GitLab incremental steps). No unified abstraction exists. Cross-provider comparison is meaningless. |
| Interpretation clarity | WARN | Requires understanding of provider-specific rollout mechanics. Non-technical users cannot interpret without context. |

**Rejection rationale**: Provider-specific denominator makes cross-provider comparison invalid. Signal quality gate fails — no unified rollout progression abstraction exists. Revisit if a provider-agnostic rollout stage model is designed. Logged in CHAOS-832.

#### `flag_churn_rate` — REJECT

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PARTIAL | Schema field exists (`flag_churn_rate`) but returns `None`. |
| Confidence floor | PASS | No min sample — count-based metric. |
| Misuse risk | WARN | "Churn rate" framing could be used to pressure teams into fewer flag changes, discouraging safe incremental rollouts. Conflicts with platform mission of "learning, not judgment." |
| Signal quality | WARN | Denominator (weeks_in_window) is clear, but "toggle" and "rule_change" event types vary significantly across providers. A rule change adding a user segment is not equivalent to a panic toggle-off. |
| Interpretation clarity | FAIL | "Volatility indicator" is ambiguous — high churn could mean healthy iteration or panic. Without context, this metric invites misinterpretation. No user guide entry exists. |

**Rejection rationale**: Misuse risk (pressuring against flag changes) combined with interpretation ambiguity. The metric conflates healthy iteration with instability. Logged in CHAOS-832.

### Data Quality / Linkage Metrics

#### `issue_to_release_impact_link_rate` — PROVISIONAL

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PARTIAL | Schema field exists (`issue_to_release_impact_link_rate`) but returns `None`. Requires work graph edge traversal (issue → PR → deployment → release). |
| Confidence floor | WARN | Min sample 50 work items over 30 days — achievable for active teams but many smaller teams won't meet threshold. |
| Misuse risk | PASS | Data quality signal, no individual attribution. |
| Signal quality | PASS | Denominator (completed work items) is well-defined via existing work-item completion semantics. |
| Interpretation clarity | PASS | Clear meaning: "what fraction of completed work has measurable post-release signal." |

**Condition for promotion**: Implement work graph traversal computation. Track via CHAOS-832.

#### `rollback_or_disable_after_impact_spike` — PROVISIONAL

| Gate | Verdict | Notes |
|------|---------|-------|
| Implementability | PARTIAL | Schema field exists (`rollback_or_disable_after_impact_spike`) but returns `None`. Requires joining flag events (toggle_off, rollback, disable) with deployment timestamps. |
| Confidence floor | PASS | Count-based, no min sample. |
| Misuse risk | PASS | Stability response marker, no individual attribution. |
| Signal quality | WARN | "Impact spike" is not formally defined — the PRD says "no dependency on undefined alert mechanism." Current definition uses temporal correlation only (within 72h of deploy). |
| Interpretation clarity | PASS | Clear meaning: count of defensive actions after deployment. |

**Condition for promotion**: Implement flag event join with deployment window. Clarify "impact spike" trigger definition. Track via CHAOS-832.

## Summary

| Metric | Verdict | Gate Results |
|--------|---------|-------------|
| `release_user_friction_delta` | **PROMOTE** | 5/5 PASS |
| `release_error_rate_delta` | **PROMOTE** | 4 PASS, 1 WARN |
| `release_impact_confidence_score` | **PROMOTE** | 5/5 PASS |
| `release_impact_coverage_ratio` | **PROMOTE** | 5/5 PASS |
| `time_to_first_user_issue_after_release` | **PROVISIONAL** | 1 PARTIAL, 2 WARN, 2 PASS |
| `flag_exposure_rate` | **PROVISIONAL** | 1 PARTIAL, 4 PASS |
| `flag_activation_rate` | **PROVISIONAL** | 1 PARTIAL, 1 WARN, 3 PASS |
| `flag_reliability_guardrail` | **PROVISIONAL** | 1 PARTIAL, 4 PASS |
| `flag_friction_delta` | **PROVISIONAL** | 1 PARTIAL, 1 WARN, 3 PASS |
| `flag_rollout_half_life` | **REJECT** | 1 PARTIAL, 1 FAIL, 2 WARN, 1 PASS |
| `flag_churn_rate` | **REJECT** | 1 PARTIAL, 1 FAIL, 2 WARN, 1 PASS |
| `issue_to_release_impact_link_rate` | **PROVISIONAL** | 1 PARTIAL, 1 WARN, 3 PASS |
| `rollback_or_disable_after_impact_spike` | **PROVISIONAL** | 1 PARTIAL, 1 WARN, 3 PASS |

**Totals**: 4 PROMOTE, 7 PROVISIONAL, 2 REJECT
