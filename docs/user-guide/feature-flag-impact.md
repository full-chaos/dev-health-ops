# Release and Feature Flag Impact

This guide explains how to interpret metrics that link engineering releases and feature flag rollouts to user experience signals.

## Overview

Release impact metrics measure shifts in user friction, errors, and adoption following a deployment or flag change. These signals help teams understand the immediate effect of their work on the system and its users.

### Limitations

These metrics are observational. They show correlations between releases and telemetry shifts within specific time windows. Our system doesn't provide causal proof. External factors like seasonality, marketing campaigns, or concurrent infrastructure changes can influence these numbers.

## Metric Catalog

### Release Metrics

#### User Friction Delta
- **Measurement**: The percentage change in friction signals per session compared to a 7 day baseline.
- **Interpretation**: A positive delta suggests increased user struggle. A negative delta suggests a smoother experience.
- **Confidence**: Requires at least 300 sessions in both baseline and post windows.
- **Example**: "The friction rate appears elevated after this release."

#### Error Rate Delta
- **Measurement**: The change in error signals per session relative to the baseline.
- **Interpretation**: Significant increases often point to regressions or unhandled edge cases in the new code.
- **Confidence**: Requires 1000 events across both windows for statistical relevance.
- **Example**: "The error rate suggests a potential regression in the checkout flow."

#### Time to First User Issue
- **Measurement**: The hours between deployment and the first linked user reported issue.
- **Interpretation**: Shorter times indicate immediate visibility of defects.
- **Confidence**: Only counts issues explicitly linked to the release in the work graph.
- **Example**: "The first user issue appeared within two hours of deployment."

### Feature Flag Metrics

#### Exposure Rate
- **Measurement**: The ratio of sessions that encountered the flag versus all eligible sessions.
- **Interpretation**: This verifies that the rollout rules are reaching the intended audience.
- **Example**: "Flag exposure appears consistent with the 10% rollout target."

#### Activation Rate
- **Measurement**: The ratio of exposed sessions that performed a defined success action.
- **Interpretation**: It measures the effectiveness of the feature in driving desired user behavior.
- **Example**: "The activation rate leans positive for the new search interface."

#### Reliability Guardrail
- **Measurement**: The ratio of error free sessions in the exposed cohort.
- **Interpretation**: This safety check ensures the flag isn't introducing silent failures.
- **Example**: "The reliability guardrail suggests the rollout is stable."

## Confidence and Data Quality

We use coverage and sample size to determine how much weight to give a metric.

### Visibility Gates

| Coverage | Status | Action |
|----------|--------|--------|
| >= 70% | Show | Metric is displayed normally. |
| 50% to 69% | Warn | Metric is shown with a warning icon. |
| < 50% | Suppress | Metric is hidden due to insufficient data. |

### Data Completeness

A `data_completeness` score below 0.80 means some telemetry is still arriving. Mobile clients or batch exports often cause these delays. Treat these numbers as preliminary until the score rises.

### Concurrent Deploys

When multiple releases happen in the same environment window, attribution becomes uncertain. The system flags these periods to prevent misattributing a spike to the wrong change.

### Contamination

If a session encounters multiple active flags, it's marked as contaminated. We exclude these sessions from impact deltas by default to keep the signal clean.

## Language Policy

To prevent over-interpretation, we use specific language when discussing impact.

### Approved Terms
- "appears"
- "suggests"
- "leans"
- "is consistent with"

### Forbidden Terms
- "caused"
- "proved"
- "determined"
- "is" / "was" (when stating impact)
- "detected"

### Examples
- **Correct**: "The friction rate appears elevated after this release."
- **Incorrect**: "This release caused increased friction."

## Common Scenarios

### High Impact, Low Confidence
"Data suggests a significant impact, but the sample size is small. Wait for more sessions before taking action."

### Low Impact, High Confidence
"The release appears clean. Telemetry shows no significant shift in friction or error rates across a large sample."

### Multiple Concurrent Deploys
"Attribution is uncertain due to three concurrent releases in this window. Check individual service logs for better resolution."

### Missing Telemetry
"Metric suppressed due to insufficient coverage. Instrumentation may be missing or failing for this surface."
