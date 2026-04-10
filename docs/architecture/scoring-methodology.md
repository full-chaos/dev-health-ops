# Scoring Methodology

The platform-health scoring model blends TestOps and engineering signals into
four dimensions. Each dimension produces a 0.0–1.0 score where higher values
*suggest* better health. The composite score blends all four dimensions.

> **Language note:** Scores are hypothesis starters, not verdicts. They
> *appear* to indicate patterns — they do not *determine* outcomes.

---

## Dimensions

### Delivery (weight 0.30)

Captures how smoothly code changes flow from commit to production.

| Signal                 | Weight | Source Table                        | Normalisation                                         |
|------------------------|--------|-------------------------------------|-------------------------------------------------------|
| pipeline_success_rate  | 0.35   | testops_pipeline_metrics_daily      | Direct (0.0–1.0 rate)                                 |
| pipeline_duration_p95  | 0.25   | testops_pipeline_metrics_daily      | Inverse linear, ceiling 3 600 s (1 h)                 |
| pr_cycle_time          | 0.25   | repo_metrics_daily                  | Inverse linear, ceiling 168 h (1 w)                   |
| throughput             | 0.15   | repo_metrics_daily                  | Linear, ceiling 50 PRs/day                            |

### Durability (weight 0.25)

Reflects test and coverage robustness — how likely changes are to survive.

| Signal                    | Weight | Source Table                        | Normalisation                             |
|---------------------------|--------|-------------------------------------|-------------------------------------------|
| coverage_line_pct         | 0.30   | testops_coverage_metrics_daily      | Divide by 100                             |
| test_pass_rate            | 0.30   | testops_test_metrics_daily          | Direct (0.0–1.0 rate)                     |
| test_flake_rate_inverse   | 0.25   | testops_test_metrics_daily          | 1 − flake_rate                            |
| coverage_branch_pct       | 0.15   | testops_coverage_metrics_daily      | Divide by 100                             |

### Well-being (weight 0.25)

Surfaces indicators that *lean towards* unsustainable work patterns.

| Signal                         | Weight | Source Table                        | Normalisation                           |
|--------------------------------|--------|-------------------------------------|-----------------------------------------|
| pipeline_queue_time_inverse    | 0.30   | testops_pipeline_metrics_daily      | 1 − (queue_sec / 600)                  |
| rerun_rate_inverse             | 0.25   | testops_pipeline_metrics_daily      | 1 − rerun_rate                          |
| after_hours_ratio_inverse      | 0.25   | team_metrics_daily                  | 1 − after_hours_commit_ratio            |
| weekend_ratio_inverse          | 0.20   | team_metrics_daily                  | 1 − weekend_commit_ratio                |

### Dynamics (weight 0.20)

Gauges team responsiveness and flow health — how quickly the system recovers.

| Signal                          | Weight | Source Table                        | Normalisation                          |
|---------------------------------|--------|-------------------------------------|----------------------------------------|
| quality_drag_inverse            | 0.35   | testops_quality_drag                | 1 − (drag_hours / 8)                  |
| failure_ownership               | 0.25   | testops_pipeline_metrics_daily      | rerun_rate where failure_count > 0     |
| wip_congestion_inverse          | 0.20   | work_item_metrics_daily             | 1 − wip_congestion_ratio              |
| pipeline_failure_rate_inverse   | 0.20   | testops_pipeline_metrics_daily      | 1 − failure_rate                       |

---

## Composite Score

```
composite = Σ (dimension_score × dimension_weight) / Σ dimension_weight
```

Dimension weights:

| Dimension  | Weight |
|------------|--------|
| Delivery   | 0.30   |
| Durability | 0.25   |
| Well-being | 0.25   |
| Dynamics   | 0.20   |

When a dimension has no data, its weight is excluded from the denominator so
the composite adjusts to available evidence.

---

## Missing Data

- Individual signals that cannot be fetched return `None` and are excluded
  from the dimension's weighted average.
- If *all* signals in a dimension are missing, the dimension score is `None`
  and excluded from the composite.
- If *no* dimensions can be scored, the composite score is `None`.

---

## Normalisation Contract

All normalised values are clamped to `[0.0, 1.0]`.

- **Direct signals** (rates already in 0–1): used as-is.
- **Inverse signals** (lower raw value appears healthier): `1.0 − normalised`.
- **Ceiling-based** (unbounded raw values): `raw / ceiling`, clamped.

---

## Interpretation Guidance

Scores are **trends, not absolutes**. A composite score of 0.72 does not mean
the team "appears 72% healthy" — it *suggests* that the available signals lean
toward a generally positive pattern relative to the configured ceilings.

Avoid using scores for person-to-person comparison or performance evaluation.
