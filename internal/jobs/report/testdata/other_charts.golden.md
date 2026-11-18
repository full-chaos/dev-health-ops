# Weekly Health Report — 2026-01-01 → 2026-01-07

## Summary
Success Rate appears near 95.0% for the selected window.

Pipelines Count ranges from 5 to 7 across 2 team groups in the selected window.

Avg Duration Minutes ranges from 3.5m to 4.0m across 2 team groups in the selected window.

## TestOps
Success Rate appears near 95.0% for the selected window.

### Insights
No grounded insights for this section.

### Charts
#### Success rate

| x | y | group |
| --- | ---: | --- |
| 2026-01-01 | 0.95 |  |

## Other Charts
#### Pipelines count

| x | y | group |
| --- | ---: | --- |
| team-a | 5 |  |
| team-b | 7 |  |

#### Average duration

| x | y | group |
| --- | ---: | --- |
| team-a | 3.5 |  |
| team-b | 4 |  |

## Provenance

- **chart:chart-1** — sources: testops_pipeline_metrics_daily; metrics: success_rate
- **chart:chart-2** — sources: cicd_metrics_daily; metrics: pipelines_count
- **chart:chart-3** — sources: cicd_metrics_daily; metrics: avg_duration_minutes
- **narrative:summary** — sources: testops_pipeline_metrics_daily; metrics: success_rate
- **narrative:testops** — sources: testops_pipeline_metrics_daily; metrics: success_rate
- **report:plan-2** — sources: n/a; metrics: success_rate

---
Generated at 2026-01-08T00:00:00+00:00 | Confidence: direct_fact | Scope: teams=team-a
