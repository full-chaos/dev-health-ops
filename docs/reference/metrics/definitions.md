---
page_id: ref-metric-defs
summary: Contract for generated metric definitions and the source fields every entry must expose.
content_type: generated-reference
owner: product-analytics
source_of_truth:
  - current metrics schema and computation code
applicability: current
lifecycle: active
---

# Canonical metric definitions

Generate one entry per currently supported metric with:

- stable key and display label;
- question answered;
- unit and value domain;
- included population and exclusions;
- event time and window semantics;
- exact formula and aggregation;
- allowed scope and filters;
- measured-zero, null, unavailable, partial, and stale behavior;
- source tables, fields, and computation code;
- version or applicability;
- interpretation limits.

Do not publish a metric from an old planning document when no current computation or product surface supports it.

## Ask Dev V1 metric registry

Ask Dev V1 exposes exactly these eight metrics. The registry version is
`ask-dev-metrics.v1`; an unknown or deferred metric key is rejected rather than
silently mapped to a different measure.

| Stable key | Unit | Window aggregation | Source |
| --- | --- | --- | --- |
| `items_completed` | items | Sum the latest daily work-scope rows | `work_item_metrics_daily` |
| `cycle_time_p50_hours` | hours | Average the persisted latest daily/scope p50 values | `work_item_metrics_daily` |
| `avg_wip` | items | Average the latest daily status snapshots | `work_item_state_durations_daily` |
| `deployments_count` | deployments | Sum the latest daily repository rows | `deploy_metrics_daily` |
| `change_failure_rate` | ratio | Total failed deployments divided by total deployments | `deploy_metrics_daily` |
| `investment_allocation_pct` | percent | Canonical-theme completed-work share | `investment_metrics_daily` |
| `cyclomatic_per_kloc` | cyclomatic complexity per KLOC | Average the latest daily repository density | `repo_complexity_daily` |
| `compounding_risk_score` | score from 0 to 1 | Mean of the latest persisted scoped scores | `compounding_risk_daily` |

Change failure rate is weighted over the whole selected window. It is never an
average of daily percentages and never falls back to PR reverts, incidents, or
`repo_metrics_daily`. A zero deployment denominator is insufficient evidence;
it is distinct from a measured zero failure rate.

Investment allocation uses only the five canonical themes and
`work_items_completed` as its denominator. Unclassified work is excluded from
that denominator. Compounding risk returns its persisted score, components,
weights, and thresholds without recomputing the score at query time; a stable
digest identifies the component/weight/threshold version represented by the
returned rows.

Every definition publishes its unit, aggregation, display precision, null and
zero semantics, supported scopes and dimensions, range limits, comparison
rule, definition version, query version, source version, and freshness policy.
The query response preserves source evidence references and reports the prior
immediately preceding window of equal duration when comparison is requested.
