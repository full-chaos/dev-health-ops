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

## Blocked hours in `work_item_state_durations_daily`

`work_item_state_durations_daily` holds, for each day, the hours that work
items spent in each normalized status. Blocked Work reads the rows whose
status is `blocked`. An item's hours are `blocked` in two cases:

1. **Status name or label.** The provider status maps to `blocked` (for
   example a status named "Blocked" or "On Hold", or a label `blocked`).
2. **An open blocker.** Another work item blocks it. This comes from the
   blocking relations the providers report (`work_item_dependencies`): Jira
   issue links, GitLab issue links, Linear relations, and for GitHub the
   words "blocked by", "depends on" or "blocks" before an issue reference in
   the issue text. GitHub has no native blocking relation in this data.

For case 2 the item is `blocked` only while all of these hold:

- its own status is not `done` or `canceled`;
- the blocker is a synced work item of the same organization;
- the relation is known to exist;
- the blocker is open (until its completion time).

In that interval `blocked` replaces the item's own status. The item's total
hours in the day do not change.

When the relation is known to exist:

- **Start.** The provider's own time of the link, when the synced data
  carries one. If it does not, the first time a sync saw the relation. The
  second is too late when the link is older than the first sync that saw it,
  so blocked hours can be too low. They are never too high: the creation time
  of the two items is never used as the start.
- **End.** When the provider no longer reports the relation, the last time a
  sync saw it. A removed link is noticed when the items it belongs to are
  synced again. This end can be too early and is never too late. The blocked
  hours of earlier days do not change when a link is removed.

Limits of the relation data:

- A relation with no stored start gives no blocked hours.
- A blocker that is not a synced work item, or a finished blocker that has no
  completion time, gives no blocked hours. Missing data is not estimated.
- The current status of an item (for example in `work_item_cycle_times`) is
  not changed by a relation. Only the daily hours are.
