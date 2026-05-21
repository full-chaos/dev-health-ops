# Operating Review: aggregation contract

**Status:** Authoritative (CHAOS-1755)

## Modes

The GraphQL `operatingReview(orgId, input)` resolver supports two modes,
selected by whether `input.teamId` is provided.

| Mode | `input.teamId` | `OperatingReview.teamId` in response |
|------|----------------|--------------------------------------|
| Single team | `"team-3"` (any non-null string) | `"team-3"` |
| All teams (cross-team aggregate) | `null` / omitted | `null` |

Clients use the response `teamId` to decide labeling (e.g. render a team
name vs. an explicit "All Teams" badge). Do **not** infer aggregate mode
from the input alone; the response is the source of truth.

## Per-metric aggregation rules (All Teams mode)

When `teamId` is null, the ClickHouse queries built by
`build_operating_review_queries(team_id=None)` drop the
`AND team_id = %(team_id)s` predicate and add `team_id` to the inner
`GROUP BY` so per-team rows are not collapsed by `argMax(..., computed_at)`
mid-aggregation. The outer aggregation function then determines the
cross-team behavior per metric.

### Delivery movement

| Metric key | Aggregation across teams | Notes |
|------------|--------------------------|-------|
| `throughput` (`items_completed`) | **SUM** | Total org throughput. |
| `cycle_time_p50_hours` | **AVG** (unweighted) | Average of per-(provider, scope, team) p50s. Approximation — see "Known limitations" below. |
| `wip_count` (`wip_count_end_of_day`) | **MAX** per day across (provider, scope, team), then weekly behavior | Peaks rather than totals; see limitations. |

### Bottleneck

| Metric key | Aggregation across teams | Notes |
|------------|--------------------------|-------|
| `state_duration_hours` | **Weighted AVG** (weight = `items_touched`) | Done in Python at compute time; weights work correctly across teams because `items_touched` SUMs across them at the SQL layer. |
| `review_latency_hours` (`pr_first_review_p50_hours`) | **AVG** (unweighted) | Repo-scoped; team-agnostic at the row level. Unchanged across modes. |
| `wip_age_p90_hours` | **AVG** (unweighted) | Approximation — see limitations. |

### Risk

| Metric key | Aggregation across teams | Notes |
|------------|--------------------------|-------|
| `hotspot_risk_score` | **AVG** | Already repo-scoped; team-agnostic. |
| `ownership_concentration` | **AVG** | Already repo-scoped; team-agnostic. |
| `complexity_per_kloc` | **AVG** | Already repo-scoped; team-agnostic. |
| `bus_factor` | **MIN** | Already repo-scoped; team-agnostic. |

### Reliability

| Metric key | Aggregation across teams | Notes |
|------------|--------------------------|-------|
| `deployments_count` | **SUM** | Repo-scoped; unchanged across modes. |
| `change_failure_rate` | **AVG** of repo `change_failure_rate` | Repo-scoped; unchanged across modes. |
| `incidents_count` | **SUM** | Repo-scoped; unchanged across modes. |
| `mttr_hours` | First non-zero of `incidents.mttr_p50_hours` then `repo_metrics.mttr_hours`, both **AVG** | Repo-scoped; unchanged across modes. |

### Investment

| Metric key | Aggregation across teams | Notes |
|------------|--------------------------|-------|
| `ktlo_units` / `new_value_units` / `security_units` / `infra_units` (`delivery_units`) | **SUM** | Total org investment per area. |

## Improved / Worsened / Changed callouts

Computed exactly as in single-team mode: per-metric, comparing the
current week's aggregated value to the prior week's aggregated value.
No special-cased thresholds for aggregate mode.

## Known limitations

1. **Percentile approximations.** `cycle_time_p50_hours`, `wip_age_p90_hours`
   are stored as already-aggregated per-team daily values. Averaging them
   across teams is an average-of-averages, not a true cross-team percentile.
   To compute a true cross-team percentile we would need raw item-level
   data, which is not currently materialised in `work_item_metrics_daily`.

2. **WIP `MAX` semantics.** Aggregate WIP is the peak single
   `(provider, scope, team)` WIP per day, not the sum across teams.
   This was the original single-team behavior and is preserved for
   consistency, but it means the all-teams WIP can read lower than the
   true total work in flight.

3. **Inner `GROUP BY` extension.** When `team_id` is null we keep
   `team_id` in the inner `GROUP BY` purely so `argMax(..., computed_at)`
   continues to pick one canonical row per team per (day, provider,
   scope). Outer aggregation then combines correctly across teams. This
   is intentional and load-tested at fixture scale (10 teams × 30 days);
   for very large orgs the inner cardinality may need a different shape.

## Files

- `src/dev_health_ops/metrics/operating_review.py` —
  `build_operating_review_queries(team_id=...)` and `compute_operating_review`.
- `src/dev_health_ops/api/graphql/models/inputs.py` —
  `OperatingReviewInput.team_id: str | None`.
- `src/dev_health_ops/api/graphql/models/outputs.py` —
  `OperatingReview.team_id: str | None`.
- `src/dev_health_ops/api/graphql/resolvers/operating_review.py` —
  threads optional `team_id` through `_fetch_period_rows`.
- `tests/metrics/test_operating_review.py` — covers both modes.

## History

- **CHAOS-1755**: Introduced "All Teams" mode by making `team_id`
  optional and documenting per-metric aggregation rules.
- **CHAOS-1751**: Established `teams` as the source of truth for the
  TEAM dimension catalog (separate from this contract).
