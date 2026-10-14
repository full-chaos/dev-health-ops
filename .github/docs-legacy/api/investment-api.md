# Investment API (Aggregation & Measure)

How persisted investment distributions become the numbers shown in the product. This is
the read side of the [Investment Categorization Pipeline](../architecture/investment-categorization-pipeline.md);
the tables it reads are described in the
[Investment Data Model](../architecture/investment-data-model.md).

Query code lives in `src/dev_health_ops/api/queries/investment.py`; the user-facing
response builder is in `src/dev_health_ops/api/services/investment.py`.

---

## The one thing to understand: values are a COUNT of work units

A WorkUnit stores a **probability** per theme/subcategory. The API does **not** report
those probabilities directly. It reports the measure `COUNT` (CHAOS-9622, D5999): each
WorkUnit counts **1** in total, spread over the themes and subcategories by its stored
probabilities, and over the repositories it touches by its repository allocation weight.
The REST `/investment` and `/investment/sunburst`, the Home tile "Investment by theme" and the
Operating Review investment section all read this one measure, the one the Investment page
reads (`investmentexplain.InvestmentShareMeasure`, `MeasureValueSQL`):

```sql
-- investmentexplain.Reader.FetchInvestmentBreakdown, measure COUNT (simplified)
SELECT
    subcategory_kv.1 AS subcategory,
    splitByChar('.', subcategory_kv.1)[1] AS theme,
    sum(subcategory_kv.2 * <allocation_weight under a repo/team scope, else 1> * <owner share, else 1>) AS value
FROM latest_work_unit_investments AS work_unit_investments
ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
WHERE from_ts < %(end_ts)s AND to_ts >= %(start_ts)s AND org_id = %(org_id)s
GROUP BY subcategory, theme
ORDER BY value DESC
```

So **"60% Feature Delivery" means 60% of the work units**, not 60% of lines of code, and not
the average probability. **Lines of code are not a measure of effort** (D5999): a WorkUnit's
`effort_value` (built from churn LOC, then active hours) is never a weight. A read must name
its measure; a read that names none is refused (`ErrMeasureRequired`), so no path can fall back
to a weight. The Investment paths offer no lines-of-code measure, unit or field
(CHAOS-9695, D6014): `CHURN_LOC` stays a measure for the delivery-health analytics (team and
repository dimensions) and is refused on every Investment path (theme, subcategory and work type
dimensions, the flow, mix, coverage and ownership reads, the explanation), the flow's `unit` is
always `WORK_UNITS`, and a work unit's effort is its work-unit count, not lines of code. Sizing the work by issue
size (CHAOS-9619) will change the one named measure; it will not bring lines of code back.

The Home tile dates a unit by its END day (D5933) and the Operating Review serves the same
measure per week under the metric names `<theme>_work` (unit `work units`).

### Team scope: every read weighs a repository by the owners' share

On a team scope (`scope.level = team` with ids) each repository a unit touches is weighed by the share of it the
teams in view hold (`teamscope.RepoShare`, D5988: the linked share of the main owner, the lesser share of a
co-owner; the scoped teams that co-own one repository together read it once, never more). The REST routes, the
explanation and the Investment page (breakdown, timeseries, sankey, flow matrix and coverage) apply this one
weight (CHAOS-9629). Additive measures (COUNT) sum `value x share`; an intensive measure (cycle
time) is the share-weighted mean `sum(value x share) / sum(share)`, never `value x share` (D5998). The TEAM
dimension keeps its ownership join and applies the same share once.

### What the REST `/investment` response says about its measure

Besides `theme_distribution`, `subcategory_distribution` and the evidence-quality fields, the
response carries two additive Go-only fields (CHAOS-9622): `measure` (`"COUNT"`) and
`denominator` (the sum of all themes in the window). The values of the distributions are the
numerators, so `share = value / denominator`. A response for a store with no investment table (or without its columns) leaves both out; a window with no
units serves `COUNT` and a `denominator` of 0.

### What `effort_value` is (stored, not a weight)

Computed at materialization time by `_effort_from_work_unit`, with this precedence:

| Priority | Source | `effort_metric` |
| -------- | ------ | --------------- |
| 1 | Commit churn (additions + deletions) | `churn_loc` |
| 2 | PR churn (additions + deletions) | `churn_loc` |
| 3 | Issue active hours | `active_hours` |
| 4 | none → `0.0` | `churn_loc` |

It stays in the table, and no share is weighted by it; the Home tile's `churn_loc` line (health)
is the only read of it. The work-unit
list serves each unit's `effort` as `{"metric": "work_units", "value": <count>}`: the work-unit
count the row stands for (1 for a whole unit, the unit's allocation weight under a repository
scope), ordered by that count, then by the end of the unit. The lines of code of a unit are a separate
fact, `change_set` `{additions, deletions, total}` (null where the unit has none): `total` is the stored churn;
the split is not stored per unit, so `additions` and `deletions` are null. It is never effort, a weight or a sort key.

---

## Query surfaces

| Function | Shape | Weighting |
| -------- | ----- | --------- |
| `fetch_investment_breakdown` | subcategory + theme → value | `subcategory_prob` (COUNT) |
| `fetch_investment_edges` | theme → repo/scope → value | `theme_prob * effort_value` (Python reader, not changed by CHAOS-9622; the Go routes do not use it) |
| `fetch_investment_subcategory_edges` | subcategory → repo/scope → value | `subcategory_prob * effort_value` (Python reader, not changed by CHAOS-9622; the Go routes do not use it) |
| `fetch_investment_team_edges` | subcategory → team → value | `subcategory_prob * effort_value` (Python reader, not changed by CHAOS-9622; the Go routes do not use it) |

All of them `ARRAY JOIN` the stored `Map` distribution into rows, filter by the time
window (`from_ts < end_ts AND to_ts >= start_ts`) and `org_id`, then group.

Themes are derived in SQL from the subcategory prefix
(`splitByChar('.', subcategory)[1]`), mirroring the deterministic roll-up done at
compute time — the API never re-categorizes.

---

## `unassigned` means missing scope, not a category

Some edge queries surface an `unassigned` **scope label** for WorkUnits with no resolved
repo or team. The exact expression varies by query, e.g.:

```sql
-- fetch_investment_subcategory_edges: missing repo
ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS target
-- fetch_investment_team_edges: missing team
ifNull(nullIf(unit_team.team, ''), 'unassigned') AS target
```

(`fetch_investment_edges` and the sunburst query instead use `ifNull(r.repo, toString(repo_id))`,
which can fall back to the raw `repo_id`.) In every case `unassigned` is a **scope/grouping
label** for a WorkUnit with no resolved repo/team — it is **not** an investment category and
never appears in a theme or subcategory distribution. The categorization itself never returns
"unknown" (see the [pipeline guarantees](../architecture/investment-categorization-pipeline.md#guarantees)).

> Do not confuse this with the legacy rule-based classifier in
> `analytics/investment.py`. That path is explicitly deprecated/isolated to the
> pre-WorkUnit daily `investment_*` metric tables; it is not part of the canonical
> Investment View and now falls back to the legacy `product/general` bucket rather
> than an `unassigned` category.

---

## Read semantics (latest row)

`work_unit_investments` is `ReplacingMergeTree(computed_at)`, so duplicate physical rows
for the same `work_unit_id` can exist until ClickHouse merges them. Investment API reads
use an explicit latest-row subquery (`argMax(..., computed_at)` grouped by
`work_unit_id`) before any `sum(...)`. This gives API totals
latest-row-by-`computed_at` semantics without requiring `FINAL`; see the
[data model read-semantics note](../architecture/investment-data-model.md#read-semantics-important).

---

## Evidence quality stats

`api/services/investment.py` also surfaces evidence-quality statistics (the `0–1` score
and its band) alongside distributions, and guards for missing tables/columns. Use these
to convey confidence in the UI — a distribution dominated by low-quality or fallback
WorkUnits should be presented with appropriate uncertainty (see the
[LLM Categorization Contract](../llm/categorization-contract.md#ux-time-explanation)).

## Related

- [Investment Categorization Pipeline](../architecture/investment-categorization-pipeline.md)
- [Investment Data Model](../architecture/investment-data-model.md)
- [Web Investment Queries](web-graphql-investment.md)
- [View Mapping](view-mapping.md)
