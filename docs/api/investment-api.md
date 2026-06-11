# Investment API (Aggregation & Effort Weighting)

How persisted investment distributions become the numbers shown in the product. This is
the read side of the [Investment Categorization Pipeline](../architecture/investment-categorization-pipeline.md);
the tables it reads are described in the
[Investment Data Model](../architecture/investment-data-model.md).

Query code lives in `src/dev_health_ops/api/queries/investment.py`; the user-facing
response builder is in `src/dev_health_ops/api/services/investment.py`.

---

## The one thing to understand: values are effort-weighted

A WorkUnit stores a **probability** per theme/subcategory. The API does **not** report
those probabilities directly — it multiplies each probability by the WorkUnit's
`effort_value` and sums across WorkUnits:

```sql
-- fetch_investment_breakdown (simplified)
SELECT
    subcategory_kv.1 AS subcategory,
    splitByChar('.', subcategory_kv.1)[1] AS theme,
    sum(subcategory_kv.2 * effort_value) AS value
FROM work_unit_investments
ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
WHERE from_ts < %(end_ts)s AND to_ts >= %(start_ts)s AND org_id = %(org_id)s
GROUP BY subcategory, theme
ORDER BY value DESC
```

So **"60% Feature Delivery" means 60% of weighted effort**, not 60% of tickets and not
the average probability. A WorkUnit with a large `effort_value` moves the distribution
far more than a tiny one.

### What `effort_value` is

Computed at materialization time by `_effort_from_work_unit`, with this precedence:

| Priority | Source | `effort_metric` |
| -------- | ------ | --------------- |
| 1 | Commit churn (additions + deletions) | `churn_loc` |
| 2 | PR churn (additions + deletions) | `churn_loc` |
| 3 | Issue active hours | `active_hours` |
| 4 | none → `0.0` | `churn_loc` |

A WorkUnit with `effort_value == 0` contributes nothing to weighted aggregates even
though it still has a valid distribution.

---

## Query surfaces

| Function | Shape | Weighting |
| -------- | ----- | --------- |
| `fetch_investment_breakdown` | subcategory + theme → value | `subcategory_prob * effort_value` |
| `fetch_investment_edges` | theme → repo/scope → value | `theme_prob * effort_value` |
| `fetch_investment_subcategory_edges` | subcategory → repo/scope → value | `subcategory_prob * effort_value` |
| `fetch_investment_team_edges` | theme → team → value | `theme_prob * effort_value` |

All of them `ARRAY JOIN` the stored `Map` distribution into rows, filter by the time
window (`from_ts < end_ts AND to_ts >= start_ts`) and `org_id`, then group.

Themes are derived in SQL from the subcategory prefix
(`splitByChar('.', subcategory)[1]`), mirroring the deterministic roll-up done at
compute time — the API never re-categorizes.

---

## `unassigned` means missing scope, not a category

In edge queries the target is computed as, e.g.:

```sql
ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS target
```

Here `unassigned` is a **scope/grouping label** for a WorkUnit with no resolved
repo/team — it is **not** an investment category and never appears in a theme or
subcategory distribution. The categorization itself never returns "unknown"
(see the [pipeline guarantees](../architecture/investment-categorization-pipeline.md#guarantees)).

> Do not confuse this with the legacy rule-based classifier in
> `analytics/investment.py`, which uses `unassigned` as a *category* fallback. That path
> is legacy and is being deprecated/isolated; it is not part of the canonical Investment
> View.

---

## Read-semantics caveat (latest row)

`work_unit_investments` is `ReplacingMergeTree(computed_at)`, but these queries `sum(...)`
directly without `FINAL` or `argMax(..., computed_at)`. After a re-materialization,
duplicate rows for the same `work_unit_id` may be double-counted until ClickHouse merges.
This is a tracked engineering issue; see the
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
