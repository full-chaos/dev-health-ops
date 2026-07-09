# Compounding Risk

> _High churn meeting high complexity meeting low ownership meeting slow review._

Compounding Risk is a deterministic, inspectable composite signal that flags
where change pressure is overlapping with architectural and operational risk.
It is the canonical "Compounding Risk" wedge from
[`dev-health-product-market-simplification.md`](../product/dev-health-product-market-simplification.md)
and is consumed by the `/risk/compounding` web surface (CHAOS-1642).

This document specifies the formula. The implementation lives in
`src/dev_health_ops/metrics/compounding_risk.py`; the persisted output lives
in the `compounding_risk_daily` ClickHouse table.

---

## Inputs

For each `(org_id, repo_id, day)` row, four signals are combined:

| Component | Source | Field |
|---|---|---|
| Churn | `repo_metrics_daily` | `rework_churn_ratio_30d` |
| Complexity trend | `repo_complexity_daily` | relative change in `cyclomatic_per_kloc` over the trailing window |
| Ownership concentration | `repo_metrics_daily` | `max(single_owner_file_ratio_30d, code_ownership_gini)` |
| Review latency (tail) | `repo_metrics_daily` | `pr_first_review_p90_hours` |

Window: trailing 30 days. The complexity delta uses the first-half-versus-second-half
average so a recent rise in complexity is captured even when absolute values are small.

Inputs come from the current `(org_id, repo_id, day)` compute row. If any
required input is missing on that row, the persisted score is `NULL` and the
severity is `unknown`. **Missing data is not zero risk.**

Read surfaces resolve to the most recent complete (fully-scored) day within the
window: partial or in-progress current days whose latest Compounding Risk score
is unavailable are skipped. If no scored day exists in the window, the score is
unavailable rather than carried forward or recomputed.

---

## Formula

```
churn_norm        = clamp01( max(0, rework_churn) / CHURN_REF )           # CHURN_REF = 0.30
complexity_norm   = clamp01( max(0, complexity_delta) / COMPLEXITY_REF )  # COMPLEXITY_REF = 0.20
ownership_norm    = clamp01( max(single_owner_ratio, ownership_gini) )    # already in [0, 1]
review_norm       = clamp01( max(0, review_latency_p90h) / REVIEW_REF )   # REVIEW_REF = 48h

compounding_risk  =
      W_CHURN      * churn_norm
    + W_COMPLEXITY * complexity_norm
    + W_OWNERSHIP  * ownership_norm
    + W_REVIEW     * review_norm
```

Default weights (must sum to `1.0`; enforced at construction time):

| Weight | Default |
|---|---|
| `W_CHURN` | 0.30 |
| `W_COMPLEXITY` | 0.30 |
| `W_OWNERSHIP` | 0.20 |
| `W_REVIEW` | 0.20 |

The composite is always in `[0, 1]`.

Reference values are intentional saturation thresholds, **not** caps on the
underlying inputs. They are documented constants in
`metrics/compounding_risk.py::REFERENCE_VALUES`.

---

## Severity

| Score | Severity |
|---|---|
| `score is None` | `unknown` |
| `score < 0.40` | `low` |
| `0.40 <= score < 0.65` | `elevated` |
| `score >= 0.65` | `high` |

Thresholds and weights are persisted with each row. **Historical rows retain
the bucket they were computed under**, so changes to defaults do not silently
re-bucket past data.

---

## Persistence

Table: `compounding_risk_daily` (migration `040_compounding_risk_daily.sql`).

Append-only with `computed_at`. Read latest with:

```sql
SELECT
  scope_id,
  argMax(compounding_risk, computed_at)   AS score,
  argMax(severity, computed_at)            AS severity,
  argMax(churn_norm, computed_at)          AS churn_norm,
  argMax(complexity_norm, computed_at)     AS complexity_norm,
  argMax(ownership_norm, computed_at)      AS ownership_norm,
  argMax(review_norm, computed_at)         AS review_norm
FROM compounding_risk_daily
WHERE org_id = 'demo-org'
  AND scope = 'repo'
  AND day = today()
GROUP BY scope_id
ORDER BY score DESC NULLS LAST;
```

Every row carries:
- the **composite score** and **severity**,
- the **four normalized components** (so each contribution is visible),
- the **raw inputs** (so the score can be re-derived by hand),
- the **weights and thresholds in force at compute time** (audit trail).

This is the inspectability contract — no opaque scoring.

---

## Scope

v1 persists **repo-scope rows** and, when a repo-to-team mapping is available,
**team-scope rows**. Team rows aggregate the resolved per-repo raw inputs by
unweighted mean and then apply the same formula, preserving inspectability. The
GraphQL resolver still supports read-time aggregation as a compatibility
fallback when persisted team rows are absent.

Per the no-surveillance contract, **there is no per-person scope** for this
metric, and the web surface explicitly locks the scope picker against it.

---

## Orchestration

Compounding Risk runs as part of `dev-hops metrics daily` and can also be
recomputed from persisted inputs with `dev-hops metrics compounding-risk`:

1. `compute_daily_metrics` writes `repo_metrics_daily` for the day.
2. `build_compounding_risk_rows_for_day` reads the just-written
   `repo_metrics_daily` rows and queries `repo_complexity_daily` for the
   current row's complexity delta.
3. The resulting rows are written via `sink.write_compounding_risk_daily(...)`.

No new connector or processor work is required; the pipeline already
captures every input.

---

## Non-negotiables

- ClickHouse-only persistence.
- Sink-only writes; no file exports.
- Deterministic compute; no LLM at compute time.
- No per-person scope.
- Score is always inspectable: weights, thresholds, raw inputs, and
  normalized components are all persisted alongside the score.
