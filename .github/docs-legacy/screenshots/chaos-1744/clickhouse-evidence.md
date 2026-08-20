# CHAOS-1744 — ClickHouse Evidence: Before vs After

## Before fix (org_id dropped by `_build_snapshots`)

```
SELECT count() AS total, countIf(compounding_risk IS NOT NULL) AS non_null
FROM compounding_risk_daily;
┌─total─┬─non_null─┐
│     4 │        0 │
└───────┴──────────┘
```

Every row `compounding_risk = NULL`, `severity = unknown`. Why:

```
SELECT org_id, count() FROM repo_complexity_daily GROUP BY org_id;
┌─org_id─┬─count()─┐
│        │     720 │   <- empty string, written by buggy _build_snapshots
└────────┴─────────┘
```

While the daily job ran with `org_id = ae600a94-...`. The filter
`WHERE org_id = {org_id:String}` in `load_repo_complexity_delta_30d`
matched zero rows for every repo → `complexity_delta = None` → all
required inputs unsatisfied → score = NULL.

## After fix (org_id correctly plumbed through)

After truncating + re-running `dev-hops metrics complexity --backfill 90`
and `dev-hops metrics daily --backfill 90`:

```
SELECT
  count() AS total,
  countIf(compounding_risk IS NOT NULL) AS non_null,
  countIf(severity = 'high') AS high,
  countIf(severity = 'elevated') AS elevated,
  countIf(severity = 'low') AS low,
  countIf(severity = 'unknown') AS unknown
FROM compounding_risk_daily;
┌─total─┬─non_null─┬─high─┬─elevated─┬─low─┬─unknown─┐
│   355 │       27 │    0 │       23 │   4 │     328 │
└───────┴──────────┴──────┴──────────┴─────┴─────────┘
```

```
SELECT org_id, count() FROM repo_complexity_daily GROUP BY org_id;
┌─org_id───────────────────────────────┬─count()─┐
│ ae600a94-76bc-4166-bf36-051ee4247c73 │     360 │
└──────────────────────────────────────┴─────────┘
```

27 real scores computed (23 elevated, 4 low). Remaining 328 rows are
days where `pr_first_review_p90_hours` is NULL upstream (sparse PR
review activity) — legitimate data gap, not a bug.

## Sample rows

```
SELECT day, scope, scope_id, severity, compounding_risk, complexity_delta, review_latency_p90h
FROM compounding_risk_daily WHERE compounding_risk IS NOT NULL
ORDER BY day DESC LIMIT 5;

day        | scope | scope_id              | severity | score  | cdelta | rev_p90
-----------+-------+-----------------------+----------+--------+--------+--------
2026-05-20 | repo  | 65574b20-1e8d-...     | elevated | 0.4298 |   0    |   23
2026-05-12 | repo  | 2faf8e66-de78-...     | low      | 0.3433 |   0    |    4.7
2026-05-11 | repo  | 5b862ba7-ef97-...     | elevated | 0.4458 |   0    |   23.1
2026-05-11 | repo  | 2faf8e66-de78-...     | elevated | 0.4756 |   0    |   35.3
2026-05-10 | repo  | 5b862ba7-ef97-...     | elevated | 0.4175 |   0    |   17
```
