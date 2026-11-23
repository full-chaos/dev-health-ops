-- FORM-SCHED-ALIVE-1 (read only, Postgres). Header, WANTs, writer cites, CI exec output: FORM-SCHED-ALIVE-1.md.
-- PIN (live workers) a7febdf500025f5ac628a1bdf34d0f817a90e323.
-- Question: does the scheduler still create scheduled sync runs today (scheduled for 2026-11-23 01:00Z or later)?
-- Org scope: starts_with(<org column>::text, 'c6a38355') on EVERY FROM of an org table (organizations: id).
-- pg_catalog (stmt0) is the only other read: catalog estimates, no org data.
-- No SET. No json read. Output = counts, minutes and the triggered_by label (closed writer set) only. No id is printed.
-- The COST GATE (stmt0) runs FIRST: any error aborts the transaction, so no later statement runs after a gate stop.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- stmt0 COST GATE. Catalog only: pg_class reltuples and relpages x block_size of the 2 tables read. No table scan,
-- no size function. Over a bound, reltuples < 0 (never analyzed: unknown, fail closed) or a table not found (NULL),
-- the CASE casts 'STOP-COST-GATE-SCHED-ALIVE-1 ...' to int: a named error. Bounds (basis in the .md):
--   sync_runs      rows <= 1000000, heap bytes <= 2147483648 (2 GiB)
--   organizations  rows <=  100000, heap bytes <=  268435456 (256 MiB)
-- WANT: 2 rows, each cost_gate_ok_0 = 0 (else the named error and the read stops).
SELECT b.relname,
       CASE
         WHEN g.heap_rows IS NULL OR g.heap_rows < 0 OR g.heap_rows > b.max_rows OR g.heap_bytes > b.max_bytes
         THEN ('STOP-COST-GATE-SCHED-ALIVE-1 table=' || b.relname || ' rows=' || COALESCE(g.heap_rows::text, 'missing')
               || ' heap_bytes=' || COALESCE(g.heap_bytes::text, 'missing')
               || ' max_rows=' || b.max_rows || ' max_bytes=' || b.max_bytes)::int
         ELSE 0
       END AS cost_gate_ok_0,
       g.heap_rows, g.heap_bytes
FROM (VALUES ('sync_runs',     1000000::bigint, 2147483648::bigint),
             ('organizations',  100000::bigint,  268435456::bigint)) AS b(relname, max_rows, max_bytes)
LEFT JOIN (SELECT c.relname::text AS relname,
                  c.reltuples::bigint AS heap_rows,
                  c.relpages::bigint * current_setting('block_size')::bigint AS heap_bytes
           FROM pg_catalog.pg_class AS c
           JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
           WHERE n.nspname = 'public' AND c.relkind = 'r') AS g
  ON g.relname = b.relname
ORDER BY b.relname;

-- S0 ORG-1: organizations matched by the prefix. WANT 1.
SELECT count(*) AS orgs_matched
FROM public.organizations AS o
WHERE starts_with(o.id::text, 'c6a38355');

-- S1 SCHEDULED RUNS since the instant. triggered_by 'schedule' = the cron arm stamp (materializer.go:704, written by
-- the sync_runs INSERT at materializer.go:2474-2477, created_at = occurrence.ScheduledFor, materializer.go:556).
-- Terminal run statuses 'success','partial_failed','failed' (native_finalize_sync_run.go:118-120). One pass, FILTER.
-- WANT: sched_runs >= 1. sched_runs = 0 with C1 all_runs > 0 = a possible defect (acp's call).
SELECT count(*)                                                                         AS sched_runs,
       date_trunc('minute', max(r.created_at))                                          AS sched_max_minute,
       count(*) FILTER (WHERE r.status IN ('success', 'partial_failed', 'failed'))      AS sched_terminal,
       count(*) FILTER (WHERE r.status NOT IN ('success', 'partial_failed', 'failed'))  AS sched_non_terminal
FROM public.sync_runs AS r
WHERE starts_with(r.org_id::text, 'c6a38355')
  AND r.triggered_by = 'schedule'
  AND r.created_at >= '2026-11-23 01:00:00+00';

-- C1 CONTROL: the same counts for ALL triggered_by values since the same instant. WANT all_runs > 0.
SELECT count(*)                                                                         AS all_runs,
       date_trunc('minute', max(r.created_at))                                          AS all_max_minute,
       count(*) FILTER (WHERE r.status IN ('success', 'partial_failed', 'failed'))      AS all_terminal,
       count(*) FILTER (WHERE r.status NOT IN ('success', 'partial_failed', 'failed'))  AS all_non_terminal
FROM public.sync_runs AS r
WHERE starts_with(r.org_id::text, 'c6a38355')
  AND r.created_at >= '2026-11-23 01:00:00+00';

-- C2 CONTROL: per triggered_by label since the same instant (a renamed literal shows as its own row). Labels are a
-- closed writer set: 'schedule' (materializer.go:704) or the sync_manual_triggers label 'manual'/'backfill'
-- (materializer.go:716, CHECK ck_sync_manual_triggers_triggered_by). WANT: the row sum = C1 all_runs.
SELECT r.triggered_by,
       count(*)                                                                         AS runs,
       date_trunc('minute', max(r.created_at))                                          AS max_minute,
       count(*) FILTER (WHERE r.status IN ('success', 'partial_failed', 'failed'))      AS terminal,
       count(*) FILTER (WHERE r.status NOT IN ('success', 'partial_failed', 'failed'))  AS non_terminal
FROM public.sync_runs AS r
WHERE starts_with(r.org_id::text, 'c6a38355')
  AND r.created_at >= '2026-11-23 01:00:00+00'
GROUP BY r.triggered_by
ORDER BY r.triggered_by;

ROLLBACK;
