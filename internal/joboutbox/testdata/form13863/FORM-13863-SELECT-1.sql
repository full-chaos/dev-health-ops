-- FORM-13863-SELECT-1 (pm-14090, for acp): what the stale-finalize stage of ops #6824 (b0b99a5cc1fd) selects among the
-- running + finalize-pending daily runs past its 24 h horizon. Counts, 8-char run cuts and days only. Run:
--   psql -X -v ON_ERROR_STOP=1 -f FORM-13863-SELECT-1.sql      (on the primary; no SET; pm keeps the session timeout)
-- Stage base = internal/joboutbox/stale_finalize_repair.go:409-416 (git show b0b99a5cc1fd): status 'running' AND
-- finalization_status 'pending' AND run.updated_at <= cutoff AND NOT EXISTS (partition with status <> 'succeeded' OR
-- updated_at > cutoff). Cutoff here = now() - 24 h (StaleFinalizeHorizon, :28).
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (reads no data table). WANT cost_gate_ok = 0, else the named cast error aborts the transaction.
-- Bounds S1-S3: one read of daily_metrics_runs (<= 200,000 rows) + per running+pending run one index probe of
-- daily_metrics_partitions through ix_daily_metrics_partition_run_status (<= 500,000 rows in the table).
WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')),
rel AS (SELECT c.relname, c.reltuples::bigint AS reltuples,
               (c.relpages::bigint + COALESCE(t.relpages::bigint, 0)) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('daily_metrics_runs', 'daily_metrics_partitions')),
idx AS (SELECT count(*) AS ok
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
        WHERE n.nspname = 'public' AND c.relname = 'ix_daily_metrics_partition_run_status'
          AND i.indisvalid AND i.indisready),
facts AS (
  SELECT (SELECT n FROM orgs) AS orgs,
         (SELECT count(*) FROM rel) AS tables,
         (SELECT count(*) FROM rel WHERE reltuples < 0) AS never_analyzed,
         (SELECT reltuples FROM rel WHERE relname = 'daily_metrics_runs') AS runs,
         (SELECT reltuples FROM rel WHERE relname = 'daily_metrics_partitions') AS partitions,
         (SELECT ok FROM idx) AS index_ok)
SELECT CASE
         WHEN orgs = 1 AND tables = 2 AND never_analyzed = 0 AND index_ok = 1 AND runs <= 200000 AND partitions <= 500000
         THEN 0
         ELSE ('STOP-G0 orgs=' || orgs || ' tables=' || tables || ' unanalyzed=' || never_analyzed
               || ' index_ok=' || index_ok || ' runs=' || runs || ' partitions=' || partitions)::int
       END AS cost_gate_ok
FROM facts;

-- S1 control (can fail): the status literal matches real rows. WANT running_runs > 0 (the 13863 runs are 'running'),
-- else STOP: the literal is wrong and S2/S3 prove nothing.
SELECT count(*) AS running_runs FROM public.daily_metrics_runs AS run WHERE run.status = 'running';

-- S2 per run: every 'running' + finalization 'pending' run past the horizon, one row each (at most 50).
--   status_arm   = a partition with status <> 'succeeded' (the arm that does NOT expire as the horizon moves)
--   recent_only  = no such partition, but a partition written after the cutoff (expires within 24 h)
--   no_partition = the run has no partition (in the stage base; classed no_partitions by the stage, never redriven)
--   in_base      = the stage base holds the run
-- WANT: the 8 runs of the 13863 card = 8 rows with status_arm = true, failed_permanent = 0, in_base = false; any other row
-- is INFO (named by its run8 and day). app_org = the run belongs to the app org (prefix only).
WITH cutoff AS (SELECT now() - interval '24 hours' AS at),
runs AS (
  SELECT run.id, run.org_id, run.target_day, run.updated_at
  FROM public.daily_metrics_runs AS run CROSS JOIN cutoff
  WHERE run.status = 'running' AND run.finalization_status = 'pending' AND run.updated_at <= cutoff.at
),
facts AS (
  SELECT r.id, r.org_id, r.target_day,
         EXISTS (SELECT 1 FROM public.daily_metrics_partitions p WHERE p.run_id = r.id AND p.status <> 'succeeded') AS status_arm,
         EXISTS (SELECT 1 FROM public.daily_metrics_partitions p CROSS JOIN cutoff
                 WHERE p.run_id = r.id AND p.updated_at > cutoff.at) AS recent_arm,
         NOT EXISTS (SELECT 1 FROM public.daily_metrics_partitions p WHERE p.run_id = r.id) AS no_partition,
         (SELECT count(*) FROM public.daily_metrics_partitions p WHERE p.run_id = r.id AND p.status = 'failed') AS failed,
         (SELECT count(*) FROM public.daily_metrics_partitions p WHERE p.run_id = r.id AND p.status = 'failed_permanent') AS failed_permanent,
         (SELECT count(*) FROM public.daily_metrics_partitions p WHERE p.run_id = r.id) AS partitions
  FROM runs AS r
)
SELECT left(f.id::text, 8) AS run8, f.target_day, starts_with(f.org_id::text, 'f0f0f0f0') AS app_org,
       f.status_arm, (NOT f.status_arm AND f.recent_arm) AS recent_only, f.no_partition,
       (NOT f.status_arm AND NOT f.recent_arm) AS in_base,
       f.partitions, f.failed, f.failed_permanent
FROM facts AS f
ORDER BY f.target_day, run8
LIMIT 50;

-- S3 the stage base as a count, with the code's predicate (stale_finalize_repair.go:409-416, $1 = now() - 24 h).
-- WANT stage_base = 0 (as FORM-14090-S0-1: statement 0 false) and status_arm_runs = the S2 status_arm rows.
SELECT count(*) FILTER (WHERE NOT EXISTS (
           SELECT 1 FROM public.daily_metrics_partitions AS partition
           WHERE partition.run_id = run.id
             AND (partition.status <> 'succeeded' OR partition.updated_at > now() - interval '24 hours'))) AS stage_base,
       count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM public.daily_metrics_partitions AS partition
           WHERE partition.run_id = run.id AND partition.status <> 'succeeded')) AS status_arm_runs
FROM public.daily_metrics_runs AS run
WHERE run.status = 'running' AND run.finalization_status = 'pending' AND run.updated_at <= now() - interval '24 hours';

ROLLBACK;
