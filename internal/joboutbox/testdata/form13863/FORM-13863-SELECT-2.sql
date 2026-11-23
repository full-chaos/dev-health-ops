-- FORM-13863-SELECT-2 (pm-14090, for acp; -1 f4654664c8d5 is frozen, fixes per READ-FORM-13863-SELECT-1 394f31d9762b).
-- What the stale-finalize stage of ops #6824 (merged; code read at b0b99a5cc1fd) selects among the 'running' +
-- finalize-'pending' daily runs. Counts, 8-char run cuts and days only (no org id, no org8). Run on the primary:
--   psql -X -v ON_ERROR_STOP=1 -f FORM-13863-SELECT-2.sql      (no SET; pm keeps the session timeout)
-- NAMED EXCEPTION (acp ruling on this sha): all-org in S1-S3 (the stage base is all-org); output run8/day only, no org8.
--   The app-org prefix is used only in G0 (ORG-1) and as the app_org boolean of S2.
-- Stage base = internal/joboutbox/stale_finalize_repair.go:409-416 at b0b99a5cc1fd: status 'running' AND finalization_status
-- 'pending' AND run.updated_at <= cutoff AND NOT EXISTS (partition with status <> 'succeeded' OR updated_at > cutoff);
-- cutoff = now() - 24 h (StaleFinalizeHorizon, :28).
-- WORK MATH (caps; prod at SIZING-14090-2 2026-11-22: runs 49,009 rows / 18.9 MB, partitions 95,410 rows / 36.1 MB,
-- 9 running+pending runs past the horizon):
--   S1: one runs seq scan <= 200,000 rows.
--   S2: one runs seq scan <= 200,000 rows + per running+pending run 6 partition probes through (run_id, status); worst
--       case the 6 probes read every partition row 4 times: <= 200,000 + 4 x 500,000 = 2.2 M rows. Prod: ~49 k + <= 6 x ~9-20 runs.
--   S3: one runs seq scan + 2 probes per running+pending run: <= 200,000 + 2 x 500,000 = 1.2 M rows.
--   Every statement <= 20 M (TEMPLATE-3 budget). Bytes caps bound the heap read: runs 100 MB, partitions 200 MB.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (reads no data table). WANT cost_gate_ok = 0, else the named cast error aborts the transaction.
WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')),
rel AS (SELECT c.oid, c.relname, c.reltuples::bigint AS reltuples,
               (c.relpages::bigint + COALESCE(t.relpages::bigint, 0)) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('daily_metrics_runs', 'daily_metrics_partitions')),
idx AS (SELECT count(*) AS ok
        FROM pg_catalog.pg_class ic JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
        JOIN pg_catalog.pg_index i ON i.indexrelid = ic.oid
        JOIN rel ON rel.oid = i.indrelid AND rel.relname = 'daily_metrics_partitions'
        JOIN pg_catalog.pg_attribute a0 ON a0.attrelid = i.indrelid AND a0.attnum = i.indkey[0]
        JOIN pg_catalog.pg_attribute a1 ON a1.attrelid = i.indrelid AND a1.attnum = i.indkey[1]
        WHERE n.nspname = 'public' AND ic.relname = 'ix_daily_metrics_partition_run_status'
          AND i.indisvalid AND i.indisready AND a0.attname = 'run_id' AND a1.attname = 'status'),
facts AS (
  SELECT (SELECT n FROM orgs) AS orgs,
         (SELECT count(*) FROM rel) AS tables,
         (SELECT count(*) FROM rel WHERE reltuples < 0) AS never_analyzed,
         (SELECT reltuples FROM rel WHERE relname = 'daily_metrics_runs') AS runs,
         (SELECT bytes FROM rel WHERE relname = 'daily_metrics_runs') AS runs_bytes,
         (SELECT reltuples FROM rel WHERE relname = 'daily_metrics_partitions') AS partitions,
         (SELECT bytes FROM rel WHERE relname = 'daily_metrics_partitions') AS partitions_bytes,
         (SELECT ok FROM idx) AS index_ok)
SELECT CASE
         WHEN orgs = 1 AND tables = 2 AND never_analyzed = 0 AND index_ok = 1
          AND runs <= 200000 AND runs_bytes <= 104857600 AND partitions <= 500000 AND partitions_bytes <= 209715200
         THEN 0
         ELSE ('STOP-G0 orgs=' || COALESCE(orgs::text, 'null') || ' tables=' || COALESCE(tables::text, 'null')
               || ' unanalyzed=' || COALESCE(never_analyzed::text, 'null') || ' index_ok=' || COALESCE(index_ok::text, 'null')
               || ' runs=' || COALESCE(runs::text, 'null') || ' runs_bytes=' || COALESCE(runs_bytes::text, 'null')
               || ' partitions=' || COALESCE(partitions::text, 'null')
               || ' partitions_bytes=' || COALESCE(partitions_bytes::text, 'null'))::int
       END AS cost_gate_ok
FROM facts;

-- S1 control (can fail): the status literal matches real rows. WANT running_runs > 0, else STOP.
SELECT count(*) AS running_runs FROM public.daily_metrics_runs AS run WHERE run.status = 'running';

-- S2 per run: EVERY 'running' + finalization 'pending' run (no horizon filter), one row each, at most 50.
--   past_horizon = run.updated_at <= cutoff
--   status_arm   = a partition with status <> 'succeeded' (no horizon: this arm does not expire)
--   recent_only  = no such partition, but a partition written after the cutoff (expires within 24 h)
--   in_base      = the stage base holds the run NOW (past_horizon AND NOT status_arm AND NOT recent)
-- WANT (each can fail; any miss = STOP to acp with the row's run8 + target_day):
--   (1) EVERY row has status_arm = t (so recent_only = f, in_base = f, no_partition = f on every row);
--   (2) exactly 8 rows with app_org = t AND target_day 2026-09-17 .. 2026-11-14 AND failed >= 1 (the card's R1 shape, card-2
--       line 22); each of them status_arm = t.
-- INFO (a card fact for pm, not a hold WANT): failed_permanent per row (a failed_permanent partition is also <> 'succeeded').
WITH cutoff AS (SELECT now() - interval '24 hours' AS at),
runs AS (
  SELECT run.id, run.org_id, run.target_day, run.updated_at <= cutoff.at AS past_horizon
  FROM public.daily_metrics_runs AS run CROSS JOIN cutoff
  WHERE run.status = 'running' AND run.finalization_status = 'pending'
),
facts AS (
  SELECT r.id, r.org_id, r.target_day, r.past_horizon,
         EXISTS (SELECT 1 FROM public.daily_metrics_partitions p WHERE p.run_id = r.id AND p.status <> 'succeeded') AS status_arm,
         EXISTS (SELECT 1 FROM public.daily_metrics_partitions p CROSS JOIN cutoff
                 WHERE p.run_id = r.id AND p.updated_at > cutoff.at) AS recent_arm,
         NOT EXISTS (SELECT 1 FROM public.daily_metrics_partitions p WHERE p.run_id = r.id) AS no_partition,
         (SELECT count(*) FROM public.daily_metrics_partitions p WHERE p.run_id = r.id AND p.status = 'failed') AS failed,
         (SELECT count(*) FROM public.daily_metrics_partitions p WHERE p.run_id = r.id AND p.status = 'failed_permanent') AS failed_permanent,
         (SELECT count(*) FROM public.daily_metrics_partitions p WHERE p.run_id = r.id) AS partitions
  FROM runs AS r
)
SELECT left(f.id::text, 8) AS run8, f.target_day, starts_with(f.org_id::text, 'f0f0f0f0') AS app_org, f.past_horizon,
       f.status_arm, (NOT f.status_arm AND f.recent_arm) AS recent_only, f.no_partition,
       (f.past_horizon AND NOT f.status_arm AND NOT f.recent_arm) AS in_base,
       f.partitions, f.failed, f.failed_permanent
FROM facts AS f
ORDER BY f.target_day, run8
LIMIT 50;

-- S3 counts with the code's predicate (stale_finalize_repair.go:409-416, $1 = now() - 24 h).
-- WANT stage_base = 0; running_pending = the S2 row count (<= 50: S2 is not truncated); status_arm_runs = running_pending
-- (every running + pending run is refused by the status arm); running_pending_past_horizon = the S2 past_horizon rows.
SELECT count(*) AS running_pending,
       count(*) FILTER (WHERE run.updated_at <= now() - interval '24 hours') AS running_pending_past_horizon,
       count(*) FILTER (WHERE run.updated_at <= now() - interval '24 hours' AND NOT EXISTS (
           SELECT 1 FROM public.daily_metrics_partitions AS partition
           WHERE partition.run_id = run.id
             AND (partition.status <> 'succeeded' OR partition.updated_at > now() - interval '24 hours'))) AS stage_base,
       count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM public.daily_metrics_partitions AS partition
           WHERE partition.run_id = run.id AND partition.status <> 'succeeded')) AS status_arm_runs
FROM public.daily_metrics_runs AS run
WHERE run.status = 'running' AND run.finalization_status = 'pending';

ROLLBACK;
