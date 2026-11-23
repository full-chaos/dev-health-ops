-- FORM-14090-S0-3 (pm-14090; -2 09fd95461514 frozen: bytes caps, null-safe STOP text, work math): prod timing of the NEW statement 0 of ops #6850 (CHAOS-14511) before its roll.
-- Statement 0 = internal/joboutbox/stale_finalize_repair.go staleFinalizeBaseExistsSQL at ops 863efd3a6e77 (#6850 head), byte-equal
-- through PREPARE (sha256 of the text from the leading newline to the last ')' = 89794a9d433d). It changed from FORM-14090-S0-1's text
-- (f3d3a8ff9aa0): the base's partition check is now an uncorrelated `run.id NOT IN (...)` set (read once per statement).
-- Run: psql -X -v ON_ERROR_STOP=1 -c '\timing on' -f FORM-14090-S0-2.sql   (no SET in the file; pm keeps the session timeout)
-- Named exception (as S0-1): statement 0 is all-org in production; the output has no id. Counts, booleans, times only.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (reads no data table). WANT cost_gate_ok = 0.
-- WORK MATH (caps; prod at SIZING-14090-2 2026-11-22: runs 49,009 rows / 18.9 MB, partitions 95,410 rows / 36.1 MB):
--   S1, each of the 3 executions: one full read of daily_metrics_partitions for the NOT IN set (<= 500,000 rows, hashed) +
--     daily_metrics_runs until the first base row (all <= 200,000 rows when the base is empty, as today) = <= 700,000 rows.
--   S2: one runs seq scan (<= 200,000) + per running+pending run past the horizon one partition probe through
--     (run_id, status) (<= 500,000 rows in all) = <= 700,000 rows.
--   Every statement <= 20 M (TEMPLATE-3). Bytes caps bound the heap read: runs 100 MB, partitions 200 MB.
WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')),
rel AS (SELECT c.relname, c.reltuples::bigint AS reltuples,
               (c.relpages::bigint + COALESCE(t.relpages::bigint, 0)) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('daily_metrics_runs', 'daily_metrics_partitions')),
facts AS (
  SELECT (SELECT n FROM orgs) AS orgs,
         (SELECT count(*) FROM rel) AS tables,
         (SELECT count(*) FROM rel WHERE reltuples < 0) AS never_analyzed,
         (SELECT reltuples FROM rel WHERE relname = 'daily_metrics_runs') AS runs,
         (SELECT bytes FROM rel WHERE relname = 'daily_metrics_runs') AS runs_bytes,
         (SELECT reltuples FROM rel WHERE relname = 'daily_metrics_partitions') AS partitions,
         (SELECT bytes FROM rel WHERE relname = 'daily_metrics_partitions') AS partitions_bytes)
SELECT CASE
         WHEN orgs = 1 AND tables = 2 AND never_analyzed = 0
          AND runs <= 200000 AND runs_bytes <= 104857600 AND partitions <= 500000 AND partitions_bytes <= 209715200
         THEN 0
         ELSE ('STOP-G0 orgs=' || COALESCE(orgs::text, 'null') || ' tables=' || COALESCE(tables::text, 'null')
               || ' unanalyzed=' || COALESCE(never_analyzed::text, 'null') || ' runs=' || COALESCE(runs::text, 'null')
               || ' runs_bytes=' || COALESCE(runs_bytes::text, 'null') || ' partitions=' || COALESCE(partitions::text, 'null')
               || ' partitions_bytes=' || COALESCE(partitions_bytes::text, 'null'))::int
       END AS cost_gate_ok
FROM facts;

PREPARE stale_finalize_statement0(timestamptz) AS
SELECT EXISTS (SELECT 1 FROM public.daily_metrics_runs AS run WHERE run.status = 'running'
	AND run.finalization_status = 'pending'
	AND run.updated_at <= $1
	AND run.id NOT IN (
		SELECT partition.run_id FROM public.daily_metrics_partitions AS partition
		WHERE (partition.status <> 'succeeded' OR partition.updated_at > $1)
			AND partition.run_id IS NOT NULL
	));

-- S1 statement 0 three times, cutoff = now() - 24 h. WANT the first time <= 75 ms (half its 150 ms timeout); > 75 ms = STOP:
-- report the three times. exists = INFO (today false: FORM-14090-S0-1).
EXECUTE stale_finalize_statement0(now() - interval '24 hours');
EXECUTE stale_finalize_statement0(now() - interval '24 hours');
EXECUTE stale_finalize_statement0(now() - interval '24 hours');

-- S2 control (can fail): the same base counted with the OLD correlated partition check (FORM-14090-S0-1's form) and the
-- literals matched to real rows. WANT runs_total > 0; (base_rows > 0) = the S1 exists value, else STOP (the two forms disagree).
SELECT count(*) AS runs_total,
       count(*) FILTER (WHERE run.status = 'running' AND run.finalization_status = 'pending') AS running_pending,
       count(*) FILTER (WHERE run.status = 'running'
           AND run.finalization_status = 'pending'
           AND run.updated_at <= now() - interval '24 hours'
           AND NOT EXISTS (
               SELECT 1 FROM public.daily_metrics_partitions AS partition
               WHERE partition.run_id = run.id
                   AND (partition.status <> 'succeeded' OR partition.updated_at > now() - interval '24 hours')
           )) AS base_rows
FROM public.daily_metrics_runs AS run;

DEALLOCATE stale_finalize_statement0;
ROLLBACK;
