-- FORM-14511-RECHECK-4 (pm-14090; -3 275fb0ee6cf0 frozen: its S3 passed a subquery as an EXECUTE parameter, which
-- PostgreSQL refuses (0A000 "cannot use subquery in EXECUTE parameter", exec leg job 137560514346). S3 now takes the run id
-- from S3a through psql's gset meta-command. Nothing else changed.) -3 was RECHECK-2 3512faad361f + bytes caps for runs and
-- partitions, null-safe STOP text, the outbox index tied to its table, work math for the new statement 0. Was RECHECK-2 = RECHECK-1 (c632e5c5cff1) with statement 0 replaced by its NEW text at ops
-- 863efd3a6e77 (#6850 head; sha 89794a9d433d); the re-check text is unchanged (sha 3e1fe377912e). Everything else
-- (G0, S2-S5, WANTs, work math) is RECHECK-1's; read that md for the reasons. Run AFTER the #6850 roll:
--   psql -X -v ON_ERROR_STOP=1 -c '\timing on' -f FORM-14511-RECHECK-4.sql
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (reads no data table). WANT cost_gate_ok = 0.
-- WORK MATH (caps; prod at SIZING-14090-2: runs 49,009 / partitions 95,410 / outbox 473,084 rows):
-- S1 (new statement 0) reads daily_metrics_partitions once in full for the NOT IN set (<= 500,000) + daily_metrics_runs until
-- the first base row (<= 200,000) = <= 700,000 rows;
-- S3/S4 each read at most 1 index probe + the heap rows of one run's finalize outbox rows (index present, valid, ready);
-- S2 sorts the app-org runs once (<= runs rows); S5 reads the partial index once (<= finalize rows).
-- Caps (prod S0 sizes from SIZING-14090-2 stmt0b x about 4): runs <= 200,000 rows / 100 MB; partitions <= 500,000 rows / 200 MB;
-- outbox <= 2,000,000 rows and <= 2 GiB heap+toast. Every statement <= 20 M rows (TEMPLATE-3). Needed indexes: ix_worker_job_outbox_finalize_domain_id and ix_daily_metrics_partition_run_status present,
-- valid, ready.
WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')),
rel AS (SELECT c.relname, c.reltuples::bigint AS reltuples,
               (c.relpages::bigint + COALESCE(t.relpages::bigint, 0)) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('daily_metrics_runs', 'daily_metrics_partitions', 'worker_job_outbox')),
idx AS (SELECT c.relname, i.indisvalid AND i.indisready AS ok
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
        JOIN pg_catalog.pg_class tbl ON tbl.oid = i.indrelid
        WHERE n.nspname = 'public' AND c.relkind = 'i'
          AND ((c.relname = 'ix_worker_job_outbox_finalize_domain_id' AND tbl.relname = 'worker_job_outbox')
            OR (c.relname = 'ix_daily_metrics_partition_run_status' AND tbl.relname = 'daily_metrics_partitions'))),
facts AS (
  SELECT (SELECT n FROM orgs) AS orgs,
         (SELECT count(*) FROM rel) AS tables,
         (SELECT count(*) FROM rel WHERE reltuples < 0) AS never_analyzed,
         (SELECT reltuples FROM rel WHERE relname = 'daily_metrics_runs') AS runs,
         (SELECT bytes FROM rel WHERE relname = 'daily_metrics_runs') AS runs_bytes,
         (SELECT reltuples FROM rel WHERE relname = 'daily_metrics_partitions') AS partitions,
         (SELECT bytes FROM rel WHERE relname = 'daily_metrics_partitions') AS partitions_bytes,
         (SELECT reltuples FROM rel WHERE relname = 'worker_job_outbox') AS outbox,
         (SELECT bytes FROM rel WHERE relname = 'worker_job_outbox') AS outbox_bytes,
         (SELECT count(*) FROM idx WHERE ok) AS indexes_ok)
SELECT CASE
         WHEN orgs = 1 AND tables = 3 AND never_analyzed = 0 AND indexes_ok = 2
          AND runs <= 200000 AND runs_bytes <= 104857600 AND partitions <= 500000 AND partitions_bytes <= 209715200
          AND outbox <= 2000000 AND outbox_bytes <= 2147483648
         THEN 0
         ELSE ('STOP-G0 orgs=' || COALESCE(orgs::text, 'null') || ' tables=' || COALESCE(tables::text, 'null')
               || ' unanalyzed=' || COALESCE(never_analyzed::text, 'null') || ' indexes_ok=' || COALESCE(indexes_ok::text, 'null')
               || ' runs=' || COALESCE(runs::text, 'null') || ' runs_bytes=' || COALESCE(runs_bytes::text, 'null')
               || ' partitions=' || COALESCE(partitions::text, 'null') || ' partitions_bytes=' || COALESCE(partitions_bytes::text, 'null')
               || ' outbox=' || COALESCE(outbox::text, 'null') || ' outbox_bytes=' || COALESCE(outbox_bytes::text, 'null'))::int
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
PREPARE stale_finalize_recheck(text, timestamptz) AS
SELECT EXISTS (
	SELECT 1 FROM public.worker_job_outbox AS outbox
	WHERE outbox.job_kind = 'metrics.daily_finalize'
		AND outbox.args #>> '{domain,id}' = $1
		AND (outbox.status IN ('pending', 'claimed') OR outbox.created_at > $2)
);

-- S1 statement 0, cutoff = now() - 24 h (one now() per REPEATABLE READ transaction). WANT time <= 75 ms (half its 150 ms timeout);
-- exists = INFO.
EXECUTE stale_finalize_statement0(now() - interval '24 hours');

-- S2 control (can fail): the app org has a daily run to re-check. WANT runs > 0 (else S3 has no real run and STOP: report it).
SELECT count(*) AS app_org_runs FROM public.daily_metrics_runs AS run WHERE starts_with(run.org_id::text, 'f0f0f0f0');

-- S3a the newest app-org run id = INFO (reads daily_metrics_runs once, <= runs cap). psql keeps it as the variable
-- recheck_run (gset on the next line). No row = a psql error, and ON_ERROR_STOP stops the file (S2 already said why).
SELECT run.id::text AS recheck_run FROM public.daily_metrics_runs AS run
WHERE starts_with(run.org_id::text, 'f0f0f0f0') ORDER BY run.updated_at DESC, run.id DESC LIMIT 1
\gset

-- S3 re-check of that run (it has finalize outbox rows: the index stops at the first match). WANT time <= 100 ms (half the
-- 200 ms per-run deadline it runs under); the boolean = INFO.
EXECUTE stale_finalize_recheck(:'recheck_run', now() - interval '24 hours');

-- S4 re-check, an id no outbox row holds (the miss: one full index probe, the worst case of the candidate path).
-- WANT exists = false AND time <= 100 ms.
EXECUTE stale_finalize_recheck('00000000-0000-4000-8000-000000000000', now() - interval '24 hours');

-- S5 control (can fail): the partial index predicate literal matches real rows. WANT finalize_rows > 0 (else the index and
-- the re-check read nothing and S3/S4 prove nothing: STOP, report). Reads the partial index only (<= finalize rows).
SELECT count(*) AS finalize_rows FROM public.worker_job_outbox AS outbox WHERE outbox.job_kind = 'metrics.daily_finalize';

DEALLOCATE stale_finalize_statement0;
DEALLOCATE stale_finalize_recheck;
ROLLBACK;
