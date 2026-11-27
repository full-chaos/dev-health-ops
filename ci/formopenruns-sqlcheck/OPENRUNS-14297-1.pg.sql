-- OPENRUNS-14297-1.pg (acp-train43, 2026-11-27; CHAOS-14297 wave 2, driver W2-DRIVER-7): the org's OPEN backfill runs, ANY prefix. Read by
-- the driver after a START with an unknown outcome (rc != 0 or no run prefix printed): it WAITs until stmt1 open_runs = 0, then writes
-- RETRY kind=start. Form = FORM-TEMPLATE-5 (b) (7fb32b16d35d): one transaction, REPEATABLE READ READ ONLY .. ROLLBACK, no SET, a G0
-- catalog gate that reads no data table, the org prefix on every FROM with org_id, counts / 8-char prefixes only.
-- Placeholders: {{ORG_PREFIX}} (8 chars; the driver's psql_read fill). {{RUN_PREFIX}} is not used. Open = status NOT IN ('success',
-- 'partial_failed', 'failed') (RESYNC-UNITS-14297-4 stmt1); units open = status NOT IN ('success', 'failed') (its stmt2).
-- A failed gate is a named cast error: psql rc != 0 = the driver's read failure = a WAIT (never read as 0 open runs).
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (reads no data table): organizations ORG-1 = 1 (the template's orgs CTE), the 2 tables found, sync_runs heap + toast
-- <= 1 GiB (stmt00 / stmt1 scan it by the org prefix), the (sync_run_id, status) index valid (stmt1 reads units of the open runs only).
-- WANT cost_gate_ok = 0.
WITH orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, '{{ORG_PREFIX}}')),
rel AS (SELECT c.relname,
               pg_relation_size(c.oid)::bigint
               + CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_relation_size(c.reltoastrelid)::bigint END AS bytes
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname IN ('sync_runs', 'sync_run_units')),
ix AS (SELECT count(*) AS n FROM pg_catalog.pg_index i
       JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = 'public' AND c.relname = 'ix_sync_run_units_run_status' AND i.indisvalid)
SELECT CASE WHEN (SELECT n FROM orgs) = 1
             AND (SELECT count(*) FROM rel) = 2
             AND coalesce((SELECT bytes FROM rel WHERE relname = 'sync_runs'), -1) BETWEEN 0 AND 1073741824::bigint
             AND (SELECT n FROM ix) = 1
            THEN 0
            ELSE ('STOP-G0 orgs=' || (SELECT n FROM orgs) || ' rels=' || (SELECT count(*) FROM rel)
                  || ' runs_bytes=' || coalesce((SELECT bytes FROM rel WHERE relname = 'sync_runs'), -1) || ' ix=' || (SELECT n FROM ix))::int
       END AS cost_gate_ok;

-- stmt00 ORG-1 on the read table. WANT 1.
SELECT count(DISTINCT org_id) AS orgs FROM public.sync_runs WHERE starts_with(org_id::text, '{{ORG_PREFIX}}');

-- stmt1 the org's open backfill runs (any prefix), capped at 1000 (a bigger set = STOP-S1, never a partial count); open_units = the
-- open units of those runs (the run_status index); open_prefixes = the first 20 run prefixes (8 chars), '-' for none.
-- WANT (driver): open_runs = 0 ends the WAIT; open_runs > 0 = WAIT, the WAIT line names open_prefixes.
WITH openr AS MATERIALIZED (
  SELECT r.id, r.org_id FROM public.sync_runs AS r
  WHERE starts_with(r.org_id::text, '{{ORG_PREFIX}}') AND r.triggered_by = 'backfill'
    AND r.status NOT IN ('success', 'partial_failed', 'failed')
  LIMIT 1001)
SELECT CASE WHEN (SELECT count(*) FROM openr) <= 1000 THEN (SELECT count(*) FROM openr)
            ELSE ('STOP-S1 open_runs>1000')::int END AS open_runs,
       (SELECT count(*) FROM public.sync_run_units AS u JOIN openr AS o ON u.sync_run_id = o.id AND u.org_id = o.org_id
        WHERE starts_with(u.org_id::text, '{{ORG_PREFIX}}') AND u.status NOT IN ('success', 'failed')) AS open_units,
       coalesce((SELECT string_agg(left(z.id::text, 8), ',' ORDER BY z.id) FROM (SELECT id FROM openr ORDER BY id LIMIT 20) AS z), '-')
                                                                                                    AS open_prefixes;

ROLLBACK;
