-- FORM-14542-2-PG (CHAOS-14542): map the unit cuts of the FORM-14542-2 SigNoz out to sync_run_units.dataset_key.
-- READ ONLY. Pin ops 3eb714a54ffb. Md: FORM-14542-2.md (section PG FORM). The lane runs nothing; pm runs it once.
-- Run line (primary; the file is not edited, the input is a psql variable that psql quotes as one literal):
--   PGOPTIONS='-c statement_timeout=30s' psql -X -v ON_ERROR_STOP=1 -v u8s='{<cut1>,<cut2>,...}' -f FORM-14542-2-PG.sql
-- u8s = every `unit` label of the FORM-14542-2 out (8 lowercase hex chars each), in a Postgres text[] literal.
-- Join: job.execute dev_health.domain.id (jobruntime/adapter.go:722) = Payload.UnitID (jobs/providerunit/providerunit.go:1137-1139)
--   = sync_run_units.id (claimUnitSQL, providersync/repository_postgres.go:1074). dataset_key: baseline/head.json sync_run_units.
-- Output: 8-char cuts, dataset/provider/class/mode words, counts. No full id, no payload, no error text.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (reads no data table but organizations by its pkey prefix). WANT cost_gate_ok = 0.
WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, 'c6a38355')),
rel AS (SELECT c.relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size AS heap_bytes,
               COALESCE(t.relpages::bigint, 0) * bs.block_size AS toast_bytes
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname = 'sync_run_units'),
pk AS (SELECT count(*) AS n FROM pg_catalog.pg_index i
       JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
       JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
       WHERE n.nspname = 'public' AND ic.relname = 'sync_run_units_pkey' AND i.indisvalid AND i.indisready AND i.indisprimary)
SELECT CASE WHEN orgs.n = 1 AND (SELECT count(*) FROM rel) = 1
                 AND (SELECT min(reltuples) FROM rel) BETWEEN 0 AND 1000000
                 AND (SELECT max(heap_bytes + toast_bytes) FROM rel) <= 1073741824
                 AND pk.n = 1
            THEN 0
            ELSE ('STOP-G0 orgs=' || orgs.n || ' rel=' || (SELECT count(*) FROM rel)
                  || ' reltuples=' || COALESCE((SELECT min(reltuples) FROM rel)::text, 'none')
                  || ' bytes=' || COALESCE((SELECT max(heap_bytes + toast_bytes) FROM rel)::text, 'none')
                  || ' pk=' || pk.n)::int
       END AS cost_gate_ok
FROM orgs CROSS JOIN pk;

-- stmt0: WANT orgs = 1.
SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'c6a38355');

-- G1 input gate (reads only the psql variable). WANT input_ok = 0: 1..500 cuts, each ^[0-9a-f]{8}$, no repeat.
SELECT CASE WHEN count(*) BETWEEN 1 AND 500
                 AND count(*) FILTER (WHERE u8 IS NULL OR u8 !~ '^[0-9a-f]{8}$') = 0
                 AND count(DISTINCT u8) = count(*)
            THEN 0
            ELSE ('STOP-G1 cuts=' || count(*) || ' bad=' || count(*) FILTER (WHERE u8 IS NULL OR u8 !~ '^[0-9a-f]{8}$')
                  || ' distinct=' || count(DISTINCT u8))::int
       END AS input_ok
FROM unnest(:'u8s'::text[]) AS input(u8);

-- S1 map: one row per cut. Each cut reads at most 2 rows by a pkey range (ORDER BY id LIMIT 2).
-- WANT rows = the cut count; matched = 1 per cut (0 = not this org or gone; 2 = ambiguous cut); provider github.
SELECT input.u8,
       count(u.id) AS matched,
       COALESCE(string_agg(DISTINCT lower(btrim(u.provider)), ','), '') AS provider,
       COALESCE(string_agg(DISTINCT lower(btrim(u.dataset_key)), ','), '') AS dataset_key,
       COALESCE(string_agg(DISTINCT lower(btrim(u.cost_class)), ','), '') AS cost_class,
       COALESCE(string_agg(DISTINCT lower(btrim(u.mode)), ','), '') AS mode
FROM unnest(:'u8s'::text[]) AS input(u8)
LEFT JOIN LATERAL (
    SELECT s.id, s.provider, s.dataset_key, s.cost_class, s.mode
    FROM public.sync_run_units AS s
    WHERE s.id >= (input.u8 || '-0000-0000-0000-000000000000')::uuid
      AND s.id <= (input.u8 || '-ffff-ffff-ffff-ffffffffffff')::uuid
      AND starts_with(s.org_id, 'c6a38355')
    ORDER BY s.id
    LIMIT 2
) AS u ON true
GROUP BY input.u8
ORDER BY input.u8;

-- S2 control + summary over the same cuts. WANT cuts = the cut count (> 0); cuts_matched_1 + cuts_unmatched + cuts_ambiguous = cuts.
WITH m AS (
    SELECT input.u8, count(u.id) AS matched,
           min(lower(btrim(u.dataset_key))) AS dataset_key, min(lower(btrim(u.cost_class))) AS cost_class
    FROM unnest(:'u8s'::text[]) AS input(u8)
    LEFT JOIN LATERAL (
        SELECT s.id, s.dataset_key, s.cost_class
        FROM public.sync_run_units AS s
        WHERE s.id >= (input.u8 || '-0000-0000-0000-000000000000')::uuid
          AND s.id <= (input.u8 || '-ffff-ffff-ffff-ffffffffffff')::uuid
          AND starts_with(s.org_id, 'c6a38355')
        ORDER BY s.id
        LIMIT 2
    ) AS u ON true
    GROUP BY input.u8
)
SELECT count(*) AS cuts,
       count(*) FILTER (WHERE matched = 1) AS cuts_matched_1,
       count(*) FILTER (WHERE matched = 0) AS cuts_unmatched,
       count(*) FILTER (WHERE matched >= 2) AS cuts_ambiguous,
       count(*) FILTER (WHERE matched = 1 AND dataset_key = 'cicd') AS cicd,
       count(*) FILTER (WHERE matched = 1 AND dataset_key = 'tests') AS tests,
       count(*) FILTER (WHERE matched = 1 AND dataset_key NOT IN ('cicd', 'tests')) AS other_dataset,
       count(*) FILTER (WHERE matched = 1 AND dataset_key = 'cicd' AND cost_class <> 'medium') AS cicd_not_medium,
       count(*) FILTER (WHERE matched = 1 AND dataset_key = 'tests' AND cost_class <> 'heavy') AS tests_not_heavy
FROM m;

ROLLBACK;
