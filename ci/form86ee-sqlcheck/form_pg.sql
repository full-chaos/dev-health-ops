-- FORM-W2-86EE-ERRCLASS-2: why did sync run <r1> (wave-2 row W11-EFB-1118, github backfill) fail; what did its re-run <r2> do.
-- READ ONLY. Pin ops a7febdf50002. Md: FORM-W2-86EE-ERRCLASS-2.md. The lane runs nothing against prod; pm runs it once after an acp read.
-- Run line (primary; the two run prefixes are psql variables, each quoted by psql as one literal; the file is not edited; the exact kubectl exec line, PGOPTIONS inside the pod sh -c, is in the md):
--   PGOPTIONS='-c statement_timeout=30s' psql -X -v ON_ERROR_STOP=1 -v r1=<8 hex> -v r2=<8 hex> -f FORM-W2-86EE-ERRCLASS-2.sql
-- Output: run status + counters, unit counts by status x error code x error category x cause class x dataset_key, failed-unit rows.
-- Codes are shape-masked (one literal 'sync unit lease expired' is named) to ^[a-z][a-z0-9_]{0,63}$ else '(non-code)'; ids are left(id::text, 8); no error text, no payload, no URL.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (reads no data table but organizations). WANT cost_gate_ok = 0.
WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
rel AS (SELECT c.relname, c.reltuples::bigint AS reltuples,
                   c.relpages::bigint * bs.block_size AS heap_bytes,
                   COALESCE(t.relpages::bigint, 0) * bs.block_size AS toast_bytes
            FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            CROSS JOIN bs LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
            WHERE n.nspname = 'public' AND c.relkind = 'r'
              AND c.relname IN ('organizations', 'sync_runs', 'sync_run_units')),
    cap(relname, max_tuples, max_bytes) AS (VALUES
        ('organizations', 100000::bigint, 134217728::bigint),
        ('sync_runs', 1000000::bigint, 1073741824::bigint),
        ('sync_run_units', 1000000::bigint, 2147483648::bigint)),
    relok AS (SELECT count(*) AS n,
                     count(*) FILTER (WHERE rel.reltuples BETWEEN 0 AND cap.max_tuples
                                        AND rel.heap_bytes + rel.toast_bytes <= cap.max_bytes) AS ok,
                     COALESCE(string_agg(rel.relname || ':' || rel.reltuples || ':' || (rel.heap_bytes + rel.toast_bytes), ',' ORDER BY rel.relname), '') AS txt
              FROM rel JOIN cap ON cap.relname = rel.relname),
    ix AS (SELECT count(*) AS n FROM pg_catalog.pg_index i
           JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
           JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
           WHERE n.nspname = 'public' AND i.indisvalid AND i.indisready
             AND ic.relname IN ('sync_runs_pkey', 'ix_sync_run_units_run_status')),
    orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0'))
SELECT CASE WHEN orgs.n = 1 AND relok.n = 3 AND relok.ok = 3 AND ix.n = 2
            THEN 0
            ELSE ('STOP-G0 orgs=' || orgs.n || ' rels=' || relok.n || ' relok=' || relok.ok
                  || ' ix=' || ix.n || ' ' || relok.txt)::int
       END AS cost_gate_ok
FROM orgs CROSS JOIN relok CROSS JOIN ix;

-- stmt0 ORG-1: WANT orgs = 1.
SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0');

-- G1 input gate (reads only the psql variables). WANT input_ok = 0: two prefixes, each ^[0-9a-f]{8}$, different.
SELECT CASE WHEN :'r1' ~ '^[0-9a-f]{8}$' AND :'r2' ~ '^[0-9a-f]{8}$' AND :'r1' <> :'r2'
            THEN 0
            ELSE ('STOP-G1 r1_ok=' || (:'r1' ~ '^[0-9a-f]{8}$') || ' r2_ok=' || (:'r2' ~ '^[0-9a-f]{8}$')
                  || ' distinct=' || (:'r1' <> :'r2'))::int
       END AS input_ok;

-- @@S1 runs: status + counters + live unit tally (control). WANT 2 rows (1 per prefix); unit_rows > 0; total_ok / completed_ok / failed_ok true.
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (SELECT c.relname, c.reltuples::bigint AS reltuples,
                   c.relpages::bigint * bs.block_size AS heap_bytes,
                   COALESCE(t.relpages::bigint, 0) * bs.block_size AS toast_bytes
            FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            CROSS JOIN bs LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
            WHERE n.nspname = 'public' AND c.relkind = 'r'
              AND c.relname IN ('organizations', 'sync_runs', 'sync_run_units')),
    cap(relname, max_tuples, max_bytes) AS (VALUES
        ('organizations', 100000::bigint, 134217728::bigint),
        ('sync_runs', 1000000::bigint, 1073741824::bigint),
        ('sync_run_units', 1000000::bigint, 2147483648::bigint)),
    relok AS (SELECT count(*) AS n,
                     count(*) FILTER (WHERE rel.reltuples BETWEEN 0 AND cap.max_tuples
                                        AND rel.heap_bytes + rel.toast_bytes <= cap.max_bytes) AS ok,
                     COALESCE(string_agg(rel.relname || ':' || rel.reltuples || ':' || (rel.heap_bytes + rel.toast_bytes), ',' ORDER BY rel.relname), '') AS txt
              FROM rel JOIN cap ON cap.relname = rel.relname),
    ix AS (SELECT count(*) AS n FROM pg_catalog.pg_index i
           JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
           JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
           WHERE n.nspname = 'public' AND i.indisvalid AND i.indisready
             AND ic.relname IN ('sync_runs_pkey', 'ix_sync_run_units_run_status')),
    orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0'))
    SELECT CASE WHEN orgs.n = 1 AND relok.n = 3 AND relok.ok = 3 AND ix.n = 2
                THEN 0
                ELSE ('STOP-S1-G0 orgs=' || orgs.n || ' rels=' || relok.n || ' relok=' || relok.ok
                      || ' ix=' || ix.n || ' ' || relok.txt)::int
           END AS ok
    FROM orgs CROSS JOIN relok CROSS JOIN ix
),
runs AS MATERIALIZED (
    SELECT p.pfx, s.id, s.status, s.mode, s.total_units, s.completed_units, s.failed_units,
           s.created_at, s.started_at, s.completed_at, s.error,
           count(s.id) OVER (PARTITION BY p.pfx) AS matched
    FROM (VALUES (:'r1'), (:'r2')) AS p(pfx)
    CROSS JOIN gate g
    LEFT JOIN LATERAL (
        SELECT r.id, r.status, r.mode, r.total_units, r.completed_units, r.failed_units,
               r.created_at, r.started_at, r.completed_at, r.error
        FROM public.sync_runs AS r
        WHERE g.ok = 0
          AND r.id >= (p.pfx || '-0000-0000-0000-000000000000')::uuid
          AND r.id <= (p.pfx || '-ffff-ffff-ffff-ffffffffffff')::uuid
          AND starts_with(r.org_id, 'f0f0f0f0')
        ORDER BY r.id
        LIMIT 2
    ) AS s ON true
),
uc AS MATERIALIZED (
    SELECT count(*) AS n FROM (
        SELECT 1 FROM public.sync_run_units AS u CROSS JOIN gate g
        WHERE g.ok = 0
          AND u.sync_run_id IN (SELECT id FROM runs WHERE id IS NOT NULL)
          AND starts_with(u.org_id, 'f0f0f0f0')
        LIMIT 20001
    ) AS capped
),
gate2 AS MATERIALIZED (
    SELECT CASE WHEN (SELECT count(DISTINCT pfx) FROM runs WHERE matched = 1) = 2
                 AND uc.n BETWEEN 1 AND 20000
                THEN 0
                ELSE ('STOP-S1-G2 runs_matched_1=' || (SELECT count(DISTINCT pfx) FROM runs WHERE matched = 1)
                      || ' runs_ambiguous=' || (SELECT count(DISTINCT pfx) FROM runs WHERE matched >= 2)
                      || ' unit_rows_capped=' || uc.n)::int
           END AS ok
    FROM uc
)
SELECT r.pfx AS run_prefix, left(r.id::text, 8) AS run8, CASE WHEN r.status IS NULL THEN '' WHEN r.status ~ '^[a-z][a-z0-9_]{0,63}$' THEN r.status ELSE '(non-code)' END AS run_status, CASE WHEN r.mode IS NULL THEN '' WHEN r.mode ~ '^[a-z][a-z0-9_]{0,63}$' THEN r.mode ELSE '(non-code)' END AS run_mode,
       r.total_units, r.completed_units, r.failed_units,
       CASE WHEN isfinite(r.created_at) THEN to_char(r.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI"Z"') END AS created_min, CASE WHEN isfinite(r.started_at) THEN to_char(r.started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI"Z"') END AS started_min, CASE WHEN isfinite(r.completed_at) THEN to_char(r.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI"Z"') END AS completed_min, CASE WHEN r.error IS NULL THEN '' WHEN r.error ~ '^[a-z][a-z0-9_]{0,63}$' THEN r.error ELSE '(non-code)' END AS run_error_code,
       t.unit_rows, t.success_rows, t.failed_rows, t.open_rows, t.bad_json_rows,
       (t.unit_rows = r.total_units) AS total_ok,
       (t.success_rows = r.completed_units) AS completed_ok,
       (t.failed_rows = r.failed_units) AS failed_ok
FROM runs AS r
CROSS JOIN gate2 AS g2
LEFT JOIN LATERAL (
    SELECT count(*) AS unit_rows,
           count(*) FILTER (WHERE u.status = 'success') AS success_rows,
           count(*) FILTER (WHERE u.status = 'failed') AS failed_rows,
           count(*) FILTER (WHERE u.status NOT IN ('success', 'failed')) AS open_rows,
           count(*) FILTER (WHERE u.result IS NOT NULL AND NOT pg_input_is_valid(u.result::text, 'jsonb')) AS bad_json_rows
    FROM public.sync_run_units AS u
    WHERE g2.ok = 0 AND u.sync_run_id = r.id AND starts_with(u.org_id, 'f0f0f0f0')
) AS t ON true
WHERE r.matched = 1
ORDER BY r.pfx;

-- @@S2 units grouped by status x error code x error category x cause class x dataset_key. WANT sum of n per run_prefix = run_total = S1 unit_rows; rows < 500.
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (SELECT c.relname, c.reltuples::bigint AS reltuples,
                   c.relpages::bigint * bs.block_size AS heap_bytes,
                   COALESCE(t.relpages::bigint, 0) * bs.block_size AS toast_bytes
            FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            CROSS JOIN bs LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
            WHERE n.nspname = 'public' AND c.relkind = 'r'
              AND c.relname IN ('organizations', 'sync_runs', 'sync_run_units')),
    cap(relname, max_tuples, max_bytes) AS (VALUES
        ('organizations', 100000::bigint, 134217728::bigint),
        ('sync_runs', 1000000::bigint, 1073741824::bigint),
        ('sync_run_units', 1000000::bigint, 2147483648::bigint)),
    relok AS (SELECT count(*) AS n,
                     count(*) FILTER (WHERE rel.reltuples BETWEEN 0 AND cap.max_tuples
                                        AND rel.heap_bytes + rel.toast_bytes <= cap.max_bytes) AS ok,
                     COALESCE(string_agg(rel.relname || ':' || rel.reltuples || ':' || (rel.heap_bytes + rel.toast_bytes), ',' ORDER BY rel.relname), '') AS txt
              FROM rel JOIN cap ON cap.relname = rel.relname),
    ix AS (SELECT count(*) AS n FROM pg_catalog.pg_index i
           JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
           JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
           WHERE n.nspname = 'public' AND i.indisvalid AND i.indisready
             AND ic.relname IN ('sync_runs_pkey', 'ix_sync_run_units_run_status')),
    orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0'))
    SELECT CASE WHEN orgs.n = 1 AND relok.n = 3 AND relok.ok = 3 AND ix.n = 2
                THEN 0
                ELSE ('STOP-S2-G0 orgs=' || orgs.n || ' rels=' || relok.n || ' relok=' || relok.ok
                      || ' ix=' || ix.n || ' ' || relok.txt)::int
           END AS ok
    FROM orgs CROSS JOIN relok CROSS JOIN ix
),
runs AS MATERIALIZED (
    SELECT p.pfx, s.id, s.status, s.mode, s.total_units, s.completed_units, s.failed_units,
           s.created_at, s.started_at, s.completed_at, s.error,
           count(s.id) OVER (PARTITION BY p.pfx) AS matched
    FROM (VALUES (:'r1'), (:'r2')) AS p(pfx)
    CROSS JOIN gate g
    LEFT JOIN LATERAL (
        SELECT r.id, r.status, r.mode, r.total_units, r.completed_units, r.failed_units,
               r.created_at, r.started_at, r.completed_at, r.error
        FROM public.sync_runs AS r
        WHERE g.ok = 0
          AND r.id >= (p.pfx || '-0000-0000-0000-000000000000')::uuid
          AND r.id <= (p.pfx || '-ffff-ffff-ffff-ffffffffffff')::uuid
          AND starts_with(r.org_id, 'f0f0f0f0')
        ORDER BY r.id
        LIMIT 2
    ) AS s ON true
),
uc AS MATERIALIZED (
    SELECT count(*) AS n FROM (
        SELECT 1 FROM public.sync_run_units AS u CROSS JOIN gate g
        WHERE g.ok = 0
          AND u.sync_run_id IN (SELECT id FROM runs WHERE id IS NOT NULL)
          AND starts_with(u.org_id, 'f0f0f0f0')
        LIMIT 20001
    ) AS capped
),
gate2 AS MATERIALIZED (
    SELECT CASE WHEN (SELECT count(DISTINCT pfx) FROM runs WHERE matched = 1) = 2
                 AND uc.n BETWEEN 1 AND 20000
                THEN 0
                ELSE ('STOP-S2-G2 runs_matched_1=' || (SELECT count(DISTINCT pfx) FROM runs WHERE matched = 1)
                      || ' runs_ambiguous=' || (SELECT count(DISTINCT pfx) FROM runs WHERE matched >= 2)
                      || ' unit_rows_capped=' || uc.n)::int
           END AS ok
    FROM uc
)
SELECT x.run_prefix, x.unit_status, x.error_code, x.error_category, x.cause_class, x.retry_reason, x.dataset_key,
       count(*) AS n,
       min(x.since_day) AS since_min_day, max(x.before_day) AS before_max_day,
       min(x.span_days) AS span_min_days, max(x.span_days) AS span_max_days, max(x.attempts) AS attempts_max,
       count(*) FILTER (WHERE x.bad_json) AS bad_json,
       count(*) FILTER (WHERE x.lease_held) AS leased,
       count(*) FILTER (WHERE x.lease_past) AS lease_past,
       max(x.hb_age_min) AS hb_age_max_min,
       max(x.expired_lease_retry_count) AS expired_lease_retry_max,
       max(x.rate_limit_deferrals) AS rate_limit_deferrals_max,
       max(x.budget_deferrals) AS budget_deferrals_max,
       count(*) FILTER (WHERE x.exhausted) AS retry_exhausted,
       sum(count(*)) OVER (PARTITION BY x.run_prefix) AS run_total
FROM (
    SELECT r.pfx AS run_prefix, CASE WHEN u.status IS NULL THEN '' WHEN u.status ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.status ELSE '(non-code)' END AS unit_status, CASE WHEN u.error = 'sync unit lease expired' THEN 'sync_unit_lease_expired' ELSE CASE WHEN u.error IS NULL THEN '' WHEN u.error ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.error ELSE '(non-code)' END END AS error_code,
           CASE WHEN u.rj->>'error_category' IS NULL THEN '' WHEN u.rj->>'error_category' ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.rj->>'error_category' ELSE '(non-code)' END AS error_category, CASE WHEN u.rj->>'cause_class' IS NULL THEN '' WHEN u.rj->>'cause_class' ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.rj->>'cause_class' ELSE '(non-code)' END AS cause_class, CASE WHEN u.last_retry_reason IS NULL THEN '' WHEN u.last_retry_reason ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.last_retry_reason ELSE '(non-code)' END AS retry_reason, CASE WHEN u.dataset_key IS NULL THEN '' WHEN u.dataset_key ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.dataset_key ELSE '(non-code)' END AS dataset_key,
           CASE WHEN isfinite(u.since_at) THEN to_char(u.since_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') END AS since_day, CASE WHEN isfinite(u.before_at) THEN to_char(u.before_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') END AS before_day,
           CASE WHEN isfinite(u.since_at) AND isfinite(u.before_at) THEN ((u.before_at AT TIME ZONE 'UTC')::date - (u.since_at AT TIME ZONE 'UTC')::date) END AS span_days,
           u.attempts, u.bad_json, u.lease_held, u.lease_past, u.hb_age_min, u.expired_lease_retry_count,
           u.rate_limit_deferrals, u.budget_deferrals, u.exhausted
    FROM runs AS r
    CROSS JOIN gate2 AS g2
    JOIN LATERAL (
        SELECT u.status, u.error, u.last_retry_reason, u.dataset_key, u.since_at, u.before_at, u.attempts, u.duration_seconds,
               CASE WHEN pg_input_is_valid(u.result::text, 'jsonb') THEN u.result::jsonb END AS rj,
               (u.result IS NOT NULL AND NOT pg_input_is_valid(u.result::text, 'jsonb')) AS bad_json,
               (u.lease_owner IS NOT NULL) AS lease_held,
               (u.lease_expires_at IS NOT NULL AND u.lease_expires_at < now()) AS lease_past,
               CASE WHEN isfinite(u.last_heartbeat_at) THEN round(extract(epoch FROM now() - u.last_heartbeat_at) / 60)::int END AS hb_age_min,
               u.expired_lease_retry_count, u.rate_limit_deferrals, u.budget_deferrals,
               (u.retry_exhausted_at IS NOT NULL) AS exhausted
        FROM public.sync_run_units AS u
        WHERE g2.ok = 0 AND u.sync_run_id = r.id AND starts_with(u.org_id, 'f0f0f0f0')
        OFFSET 0
    ) AS u ON true
    WHERE r.matched = 1
) AS x
GROUP BY x.run_prefix, x.unit_status, x.error_code, x.error_category, x.cause_class, x.retry_reason, x.dataset_key
ORDER BY x.run_prefix, x.unit_status, x.error_code, x.error_category, x.cause_class, x.retry_reason, x.dataset_key
LIMIT 500;

-- @@S3 failed units (status failed), up to 200 rows, oldest update first. WANT rows per run_prefix = S1 failed_rows (up to 200).
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (SELECT c.relname, c.reltuples::bigint AS reltuples,
                   c.relpages::bigint * bs.block_size AS heap_bytes,
                   COALESCE(t.relpages::bigint, 0) * bs.block_size AS toast_bytes
            FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            CROSS JOIN bs LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
            WHERE n.nspname = 'public' AND c.relkind = 'r'
              AND c.relname IN ('organizations', 'sync_runs', 'sync_run_units')),
    cap(relname, max_tuples, max_bytes) AS (VALUES
        ('organizations', 100000::bigint, 134217728::bigint),
        ('sync_runs', 1000000::bigint, 1073741824::bigint),
        ('sync_run_units', 1000000::bigint, 2147483648::bigint)),
    relok AS (SELECT count(*) AS n,
                     count(*) FILTER (WHERE rel.reltuples BETWEEN 0 AND cap.max_tuples
                                        AND rel.heap_bytes + rel.toast_bytes <= cap.max_bytes) AS ok,
                     COALESCE(string_agg(rel.relname || ':' || rel.reltuples || ':' || (rel.heap_bytes + rel.toast_bytes), ',' ORDER BY rel.relname), '') AS txt
              FROM rel JOIN cap ON cap.relname = rel.relname),
    ix AS (SELECT count(*) AS n FROM pg_catalog.pg_index i
           JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
           JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
           WHERE n.nspname = 'public' AND i.indisvalid AND i.indisready
             AND ic.relname IN ('sync_runs_pkey', 'ix_sync_run_units_run_status')),
    orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0'))
    SELECT CASE WHEN orgs.n = 1 AND relok.n = 3 AND relok.ok = 3 AND ix.n = 2
                THEN 0
                ELSE ('STOP-S3-G0 orgs=' || orgs.n || ' rels=' || relok.n || ' relok=' || relok.ok
                      || ' ix=' || ix.n || ' ' || relok.txt)::int
           END AS ok
    FROM orgs CROSS JOIN relok CROSS JOIN ix
),
runs AS MATERIALIZED (
    SELECT p.pfx, s.id, s.status, s.mode, s.total_units, s.completed_units, s.failed_units,
           s.created_at, s.started_at, s.completed_at, s.error,
           count(s.id) OVER (PARTITION BY p.pfx) AS matched
    FROM (VALUES (:'r1'), (:'r2')) AS p(pfx)
    CROSS JOIN gate g
    LEFT JOIN LATERAL (
        SELECT r.id, r.status, r.mode, r.total_units, r.completed_units, r.failed_units,
               r.created_at, r.started_at, r.completed_at, r.error
        FROM public.sync_runs AS r
        WHERE g.ok = 0
          AND r.id >= (p.pfx || '-0000-0000-0000-000000000000')::uuid
          AND r.id <= (p.pfx || '-ffff-ffff-ffff-ffffffffffff')::uuid
          AND starts_with(r.org_id, 'f0f0f0f0')
        ORDER BY r.id
        LIMIT 2
    ) AS s ON true
),
uc AS MATERIALIZED (
    SELECT count(*) AS n FROM (
        SELECT 1 FROM public.sync_run_units AS u CROSS JOIN gate g
        WHERE g.ok = 0
          AND u.sync_run_id IN (SELECT id FROM runs WHERE id IS NOT NULL)
          AND starts_with(u.org_id, 'f0f0f0f0')
        LIMIT 20001
    ) AS capped
),
gate2 AS MATERIALIZED (
    SELECT CASE WHEN (SELECT count(DISTINCT pfx) FROM runs WHERE matched = 1) = 2
                 AND uc.n BETWEEN 1 AND 20000
                THEN 0
                ELSE ('STOP-S3-G2 runs_matched_1=' || (SELECT count(DISTINCT pfx) FROM runs WHERE matched = 1)
                      || ' runs_ambiguous=' || (SELECT count(DISTINCT pfx) FROM runs WHERE matched >= 2)
                      || ' unit_rows_capped=' || uc.n)::int
           END AS ok
    FROM uc
)
SELECT r.pfx AS run_prefix, left(u.id::text, 8) AS unit8, CASE WHEN u.dataset_key IS NULL THEN '' WHEN u.dataset_key ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.dataset_key ELSE '(non-code)' END AS dataset_key,
       CASE WHEN u.status IS NULL THEN '' WHEN u.status ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.status ELSE '(non-code)' END AS unit_status, CASE WHEN u.mode IS NULL THEN '' WHEN u.mode ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.mode ELSE '(non-code)' END AS unit_mode, CASE WHEN u.cost_class IS NULL THEN '' WHEN u.cost_class ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.cost_class ELSE '(non-code)' END AS cost_class,
       CASE WHEN isfinite(u.since_at) THEN to_char(u.since_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') END AS since_day, CASE WHEN isfinite(u.before_at) THEN to_char(u.before_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') END AS before_day, CASE WHEN isfinite(u.since_at) AND isfinite(u.before_at) THEN ((u.before_at AT TIME ZONE 'UTC')::date - (u.since_at AT TIME ZONE 'UTC')::date) END AS span_days,
       u.attempts, CASE WHEN u.error = 'sync unit lease expired' THEN 'sync_unit_lease_expired' ELSE CASE WHEN u.error IS NULL THEN '' WHEN u.error ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.error ELSE '(non-code)' END END AS error_code, CASE WHEN u.rj->>'error_category' IS NULL THEN '' WHEN u.rj->>'error_category' ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.rj->>'error_category' ELSE '(non-code)' END AS error_category, CASE WHEN u.rj->>'cause_class' IS NULL THEN '' WHEN u.rj->>'cause_class' ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.rj->>'cause_class' ELSE '(non-code)' END AS cause_class, CASE WHEN u.last_retry_reason IS NULL THEN '' WHEN u.last_retry_reason ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.last_retry_reason ELSE '(non-code)' END AS retry_reason,
       CASE WHEN u.rj->'cause_detail'->>'limit' IS NULL THEN '' WHEN u.rj->'cause_detail'->>'limit' ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.rj->'cause_detail'->>'limit' ELSE '(non-code)' END AS cause_limit, CASE WHEN u.rj->'cause_detail'->>'table' IS NULL THEN '' WHEN u.rj->'cause_detail'->>'table' ~ '^[a-z][a-z0-9_]{0,63}$' THEN u.rj->'cause_detail'->>'table' ELSE '(non-code)' END AS cause_table,
       CASE WHEN u.rj->'cause_detail'->>'rows' ~ '^[0-9]{1,18}$' THEN u.rj->'cause_detail'->>'rows' ELSE '' END AS cause_rows,
       CASE WHEN u.rj->'cause_detail'->>'bytes' ~ '^[0-9]{1,18}$' THEN u.rj->'cause_detail'->>'bytes' ELSE '' END AS cause_bytes,
       u.duration_seconds, u.bad_json, u.lease_held, u.lease_past, u.hb_age_min, u.expired_lease_retry_count,
       u.rate_limit_deferrals, u.budget_deferrals, u.exhausted, CASE WHEN isfinite(u.updated_at) THEN to_char(u.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI"Z"') END AS updated_min
FROM runs AS r
CROSS JOIN gate2 AS g2
JOIN LATERAL (
        SELECT s.id, s.mode, s.cost_class, s.updated_at, s.status, s.error, s.last_retry_reason, s.dataset_key, s.since_at, s.before_at,
               s.attempts, s.duration_seconds,
               CASE WHEN pg_input_is_valid(s.result::text, 'jsonb') THEN s.result::jsonb END AS rj,
               (s.result IS NOT NULL AND NOT pg_input_is_valid(s.result::text, 'jsonb')) AS bad_json,
               (s.lease_owner IS NOT NULL) AS lease_held,
               (s.lease_expires_at IS NOT NULL AND s.lease_expires_at < now()) AS lease_past,
               CASE WHEN isfinite(s.last_heartbeat_at) THEN round(extract(epoch FROM now() - s.last_heartbeat_at) / 60)::int END AS hb_age_min,
               s.expired_lease_retry_count, s.rate_limit_deferrals, s.budget_deferrals,
               (s.retry_exhausted_at IS NOT NULL) AS exhausted
        FROM public.sync_run_units AS s
        WHERE g2.ok = 0 AND s.sync_run_id = r.id AND s.status = 'failed' AND starts_with(s.org_id, 'f0f0f0f0')
        OFFSET 0
) AS u ON true
WHERE r.matched = 1
ORDER BY r.pfx, u.updated_at, u.id
LIMIT 200;

ROLLBACK;
