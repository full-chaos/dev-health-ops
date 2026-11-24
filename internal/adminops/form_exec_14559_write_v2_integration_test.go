//go:build integration

package adminops

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DO NOT MERGE. CHAOS-14559 config-write card exec leg, second FORM pair: FORM-14559-G0-2 / FORM-14559-READBACK-3 as pm
// runs them, on synthetic rows. The embedded texts are the .sql files with the org prefix, config prefix, integration
// prefix and repository replaced by placeholders, nothing else (sha256 checked). No prod id is in this file.
const (
	write14559G0V2SHA256       = "22bbe21e8c031e445dadd21f35eba04b531eaa827d84d110b264880b0d8e7301"
	write14559ReadbackV3SHA256 = "3e9e14c81122f969f606f15331077f7b09c433fc18a3d75a8803e34d5f193c53"
	write14559G0V2SQL          = `-- FORM-14559-G0-2 (lane-14559, 2026-11-24, live go-api pin ec6e91ac5fb4). PostgreSQL prod, READ ONLY, CHAOS-14559.
-- The T0 read of CARD-14559-WRITE-6: the card's write target is the one GitHub parent sync configuration of org {{ORG_PREFIX}}
-- whose id starts {{CONFIG_PREFIX}}, its integration starts {{INTEGRATION_PREFIX}}, and no integration of the org holds an exclude list yet.
-- S2 also records the integration config's length, text md5 and semantic md5 (8-char cuts) for READBACK-3 R1.
-- Changed vs FORM-14559-G0-1: the catalog gate in EVERY statement (one-time filter), and S2's new columns.
-- NAMED EXCEPTION (pm, MAIN-WORDS-8): S1 prints the configuration's FULL id; it feeds the verb's --config-id. The out
-- file stays 0600 in pm-guard; the id is never copied to chat, a card, a PR or a log.
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a), PostgreSQL form (b). Run line (card 3.3):
-- psql -X -v ON_ERROR_STOP=1 -f FORM-14559-G0-2.sql (on the primary; one transaction, ends ROLLBACK).
-- No SET, EXPLAIN or write. Key tests are strpos on the stored text. The one cast to jsonb is guarded:
-- CASE WHEN pg_input_is_valid(i.config::text, 'jsonb') THEN i.config::jsonb END (no parse error, no stored text).
-- Org prefix {{ORG_PREFIX}} by starts_with on every FROM with org_id. WANT text and work math: FORM-14559-G0-2.md.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (pg_catalog) and ORG-1. WANT cost_gate_ok = 0. Caps (reltuples / heap + TOAST bytes):
-- organizations 100000 / 268435456, integrations 150000 / 402653184, sync_configurations 200000 / 536870912.
-- Needed index ix_sync_configurations_integration_id valid and ready. A table missing, never analyzed, over a cap,
-- the index missing or not valid/ready, or orgs <> 1 = the named cast error STOP-G0 (sizes and counts only).
-- S1 and S2 hold the same gate as a one-time filter, so each statement stops itself before it reads a row.
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname IN ('organizations', 'integrations', 'sync_configurations')
    ),
    caps (relname, max_rows, max_bytes) AS (
        VALUES ('organizations', 100000::bigint, 268435456::bigint),
               ('integrations', 150000::bigint, 402653184::bigint),
               ('sync_configurations', 200000::bigint, 536870912::bigint)
    ),
    idx AS (
        SELECT count(*) FILTER (WHERE i.indisvalid AND i.indisready) AS ready
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
        WHERE n.nspname = 'public' AND ic.relname = 'ix_sync_configurations_integration_id'
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, '{{ORG_PREFIX}}')
    )
    SELECT CASE WHEN count(rel.relname) = 3
                 AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
                 AND max(idx.ready) = 1
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                               || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                      || ' index_ready=' || max(idx.ready)::text || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN idx CROSS JOIN org
)
SELECT cost_gate_ok FROM gate;

-- S1 the write target: GitHub, active, planner-managed parent (no parent_id) sync configurations of the org whose id
-- starts {{CONFIG_PREFIX}}. WANT configs = 1, config_id = the full id (NAMED EXCEPTION, see the header), integration_prefix = {{INTEGRATION_PREFIX}}.
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname IN ('organizations', 'integrations', 'sync_configurations')
    ),
    caps (relname, max_rows, max_bytes) AS (
        VALUES ('organizations', 100000::bigint, 268435456::bigint),
               ('integrations', 150000::bigint, 402653184::bigint),
               ('sync_configurations', 200000::bigint, 536870912::bigint)
    ),
    idx AS (
        SELECT count(*) FILTER (WHERE i.indisvalid AND i.indisready) AS ready
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
        WHERE n.nspname = 'public' AND ic.relname = 'ix_sync_configurations_integration_id'
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, '{{ORG_PREFIX}}')
    )
    SELECT CASE WHEN count(rel.relname) = 3
                 AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
                 AND max(idx.ready) = 1
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                               || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                      || ' index_ready=' || max(idx.ready)::text || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN idx CROSS JOIN org
)
SELECT count(*) AS configs,
       COALESCE(min(c.id::text), '') AS config_id,
       COALESCE(min(left(c.integration_id::text, 8)), '') AS integration_prefix
FROM public.sync_configurations c
WHERE (SELECT g.cost_gate_ok FROM gate g) = 0
  AND starts_with(c.org_id::text, '{{ORG_PREFIX}}') AND starts_with(c.id::text, '{{CONFIG_PREFIX}}')
  AND lower(c.provider) = 'github' AND c.is_active AND c.planner_managed AND c.parent_id IS NULL;

-- S2 its integration (id starts {{INTEGRATION_PREFIX}}, in the org): WANT integrations = 1, integration_key_set false,
-- configs_of_integration >= 1, configs_with_key_in_options 0, bad_json 0, org_integrations_with_key 0;
-- INFO config_length, config_md5_8 (md5 of the stored text), semantic_md5_8 (md5 of the parsed document without the
-- key; the input READBACK-3 R1 compares). The stored text is never selected.
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname IN ('organizations', 'integrations', 'sync_configurations')
    ),
    caps (relname, max_rows, max_bytes) AS (
        VALUES ('organizations', 100000::bigint, 268435456::bigint),
               ('integrations', 150000::bigint, 402653184::bigint),
               ('sync_configurations', 200000::bigint, 536870912::bigint)
    ),
    idx AS (
        SELECT count(*) FILTER (WHERE i.indisvalid AND i.indisready) AS ready
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
        WHERE n.nspname = 'public' AND ic.relname = 'ix_sync_configurations_integration_id'
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, '{{ORG_PREFIX}}')
    )
    SELECT CASE WHEN count(rel.relname) = 3
                 AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
                 AND max(idx.ready) = 1
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                               || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                      || ' index_ready=' || max(idx.ready)::text || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN idx CROSS JOIN org
),
orgkey AS MATERIALIZED (
    SELECT count(*) FILTER (WHERE strpos(i.config::text, '"ci_artifact_exclude_prefixes"') > 0) AS org_integrations_with_key
    FROM public.integrations i
    WHERE starts_with(i.org_id::text, '{{ORG_PREFIX}}')
),
ints AS MATERIALIZED (
    SELECT i.id,
           strpos(i.config::text, '"ci_artifact_exclude_prefixes"') > 0 AS integration_key_set,
           length(i.config::text) AS config_length,
           left(md5(i.config::text), 8) AS config_md5_8,
           CASE WHEN pg_input_is_valid(i.config::text, 'jsonb') THEN i.config::jsonb END AS doc
    FROM public.integrations i
    WHERE starts_with(i.org_id::text, '{{ORG_PREFIX}}') AND starts_with(i.id::text, '{{INTEGRATION_PREFIX}}') AND lower(i.provider) = 'github'
),
cfg AS MATERIALIZED (
    SELECT c.integration_id, count(*) AS configs_of_integration,
           count(*) FILTER (WHERE strpos(c.sync_options::text, '"ci_artifact_exclude_prefixes"') > 0) AS configs_with_key_in_options
    FROM ints JOIN public.sync_configurations c ON c.integration_id = ints.id
    WHERE starts_with(c.org_id::text, '{{ORG_PREFIX}}')
    GROUP BY c.integration_id
)
SELECT count(ints.id) AS integrations,
       COALESCE(bool_or(ints.integration_key_set), false) AS integration_key_set,
       COALESCE(sum(cfg.configs_of_integration), 0)::bigint AS configs_of_integration,
       COALESCE(sum(cfg.configs_with_key_in_options), 0)::bigint AS configs_with_key_in_options,
       count(*) FILTER (WHERE ints.id IS NOT NULL AND (ints.doc IS NULL OR jsonb_typeof(ints.doc) <> 'object')) AS bad_json,
       (SELECT k.org_integrations_with_key FROM orgkey k) AS org_integrations_with_key,
       COALESCE(max(ints.config_length), 0) AS config_length,
       COALESCE(max(ints.config_md5_8), '') AS config_md5_8,
       COALESCE(max(CASE WHEN jsonb_typeof(ints.doc) = 'object'
                         THEN left(md5((ints.doc - 'ci_artifact_exclude_prefixes')::text), 8) END), '') AS semantic_md5_8
FROM ints LEFT JOIN cfg ON cfg.integration_id = ints.id
WHERE (SELECT g.cost_gate_ok FROM gate g) = 0;

ROLLBACK;
`
	write14559ReadbackV3SQL = `-- FORM-14559-READBACK-3 (lane-14559, 2026-11-24, live go-api pin ec6e91ac5fb4). PostgreSQL prod, READ ONLY, CHAOS-14559.
-- The read-back of CARD-14559-WRITE-6: the integration of org {{ORG_PREFIX}} whose id starts {{INTEGRATION_PREFIX}} holds exactly the list
-- go-images-, go-test-timings-, go-junit-part- (the one store; every sync config of the integration shows it), no sync
-- config keeps a copy, it is the ONE integration of the org with the key, its other keys are unchanged (semantic md5 =
-- FORM-14559-G0-2 S2), and the GitHub tests units of its full-chaos/dev-health-ops source created after the write carry
-- excluded_non_report_prefix in their result. T0 = the integration's updated_at (the write stamps it).
-- Changed vs FORM-14559-READBACK-2: the catalog gate in EVERY statement (one-time filter); R1's new columns.
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a), PostgreSQL form (b). Run line (card 3.11), after the
-- encoding call (card 3.10) as its own psql call:
-- psql -X -v ON_ERROR_STOP=1 -f FORM-14559-READBACK-3.sql (on the primary; one transaction, ends ROLLBACK).
-- Needs standard_conforming_strings on (R2's regex literal holds backslashes) and server_encoding UTF8 (card 3.10).
-- No SET, EXPLAIN or write; no stored text is selected. Output: counts, booleans, lengths, 8-char md5 cuts, minutes.
-- The one cast to jsonb is guarded: CASE WHEN pg_input_is_valid(i.config::text, 'jsonb') THEN i.config::jsonb END.
-- Org prefix {{ORG_PREFIX}} by starts_with on every FROM with org_id. WANT text and work math: FORM-14559-READBACK-3.md.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (pg_catalog) and ORG-1. WANT cost_gate_ok = 0. Caps (reltuples / heap + TOAST bytes):
-- organizations 100000 / 268435456, integrations 150000 / 402653184, sync_configurations 200000 / 536870912,
-- integration_sources 3000000 / 2147483648, sync_run_units 5000000 / 4294967296. Needed indexes valid and ready:
-- ix_sync_configurations_integration_id, ix_sync_run_units_source_id. A table missing, never analyzed, over a cap,
-- an index missing or not valid/ready, or orgs <> 1 = the named cast error STOP-G0 (sizes and counts only).
-- R1, R2 and C1 hold the same gate as a one-time filter, so each statement stops itself before it reads a row.
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_sources', 'sync_run_units')
    ),
    caps (relname, max_rows, max_bytes) AS (
        VALUES ('organizations', 100000::bigint, 268435456::bigint),
               ('integrations', 150000::bigint, 402653184::bigint),
               ('sync_configurations', 200000::bigint, 536870912::bigint),
               ('integration_sources', 3000000::bigint, 2147483648::bigint),
               ('sync_run_units', 5000000::bigint, 4294967296::bigint)
    ),
    idx AS (
        SELECT count(*) FILTER (WHERE i.indisvalid AND i.indisready) AS ready
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
        WHERE n.nspname = 'public' AND ic.relname IN ('ix_sync_configurations_integration_id', 'ix_sync_run_units_source_id')
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, '{{ORG_PREFIX}}')
    )
    SELECT CASE WHEN count(rel.relname) = 5
                 AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
                 AND max(idx.ready) = 2
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                               || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                      || ' indexes_ready=' || max(idx.ready)::text || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN idx CROSS JOIN org
)
SELECT cost_gate_ok FROM gate;

-- R1 the list (read of record: the stored text, compared exactly, no parse). WANT integrations = 1, integration_key_set
-- true, list_exact true (the stored config holds exactly the 3 prefixes in this order, as WriteTx dumps them),
-- configs_of_integration >= 1, configs_with_key_in_options = 0, bad_json = 0, org_integrations_with_key = 1,
-- semantic_md5_8 = G0-2 S2 semantic_md5_8 (the parsed document without the key: no other key or value changed);
-- t0_minute in [Block B utc_before, utc_after]. INFO: config_length = G0-2 config_length + 86 (+84 when G0-2 had 2,
-- an empty object), text_md5_8 (the stored text with the exact key text removed) = G0-2 config_md5_8 (equal only when
-- the stored text was in pyjson.Dumps form before the write).
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_sources', 'sync_run_units')
    ),
    caps (relname, max_rows, max_bytes) AS (
        VALUES ('organizations', 100000::bigint, 268435456::bigint),
               ('integrations', 150000::bigint, 402653184::bigint),
               ('sync_configurations', 200000::bigint, 536870912::bigint),
               ('integration_sources', 3000000::bigint, 2147483648::bigint),
               ('sync_run_units', 5000000::bigint, 4294967296::bigint)
    ),
    idx AS (
        SELECT count(*) FILTER (WHERE i.indisvalid AND i.indisready) AS ready
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
        WHERE n.nspname = 'public' AND ic.relname IN ('ix_sync_configurations_integration_id', 'ix_sync_run_units_source_id')
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, '{{ORG_PREFIX}}')
    )
    SELECT CASE WHEN count(rel.relname) = 5
                 AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
                 AND max(idx.ready) = 2
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                               || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                      || ' indexes_ready=' || max(idx.ready)::text || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN idx CROSS JOIN org
),
orgkey AS MATERIALIZED (
    SELECT count(*) FILTER (WHERE strpos(i.config::text, '"ci_artifact_exclude_prefixes"') > 0) AS org_integrations_with_key
    FROM public.integrations i
    WHERE starts_with(i.org_id::text, '{{ORG_PREFIX}}')
),
ints AS MATERIALIZED (
    SELECT i.id,
           strpos(i.config::text, '"ci_artifact_exclude_prefixes"') > 0 AS integration_key_set,
           strpos(i.config::text, '"ci_artifact_exclude_prefixes": ["go-images-", "go-test-timings-", "go-junit-part-"]') > 0 AS list_exact,
           length(i.config::text) AS config_length,
           left(md5(replace(replace(i.config::text, ', "ci_artifact_exclude_prefixes": ["go-images-", "go-test-timings-", "go-junit-part-"]', ''), '"ci_artifact_exclude_prefixes": ["go-images-", "go-test-timings-", "go-junit-part-"]', '')), 8) AS text_md5_8,
           CASE WHEN pg_input_is_valid(i.config::text, 'jsonb') THEN i.config::jsonb END AS doc,
           i.updated_at
    FROM public.integrations i
    WHERE starts_with(i.org_id::text, '{{ORG_PREFIX}}') AND starts_with(i.id::text, '{{INTEGRATION_PREFIX}}') AND lower(i.provider) = 'github'
),
cfg AS MATERIALIZED (
    SELECT c.integration_id, count(*) AS configs_of_integration,
           count(*) FILTER (WHERE strpos(c.sync_options::text, '"ci_artifact_exclude_prefixes"') > 0) AS configs_with_key_in_options
    FROM ints JOIN public.sync_configurations c ON c.integration_id = ints.id
    WHERE starts_with(c.org_id::text, '{{ORG_PREFIX}}')
    GROUP BY c.integration_id
)
SELECT count(ints.id) AS integrations,
       COALESCE(bool_or(ints.integration_key_set), false) AS integration_key_set,
       COALESCE(bool_or(ints.list_exact), false) AS list_exact,
       COALESCE(sum(cfg.configs_of_integration), 0)::bigint AS configs_of_integration,
       COALESCE(sum(cfg.configs_with_key_in_options), 0)::bigint AS configs_with_key_in_options,
       count(*) FILTER (WHERE ints.id IS NOT NULL AND (ints.doc IS NULL OR jsonb_typeof(ints.doc) <> 'object')) AS bad_json,
       (SELECT k.org_integrations_with_key FROM orgkey k) AS org_integrations_with_key,
       COALESCE(max(CASE WHEN jsonb_typeof(ints.doc) = 'object'
                         THEN left(md5((ints.doc - 'ci_artifact_exclude_prefixes')::text), 8) END), '') AS semantic_md5_8,
       COALESCE(max(ints.config_length), 0) AS config_length,
       COALESCE(max(ints.text_md5_8), '') AS text_md5_8,
       COALESCE(to_char(max(ints.updated_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI'), '') AS t0_minute
FROM ints LEFT JOIN cfg ON cfg.integration_id = ints.id
WHERE (SELECT g.cost_gate_ok FROM gate g) = 0;

-- R2 the next units: GitHub tests/cicd units of the integration's full-chaos/dev-health-ops source created after T0
-- (the integration's updated_at). INFO units_after_t0, success_after_t0, unreadable_results (success units whose result
-- text holds the escape \u0000 or a UTF-16 surrogate escape); WANT, once success_after_t0 > unreadable_results:
-- with_prefix_exclusions >= 1 (result.excluded_non_report_prefix an integer > 0). json -> de-escapes every string of the
-- document and raises 22P05 on those escapes, so the outer WHEN tests the stored text with a regex first (no parse) and
-- only the inner CASE uses ->. The inner CASE casts only 1 to 18 plain digits to bigint (no cast can raise).
-- On the live workers pin (a7febdf50002, before ec43459c6614) this counter is the code list only (card 7).
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_sources', 'sync_run_units')
    ),
    caps (relname, max_rows, max_bytes) AS (
        VALUES ('organizations', 100000::bigint, 268435456::bigint),
               ('integrations', 150000::bigint, 402653184::bigint),
               ('sync_configurations', 200000::bigint, 536870912::bigint),
               ('integration_sources', 3000000::bigint, 2147483648::bigint),
               ('sync_run_units', 5000000::bigint, 4294967296::bigint)
    ),
    idx AS (
        SELECT count(*) FILTER (WHERE i.indisvalid AND i.indisready) AS ready
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
        WHERE n.nspname = 'public' AND ic.relname IN ('ix_sync_configurations_integration_id', 'ix_sync_run_units_source_id')
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, '{{ORG_PREFIX}}')
    )
    SELECT CASE WHEN count(rel.relname) = 5
                 AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
                 AND max(idx.ready) = 2
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                               || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                      || ' indexes_ready=' || max(idx.ready)::text || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN idx CROSS JOIN org
),
ints AS MATERIALIZED (
    SELECT i.id, i.updated_at
    FROM public.integrations i
    WHERE starts_with(i.org_id::text, '{{ORG_PREFIX}}') AND starts_with(i.id::text, '{{INTEGRATION_PREFIX}}') AND lower(i.provider) = 'github'
),
src AS MATERIALIZED (
    SELECT s.id, ints.updated_at
    FROM ints JOIN public.integration_sources s ON s.integration_id = ints.id
    WHERE starts_with(s.org_id::text, '{{ORG_PREFIX}}')
      AND (lower(s.external_id) = '{{REPO}}' OR lower(s.full_name) = '{{REPO}}')
),
units AS MATERIALIZED (
    SELECT u.status, u.created_at,
           u.result::text ~* '\\u(0000|d[89a-f][0-9a-f]{2})' AS unreadable,
           CASE WHEN u.result::text ~* '\\u(0000|d[89a-f][0-9a-f]{2})' THEN NULL
                ELSE CASE WHEN (u.result -> 'excluded_non_report_prefix')::text ~ '^[0-9]{1,18}$'
                          THEN ((u.result -> 'excluded_non_report_prefix')::text)::bigint END
           END AS excluded_prefix
    FROM src JOIN public.sync_run_units u ON u.source_id = src.id
    WHERE starts_with(u.org_id::text, '{{ORG_PREFIX}}') AND u.provider = 'github' AND u.dataset_key IN ('tests', 'cicd')
      AND u.created_at > src.updated_at
)
SELECT count(*) AS units_after_t0,
       count(*) FILTER (WHERE status = 'success') AS success_after_t0,
       count(*) FILTER (WHERE status = 'success' AND unreadable) AS unreadable_results,
       count(*) FILTER (WHERE status = 'success' AND excluded_prefix > 0) AS with_prefix_exclusions,
       COALESCE(max(excluded_prefix) FILTER (WHERE status = 'success'), 0) AS max_excluded_prefix,
       COALESCE(to_char(max(created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI'), '') AS last_unit_minute
FROM units
WHERE (SELECT g.cost_gate_ok FROM gate g) = 0;

-- C1 control: units of that source of any time and status. WANT source_units > 0 (the source and its units exist).
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_sources', 'sync_run_units')
    ),
    caps (relname, max_rows, max_bytes) AS (
        VALUES ('organizations', 100000::bigint, 268435456::bigint),
               ('integrations', 150000::bigint, 402653184::bigint),
               ('sync_configurations', 200000::bigint, 536870912::bigint),
               ('integration_sources', 3000000::bigint, 2147483648::bigint),
               ('sync_run_units', 5000000::bigint, 4294967296::bigint)
    ),
    idx AS (
        SELECT count(*) FILTER (WHERE i.indisvalid AND i.indisready) AS ready
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
        WHERE n.nspname = 'public' AND ic.relname IN ('ix_sync_configurations_integration_id', 'ix_sync_run_units_source_id')
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, '{{ORG_PREFIX}}')
    )
    SELECT CASE WHEN count(rel.relname) = 5
                 AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
                 AND max(idx.ready) = 2
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                               || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                      || ' indexes_ready=' || max(idx.ready)::text || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN idx CROSS JOIN org
)
SELECT count(*) AS source_units
FROM public.integration_sources s
JOIN public.sync_run_units u ON u.source_id = s.id
WHERE starts_with(s.org_id::text, '{{ORG_PREFIX}}') AND starts_with(s.integration_id::text, '{{INTEGRATION_PREFIX}}')
  AND (lower(s.external_id) = '{{REPO}}' OR lower(s.full_name) = '{{REPO}}')
  AND starts_with(u.org_id::text, '{{ORG_PREFIX}}')
  AND (SELECT g.cost_gate_ok FROM gate g) = 0;

ROLLBACK;
`
)

const write14559OtherOrgIntegration = "f1455b07-1455-4000-8000-0000000000a7"

func write14559MD58(text string) string {
	sum := md5.Sum([]byte(text))
	return hex.EncodeToString(sum[:])[:8]
}

// write14559Cols is a one-row result as column -> value.
func write14559Cols(t *testing.T, results map[string]write14559Result, id string) map[string]string {
	t.Helper()
	result := results[id]
	if len(result.rows) != 1 {
		t.Fatalf("%s: %d rows, want 1", id, len(result.rows))
	}
	cols := map[string]string{}
	for i, name := range result.columns {
		cols[name] = result.rows[0][i]
	}
	return cols
}

func write14559SetConfig(ctx context.Context, t *testing.T, conn *pgx.Conn, id, text string) {
	t.Helper()
	if _, err := conn.Exec(ctx, `UPDATE integrations SET config = $2::json WHERE id = $1::uuid`, id, text); err != nil {
		t.Fatalf("set config: %v", err)
	}
}

// write14559EachStatementGated: every statement after G0 plans the gate as a one-time filter (it runs before any row is
// read) and stops itself with STOP-G0 when its own copy of the gate fires, after the statements before it ran.
func write14559EachStatementGated(ctx context.Context, t *testing.T, conn *pgx.Conn, pieces []write14559Piece) {
	t.Helper()
	for index, piece := range pieces {
		if index < 2 || piece.id == "ROLLBACK" {
			continue
		}
		var plan []string
		rows, err := conn.Query(ctx, "EXPLAIN (COSTS OFF) "+piece.text)
		if err != nil {
			t.Fatalf("EXPLAIN %s: %v", piece.id, err)
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, line)
		}
		rows.Close()
		if !strings.Contains(strings.Join(plan, "\n"), "One-Time Filter: ((InitPlan") {
			t.Errorf("%s: the gate is not a one-time filter:\n%s", piece.id, strings.Join(plan, "\n"))
		}
		results, err := write14559Run(ctx, t, conn, pieces, piece.id, "100000::bigint", "0::bigint")
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "22P02" || !strings.Contains(pgErr.Message, "STOP-G0") {
			t.Errorf("GATE in %s: err = %v, want 22P02 STOP-G0", piece.id, err)
			continue
		}
		if _, ran := results[piece.id]; ran || len(results) != index-1 {
			t.Errorf("GATE in %s: %d statement(s) ran, want %d before it", piece.id, len(results), index-1)
		}
	}
}

// FORM-14559-G0-2 and FORM-14559-READBACK-3 as pm runs them around the card's write: S2 records the integration's config
// length, text md5 and semantic md5 (8-char cuts); R1 shows the same semantic md5 after the write (no other key or value
// changed) and one integration of the org with the key. Cases: a Dumps-form config (text md5 and length + 86 equal too),
// a changed other value (semantic md5 unequal = STOP), a config not in Dumps form (semantic equal, text md5 unequal =
// INFO), and a config holding \u0000 (no error, bad_json 1, no stored text in any cell; the bare cast raises 22P05).
func TestFormExec14559G0V2AndReadbackV3(t *testing.T) {
	ctx := context.Background()
	db := startDatabase(t)
	write14559Seed(ctx, t, db.conn)
	g0 := write14559Prepare(t, "G0-2", write14559G0V2SQL, write14559G0V2SHA256, regexp.MustCompile(`^(G0|S[12])$`), "BEGIN,G0,S1,S2,ROLLBACK")
	rb := write14559Prepare(t, "READBACK-3", write14559ReadbackV3SQL, write14559ReadbackV3SHA256, regexp.MustCompile(`^(G0|R[12]|C1)$`), "BEGIN,G0,R1,R2,C1,ROLLBACK")
	run := func(pieces []write14559Piece, id string) map[string]string {
		t.Helper()
		results, err := write14559Run(ctx, t, db.conn, pieces, "", "", "")
		if err != nil {
			t.Fatal(err)
		}
		if got := write14559Rows(results, "G0"); got != "cost_gate_ok=0" {
			t.Fatalf("G0 = %s", got)
		}
		return write14559Cols(t, results, id)
	}
	write := func(label string) {
		t.Helper()
		code, stdout, stderr := write14559Verb(t, db, "sync-configs", "artifact-excludes", "set", "--config-id", write14559Config,
			"--prefix", "go-images-", "--prefix", "go-test-timings-", "--prefix", "go-junit-part-", "--reason", "exec "+label)
		if code != 0 || !strings.Contains(stdout, `"integration_config_changed":true`) {
			t.Fatalf("%s write: %d %s %s", label, code, stdout, stderr)
		}
	}
	expect := func(label string, got map[string]string, want map[string]string) {
		t.Helper()
		for name, value := range want {
			if got[name] != value {
				t.Errorf("%s: %s = %q, want %q (row %v)", label, name, got[name], value, got)
			}
		}
	}
	// Another org's integration holds a list: the org-wide count must not see it.
	write14559SetConfig(ctx, t, db.conn, write14559OtherOrgIntegration, `{"ci_artifact_exclude_prefixes": ["x-"]}`)

	// Case 1, a Dumps-form config.
	s2 := run(g0, "S2")
	seeded := `{"fetch_comments": true}`
	expect("case 1 S2", s2, map[string]string{"integrations": "1", "integration_key_set": "false", "configs_of_integration": "2",
		"configs_with_key_in_options": "0", "bad_json": "0", "org_integrations_with_key": "0",
		"config_length": strconv.Itoa(len(seeded)), "config_md5_8": write14559MD58(seeded), "semantic_md5_8": write14559MD58(seeded)})
	write("case 1")
	r1 := run(rb, "R1")
	expect("case 1 R1", r1, map[string]string{"integrations": "1", "integration_key_set": "true", "list_exact": "true",
		"configs_of_integration": "2", "configs_with_key_in_options": "0", "bad_json": "0", "org_integrations_with_key": "1",
		"semantic_md5_8": s2["semantic_md5_8"], "config_length": strconv.Itoa(len(seeded) + 86), "text_md5_8": s2["config_md5_8"]})

	// Row mutants at this state: each changes its statement's row.
	for _, mutant := range []struct{ name, id, from, to string }{
		{"s2-orgkey-all-orgs", "S2", "    WHERE starts_with(i.org_id::text, '" + write14559Org[:8] + "')\n),\nints AS", "    WHERE true\n),\nints AS"},
		{"r1-orgkey-all-orgs", "R1", "    WHERE starts_with(i.org_id::text, '" + write14559Org[:8] + "')\n),\nints AS", "    WHERE true\n),\nints AS"},
		{"r1-semantic-keeps-key", "R1", "(ints.doc - 'ci_artifact_exclude_prefixes')", "(ints.doc)"},
		{"r1-text-keeps-key", "R1", "left(md5(replace(replace(i.config::text,", "left(md5(concat(concat(i.config::text,"},
	} {
		pieces := g0
		if mutant.id != "S2" {
			pieces = rb
		}
		base, err := write14559Run(ctx, t, db.conn, pieces, "", "", "")
		if err != nil {
			t.Fatal(err)
		}
		mutated, err := write14559Run(ctx, t, db.conn, pieces, mutant.id, mutant.from, mutant.to)
		if err != nil {
			t.Fatalf("MUTANT %s: %v", mutant.name, err)
		}
		if write14559Rows(mutated, mutant.id) == write14559Rows(base, mutant.id) {
			t.Errorf("MUTANT %s SURVIVED: %s", mutant.name, write14559Rows(mutated, mutant.id))
		}
	}

	// Case 2, another value changed after G0-2: the semantic md5 differs = STOP.
	write14559SetConfig(ctx, t, db.conn, write14559Integration, `{"fetch_comments": false, `+write14559ListText+`}`)
	if got := run(rb, "R1"); got["semantic_md5_8"] == s2["semantic_md5_8"] || got["list_exact"] != "true" {
		t.Errorf("case 2 R1 = %v: want list_exact true and semantic_md5_8 unequal to G0-2's %s", got, s2["semantic_md5_8"])
	}

	// Case 3, a config not in Dumps form: the semantic md5 is equal, the text md5 and the length + 86 are not (INFO).
	notDumps := `{"a":1}`
	write14559SetConfig(ctx, t, db.conn, write14559Integration, notDumps)
	s2 = run(g0, "S2")
	expect("case 3 S2", s2, map[string]string{"integration_key_set": "false", "bad_json": "0", "org_integrations_with_key": "0",
		"config_length": strconv.Itoa(len(notDumps)), "config_md5_8": write14559MD58(notDumps), "semantic_md5_8": write14559MD58(`{"a": 1}`)})
	write("case 3")
	r1 = run(rb, "R1")
	expect("case 3 R1", r1, map[string]string{"list_exact": "true", "bad_json": "0", "org_integrations_with_key": "1",
		"semantic_md5_8": s2["semantic_md5_8"], "config_length": strconv.Itoa(len(notDumps) + 87), "text_md5_8": write14559MD58(`{"a": 1}`)})
	if r1["text_md5_8"] == s2["config_md5_8"] {
		t.Errorf("case 3: text_md5_8 %s equals G0-2's config_md5_8: the INFO cannot tell the form change", r1["text_md5_8"])
	}

	// Case 4, a config holding \u0000: both forms run without error, bad_json is 1, and no cell holds stored text.
	write14559SetConfig(ctx, t, db.conn, write14559Integration, `{"fetch_comments": true, "secret_marker_q7": "\u0000"}`)
	for _, form := range []struct {
		name   string
		pieces []write14559Piece
		id     string
	}{{"G0-2", g0, "S2"}, {"READBACK-3", rb, "R1"}} {
		results, err := write14559Run(ctx, t, db.conn, form.pieces, "", "", "")
		if err != nil {
			t.Fatalf("case 4 %s: %v", form.name, err)
		}
		cols := write14559Cols(t, results, form.id)
		if cols["bad_json"] != "1" || cols["semantic_md5_8"] != "" {
			t.Errorf("case 4 %s %s = %v: want bad_json 1 and no semantic md5", form.name, form.id, cols)
		}
		for id := range results {
			if rows := write14559Rows(results, id); strings.Contains(rows, "secret_marker_q7") || strings.Contains(rows, `\u0000`) {
				t.Errorf("case 4 %s %s holds stored text: %s", form.name, id, rows)
			}
		}
	}
	// Without the guard the cast raises 22P05, and its CONTEXT would quote the stored text: the guard is load-bearing.
	guarded := "CASE WHEN pg_input_is_valid(i.config::text, 'jsonb') THEN i.config::jsonb END AS doc"
	if _, err := write14559Run(ctx, t, db.conn, g0, "S2", guarded, "i.config::jsonb AS doc"); err == nil {
		t.Errorf("MUTANT s2-bare-cast SURVIVED: the bare cast read a \\u0000 config without error")
	} else if pgErr := (*pgconn.PgError)(nil); !errors.As(err, &pgErr) || pgErr.Code != "22P05" {
		t.Errorf("MUTANT s2-bare-cast: err = %v, want SQLSTATE 22P05", err)
	}

	// The gate: G0 fires on each cap, index and org; every later statement holds its own copy as a one-time filter.
	write14559Gates(ctx, t, db.conn, g0, []string{"100000::bigint", "268435456::bigint", "150000::bigint", "402653184::bigint", "200000::bigint", "536870912::bigint"},
		map[string]string{"ix_sync_configurations_integration_id": "index_ready=0"})
	write14559Gates(ctx, t, db.conn, rb,
		[]string{"100000::bigint", "268435456::bigint", "150000::bigint", "402653184::bigint", "200000::bigint", "536870912::bigint",
			"3000000::bigint", "2147483648::bigint", "5000000::bigint", "4294967296::bigint"},
		map[string]string{"ix_sync_configurations_integration_id": "indexes_ready=1", "ix_sync_run_units_source_id": "indexes_ready=1"})
	write14559EachStatementGated(ctx, t, db.conn, g0)
	write14559EachStatementGated(ctx, t, db.conn, rb)
}
