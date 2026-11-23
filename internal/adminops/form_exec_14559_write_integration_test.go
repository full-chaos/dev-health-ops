//go:build integration

package adminops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// DO NOT MERGE. CHAOS-14559 config-write card exec leg: CARD-14559-WRITE-1's argv run through the dho command tree on a
// migrated PostgreSQL with synthetic rows, and FORM-14559-G0-1 / FORM-14559-READBACK-1 executed as pm runs them. The
// embedded texts are the .sql files with the org prefix, config prefix, integration prefix and repository replaced by
// placeholders, nothing else (sha256 checked); the cells put synthetic values in at run time. No prod id is in this file.
const (
	write14559G0SHA256       = "dad25341a62662227331d9f5ee44e9f143a6bab767e91a73f56ca118c2228006"
	write14559ReadbackSHA256 = "69ca447becd26269366b18a4ecfea93b974ac39ceb377a51c2a0255e915e135a"
	write14559G0SQL          = `-- FORM-14559-G0-1 (lane-14559, 2026-11-23, ops pin ec43459c6614 = #6953 merged). PostgreSQL prod, READ ONLY, CHAOS-14559.
-- The T0 read of CARD-14559-WRITE-1: the card's write target is the one GitHub parent sync configuration of org {{ORG_PREFIX}}
-- whose id starts {{CONFIG_PREFIX}}, its integration starts {{INTEGRATION_PREFIX}}, and that integration holds no exclude list yet.
-- NAMED EXCEPTION (pm, MAIN-WORDS-8): S1 prints the configuration's FULL id; it feeds the verb's --config-id. The out
-- file stays 0600 in pm-guard; the id is never copied to chat, a card, a PR or a log.
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a), PostgreSQL form (b). Run line:
-- psql -X -v ON_ERROR_STOP=1 -f FORM-14559-G0-1.sql (on the primary; one transaction, ends ROLLBACK).
-- No SET, EXPLAIN, cast to jsonb or write. The key test is strpos on the stored text (FORM-14559-CONFIGS-3 lesson).
-- Org prefix {{ORG_PREFIX}} by starts_with on every FROM with org_id. WANT text and work math: FORM-14559-G0-1.md.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (pg_catalog) and ORG-1. WANT cost_gate_ok = 0. Caps (reltuples / heap + TOAST bytes):
-- organizations 100000 / 268435456, integrations 150000 / 402653184, sync_configurations 200000 / 536870912.
-- Needed index ix_sync_configurations_integration_id valid and ready. A table missing, never analyzed, over a cap,
-- the index missing or not valid/ready, or orgs <> 1 = the named cast error STOP-G0 (sizes and counts only).
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
FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN idx CROSS JOIN org;

-- S1 the write target: GitHub, active, planner-managed parent (no parent_id) sync configurations of the org whose id
-- starts {{CONFIG_PREFIX}}. WANT configs = 1, config_id = the full id (NAMED EXCEPTION, see the header), integration_prefix = {{INTEGRATION_PREFIX}}.
SELECT count(*) AS configs,
       COALESCE(min(c.id::text), '') AS config_id,
       COALESCE(min(left(c.integration_id::text, 8)), '') AS integration_prefix
FROM public.sync_configurations c
WHERE starts_with(c.org_id::text, '{{ORG_PREFIX}}') AND starts_with(c.id::text, '{{CONFIG_PREFIX}}')
  AND lower(c.provider) = 'github' AND c.is_active AND c.planner_managed AND c.parent_id IS NULL;

-- S2 its integration (id starts {{INTEGRATION_PREFIX}}, in the org): WANT integrations = 1, integration_key_set false,
-- configs_of_integration >= 1, configs_with_key_in_options 0. integrations read once; the configurations of that one
-- integration by ix_sync_configurations_integration_id (the index G0 checks).
WITH ints AS MATERIALIZED (
    SELECT i.id, strpos(i.config::text, '"ci_artifact_exclude_prefixes"') > 0 AS integration_key_set
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
       COALESCE(sum(cfg.configs_with_key_in_options), 0)::bigint AS configs_with_key_in_options
FROM ints LEFT JOIN cfg ON cfg.integration_id = ints.id;

ROLLBACK;
`
	write14559ReadbackSQL = `-- FORM-14559-READBACK-1 (lane-14559, 2026-11-23, ops pin ec43459c6614 = #6953 merged). PostgreSQL prod, READ ONLY, CHAOS-14559.
-- The read-back of CARD-14559-WRITE-1: the integration of org {{ORG_PREFIX}} whose id starts {{INTEGRATION_PREFIX}} holds exactly the list
-- go-images-, go-test-timings-, go-junit-part- (the one store; every sync config of the integration shows it), no sync
-- config keeps a copy, and the GitHub tests units of its full-chaos/dev-health-ops source created after the write
-- carry excluded_non_report_prefix in their result. T0 = the integration's updated_at (the write stamps it).
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a), PostgreSQL form (b). Run line:
-- psql -X -v ON_ERROR_STOP=1 -f FORM-14559-READBACK-1.sql (on the primary; one transaction, ends ROLLBACK).
-- No SET, EXPLAIN, cast to jsonb or write; no stored text is selected. Output: counts, booleans, 8-char cuts, minutes.
-- Org prefix {{ORG_PREFIX}} by starts_with on every FROM with org_id. WANT text and work math: FORM-14559-READBACK-1.md.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (pg_catalog) and ORG-1. WANT cost_gate_ok = 0. Caps (reltuples / heap + TOAST bytes):
-- organizations 100000 / 268435456, integrations 150000 / 402653184, sync_configurations 200000 / 536870912,
-- integration_sources 3000000 / 2147483648, sync_run_units 5000000 / 4294967296. Needed indexes valid and ready:
-- ix_sync_configurations_integration_id, ix_sync_run_units_source_id. A table missing, never analyzed, over a cap,
-- an index missing or not valid/ready, or orgs <> 1 = the named cast error STOP-G0 (sizes and counts only).
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
FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN idx CROSS JOIN org;

-- R1 the list (read of record: the stored text, compared exactly, no parse). WANT integrations = 1, integration_key_set
-- true, list_exact true (the stored config holds exactly the 3 prefixes in this order, as WriteTx dumps them),
-- configs_of_integration >= 1 and configs_with_key_in_options = 0 (every config of the integration then shows the
-- integration's list through the GET's overlay), t0_minute = the write's UTC minute.
WITH ints AS MATERIALIZED (
    SELECT i.id,
           strpos(i.config::text, '"ci_artifact_exclude_prefixes"') > 0 AS integration_key_set,
           strpos(i.config::text, '"ci_artifact_exclude_prefixes": ["go-images-", "go-test-timings-", "go-junit-part-"]') > 0 AS list_exact,
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
       COALESCE(to_char(max(ints.updated_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI'), '') AS t0_minute
FROM ints LEFT JOIN cfg ON cfg.integration_id = ints.id;

-- R2 the next units: GitHub tests/cicd units of the integration's full-chaos/dev-health-ops source created after T0
-- (the integration's updated_at). INFO units_after_t0, success_after_t0; WANT, once success_after_t0 >= 1:
-- with_prefix_exclusions >= 1 (result.excluded_non_report_prefix a number > 0). The value is read with json -> and
-- json_typeof (no de-escape, no jsonb cast), cast to a number only under the CASE guard.
WITH ints AS MATERIALIZED (
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
           CASE WHEN json_typeof(u.result -> 'excluded_non_report_prefix') = 'number'
                THEN (((u.result -> 'excluded_non_report_prefix')::text)::numeric)::bigint END AS excluded_prefix
    FROM src JOIN public.sync_run_units u ON u.source_id = src.id
    WHERE starts_with(u.org_id::text, '{{ORG_PREFIX}}') AND u.provider = 'github' AND u.dataset_key IN ('tests', 'cicd')
      AND u.created_at > src.updated_at
)
SELECT count(*) AS units_after_t0,
       count(*) FILTER (WHERE status = 'success') AS success_after_t0,
       count(*) FILTER (WHERE status = 'success' AND excluded_prefix > 0) AS with_prefix_exclusions,
       COALESCE(max(excluded_prefix) FILTER (WHERE status = 'success'), 0) AS max_excluded_prefix,
       COALESCE(to_char(max(created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI'), '') AS last_unit_minute
FROM units;

-- C1 control: units of that source of any time and status. WANT source_units > 0 (the source and its units exist).
SELECT count(*) AS source_units
FROM public.integration_sources s
JOIN public.sync_run_units u ON u.source_id = s.id
WHERE starts_with(s.org_id::text, '{{ORG_PREFIX}}') AND starts_with(s.integration_id::text, '{{INTEGRATION_PREFIX}}')
  AND (lower(s.external_id) = '{{REPO}}' OR lower(s.full_name) = '{{REPO}}')
  AND starts_with(u.org_id::text, '{{ORG_PREFIX}}');

ROLLBACK;
`
)

// Synthetic ids: org prefix f1455bbb, the write target config c1455bbb, its integration f1455b06; another org e1455bbb.
const (
	write14559Org          = "f1455bbb-1455-4000-8000-0000000000e1"
	write14559OtherOrg     = "e1455bbb-1455-4000-8000-0000000000e2"
	write14559Integration  = "f1455b06-1455-4000-8000-0000000000a6"
	write14559Config       = "c1455bbb-1455-4000-8000-0000000000c1"
	write14559Child        = "c1455bbc-1455-4000-8000-0000000000c2"
	write14559OtherConfig  = "c1455bbb-1455-4000-8000-0000000000c9"
	write14559Source       = "5a455b06-1455-4000-8000-000000000001"
	write14559Repo         = "acme/ci-repo"
	write14559ListText     = `"ci_artifact_exclude_prefixes": ["go-images-", "go-test-timings-", "go-junit-part-"]`
	write14559FullUUIDExpr = `[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`
)

type write14559Piece struct{ id, text string }

func write14559SQLLines(text string) string {
	var out strings.Builder
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "--") {
			out.WriteString(line + "\n")
		}
	}
	return out.String()
}

// write14559Pieces splits a FORM as psql runs it: a statement ends at a SQL line (not a "--" line) ending with ";".
func write14559Pieces(t *testing.T, text string, ids *regexp.Regexp) []write14559Piece {
	t.Helper()
	var pieces []write14559Piece
	seen := map[string]bool{}
	var current []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "--") || !strings.HasSuffix(strings.TrimRight(line, " "), ";") {
			current = append(current, line)
			continue
		}
		current = append(current, strings.TrimSuffix(strings.TrimRight(line, " "), ";"))
		statement := strings.TrimSpace(strings.Join(current, "\n"))
		current = nil
		id := ""
		for _, l := range strings.Split(statement, "\n") {
			if fields := strings.Fields(l); strings.HasPrefix(l, "--") && id == "" && len(fields) > 1 && ids.MatchString(fields[1]) {
				id = fields[1]
			}
		}
		sqlText := strings.TrimSpace(write14559SQLLines(statement))
		switch {
		case strings.HasPrefix(sqlText, "BEGIN "):
			id = "BEGIN"
		case sqlText == "ROLLBACK":
			id = "ROLLBACK"
		}
		if id == "" || seen[id] {
			t.Fatalf("a piece with no id, or id %q twice:\n%s", id, statement)
		}
		seen[id] = true
		pieces = append(pieces, write14559Piece{id: id, text: statement})
	}
	if strings.TrimSpace(write14559SQLLines(strings.Join(current, "\n"))) != "" {
		t.Fatalf("SQL after the last statement")
	}
	return pieces
}

// write14559Prepare checks the embedded text, the org slots per statement and the banned words, and puts the synthetic
// values in.
func write14559Prepare(t *testing.T, name, text, sum string, ids *regexp.Regexp, wantIDs string) []write14559Piece {
	t.Helper()
	got := sha256.Sum256([]byte(text))
	if hex.EncodeToString(got[:]) != sum {
		t.Fatalf("%s: sha256 of the embedded text = %s, want %s", name, hex.EncodeToString(got[:]), sum)
	}
	pieces := write14559Pieces(t, text, ids)
	var names []string
	orgFilter := regexp.MustCompile(`starts_with\([a-z]+\.(org_id|id)::text, '\{\{ORG_PREFIX\}\}'\)`)
	for index, piece := range pieces {
		names = append(names, piece.id)
		sqlLines := write14559SQLLines(piece.text)
		if slots, filters := strings.Count(sqlLines, "{{ORG_PREFIX}}"), len(orgFilter.FindAllString(sqlLines, -1)); slots != filters {
			t.Errorf("%s %s: %d org slot(s), %d org filter(s)", name, piece.id, slots, filters)
		}
		upper := strings.ToUpper(sqlLines)
		for _, banned := range []string{"SET ", "INSERT ", "UPDATE ", "DELETE ", "EXPLAIN", "COPY ", "CREATE ", "DROP ", "ALTER ", "::JSONB"} {
			if strings.Contains(upper, banned) {
				t.Errorf("%s %s holds %q", name, piece.id, banned)
			}
		}
		filled := strings.NewReplacer("{{ORG_PREFIX}}", write14559Org[:8], "{{CONFIG_PREFIX}}", write14559Config[:8],
			"{{INTEGRATION_PREFIX}}", write14559Integration[:8], "{{REPO}}", write14559Repo).Replace(piece.text)
		if strings.Contains(filled, "{{") {
			t.Errorf("%s %s: a placeholder is left", name, piece.id)
		}
		pieces[index].text = filled
	}
	if got := strings.Join(names, ","); got != wantIDs {
		t.Fatalf("%s pieces = %s, want %s", name, got, wantIDs)
	}
	return pieces
}

type write14559Result struct {
	columns []string
	rows    [][]string
}

// write14559Run runs the pieces in one transaction, replacing from with to in piece replaceID (every occurrence).
func write14559Run(ctx context.Context, t *testing.T, conn *pgx.Conn, pieces []write14559Piece, replaceID, from, to string) (map[string]write14559Result, error) {
	t.Helper()
	results := map[string]write14559Result{}
	for _, piece := range pieces {
		text := piece.text
		if piece.id == replaceID {
			if !strings.Contains(text, from) {
				t.Fatalf("%s does not hold %q", piece.id, from)
			}
			text = strings.ReplaceAll(text, from, to)
		}
		if piece.id == "BEGIN" || piece.id == "ROLLBACK" {
			if _, err := conn.Exec(ctx, text); err != nil {
				t.Fatalf("%s: %v", piece.id, err)
			}
			continue
		}
		rows, err := conn.Query(ctx, text)
		if err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return results, fmt.Errorf("%s: %w", piece.id, err)
		}
		var result write14559Result
		for _, field := range rows.FieldDescriptions() {
			result.columns = append(result.columns, field.Name)
		}
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				_, _ = conn.Exec(ctx, "ROLLBACK")
				return results, fmt.Errorf("%s: %w", piece.id, err)
			}
			row := make([]string, len(values))
			for i, value := range values {
				row[i] = fmt.Sprint(value)
			}
			result.rows = append(result.rows, row)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return results, fmt.Errorf("%s: %w", piece.id, err)
		}
		results[piece.id] = result
	}
	return results, nil
}

func write14559Rows(results map[string]write14559Result, id string) string {
	var out []string
	for _, row := range results[id].rows {
		var cells []string
		for i, name := range results[id].columns {
			cells = append(cells, name+"="+row[i])
		}
		out = append(out, strings.Join(cells, " "))
	}
	return strings.Join(out, "\n")
}

// write14559Seed writes the synthetic world: two orgs; the write target (a GitHub planner-managed parent) and a child of
// the same integration; an integration of the other org with a config of the same 8-char prefix; the repository source
// with one unit two days old.
func write14559Seed(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for _, org := range []string{write14559Org, write14559OtherOrg} {
		exec(`INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1::uuid, 'org-' || $1::text, 'org', 'enterprise', true, now(), now())`, org)
	}
	exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'github', 'fx', '{"fetch_comments": true}'::json, true, now() - interval '1 day', now() - interval '1 day')`, write14559Integration, write14559Org)
	exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ('f1455b07-1455-4000-8000-0000000000a7'::uuid, $1, 'github', 'fx', '{}'::json, true, now(), now())`, write14559OtherOrg)
	config := func(id, org, integration string, parent *string, plannerManaged bool) {
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, parent_id, created_at, updated_at)
VALUES ($1::uuid, $2, 'fx-' || $1::text, 'github', '["tests"]'::json, '{"all_repos": true}'::json, true, $3, $4::uuid, $5::uuid, now(), now())`,
			id, org, plannerManaged, integration, parent)
	}
	config(write14559Config, write14559Org, write14559Integration, nil, true)
	parent := write14559Config
	config(write14559Child, write14559Org, write14559Integration, &parent, false)
	config(write14559OtherConfig, write14559OtherOrg, "f1455b07-1455-4000-8000-0000000000a7", nil, true)
	exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, 'github', 'repo', $4, $4, $4, '{}'::json, true, now(), now())`, write14559Source, write14559Org, write14559Integration, write14559Repo)
	exec(`INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
VALUES ('5c455b06-1455-4000-8000-000000000001'::uuid, $1, $2::uuid, 'schedule', 'incremental', 'success', 1, 1, 0, now() - interval '2 days')`, write14559Org, write14559Integration)
	exec(`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, result, created_at, updated_at)
VALUES ('0e455b06-1455-4000-8000-000000000001'::uuid, $1, '5c455b06-1455-4000-8000-000000000001'::uuid, $2::uuid, $3::uuid, 'github', 'tests', 'heavy', 'incremental', 'success', 1,
        '{"excluded_non_report_prefix": 9}'::json, now() - interval '2 days', now() - interval '2 days')`, write14559Org, write14559Integration, write14559Source)
	for _, table := range []string{"organizations", "integrations", "sync_configurations", "integration_sources", "sync_run_units"} {
		exec(`ANALYZE ` + table)
	}
}

// write14559Verb runs `dho admin <args>` through the dho admin command tree, as the card's argv.
func write14559Verb(t *testing.T, db *database, args ...string) (int, string, string) {
	t.Helper()
	env := map[string]string{"MIGRATION_DATABASE_URI": db.uri}
	var stdout, stderr bytes.Buffer
	code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
		Args: append([]string{"admin"}, args...), Lookup: func(key string) (string, bool) { value, ok := env[key]; return value, ok },
		Stdout: &stdout, Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

func write14559IntegrationConfig(ctx context.Context, t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var text string
	if err := conn.QueryRow(ctx, `SELECT config::text FROM integrations WHERE id = $1::uuid`, write14559Integration).Scan(&text); err != nil {
		t.Fatal(err)
	}
	return text
}

// The card's argv, executed: the dry-run prints the plan (one JSON line, 8-char ids) and writes nothing; the real run
// writes the 3 prefixes into the integration's config; a re-run is byte-equal and reports no change; --clear restores
// the stored config; no output line holds a full uuid or the word "organization ".
func TestCard14559WriteArgvExecutes(t *testing.T) {
	ctx := context.Background()
	db := startDatabase(t)
	write14559Seed(ctx, t, db.conn)
	before := write14559IntegrationConfig(ctx, t, db.conn)
	fullUUID := regexp.MustCompile(write14559FullUUIDExpr)
	set := []string{"sync-configs", "artifact-excludes", "set", "--config-id", write14559Config,
		"--prefix", "go-images-", "--prefix", "go-test-timings-", "--prefix", "go-junit-part-"}
	run := func(label string, extra ...string) map[string]any {
		t.Helper()
		code, stdout, stderr := write14559Verb(t, db, append(append([]string{}, set...), extra...)...)
		if code != 0 || stderr != "" {
			t.Fatalf("%s: exit %d stderr %q", label, code, stderr)
		}
		if fullUUID.MatchString(stdout) || strings.Contains(stdout, "organization ") || strings.Count(stdout, "\n") != 1 {
			t.Fatalf("%s: stdout %q: want one line, no full uuid, no \"organization \"", label, stdout)
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(stdout), &line); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		for key, want := range map[string]any{"action": "set", "config_id_prefix": write14559Config[:8], "integration_id_prefix": write14559Integration[:8],
			"org_id_prefix": write14559Org[:8], "prefix_count": float64(3), "sync_options_changed": false} {
			if line[key] != want {
				t.Errorf("%s: %s = %v, want %v (line %s)", label, key, line[key], want, stdout)
			}
		}
		return line
	}
	plan := run("dry-run", "--dry-run", "--reason", "card exec dry-run")
	if plan["dry_run"] != true || plan["integration_config_changed"] != true {
		t.Errorf("dry-run: %v, want dry_run true and integration_config_changed true", plan)
	}
	if got := write14559IntegrationConfig(ctx, t, db.conn); got != before {
		t.Fatalf("the dry-run wrote %s, want %s", got, before)
	}
	written := run("real", "--reason", "card exec")
	want := `{"fetch_comments": true, ` + write14559ListText + `}`
	if written["dry_run"] != false || written["integration_config_changed"] != true || write14559IntegrationConfig(ctx, t, db.conn) != want {
		t.Fatalf("real: %v, stored %s, want %s", written, write14559IntegrationConfig(ctx, t, db.conn), want)
	}
	again := run("re-run", "--reason", "card exec again")
	if again["integration_config_changed"] != false || write14559IntegrationConfig(ctx, t, db.conn) != want {
		t.Fatalf("re-run: %v, stored %s, want byte-equal %s", again, write14559IntegrationConfig(ctx, t, db.conn), want)
	}
	code, stdout, stderr := write14559Verb(t, db, "sync-configs", "artifact-excludes", "set", "--config-id", write14559Config, "--clear", "--reason", "card exec rollback")
	if code != 0 || stderr != "" || fullUUID.MatchString(stdout) || write14559IntegrationConfig(ctx, t, db.conn) != before {
		t.Fatalf("--clear: exit %d stdout %q stderr %q, stored %s, want %s", code, stdout, stderr, write14559IntegrationConfig(ctx, t, db.conn), before)
	}
}

// write14559Gates fires G0 once per cap literal (lowered to 0), per needed index (renamed: not found) and for an org
// prefix that names no organization. WANT SQLSTATE 22P02 with STOP-G0 before any later statement.
func write14559Gates(ctx context.Context, t *testing.T, conn *pgx.Conn, pieces []write14559Piece, caps []string, indexes map[string]string) {
	t.Helper()
	type fire struct{ from, to, want string }
	var fires []fire
	for _, literal := range caps {
		fires = append(fires, fire{literal, "0::bigint", "STOP-G0"})
	}
	for name, want := range indexes {
		fires = append(fires, fire{"'" + name + "'", "'" + name + "_absent'", want})
	}
	fires = append(fires, fire{"starts_with(o.id::text, '" + write14559Org[:8] + "')", "starts_with(o.id::text, 'f1455bbz')", "orgs=0"})
	for _, f := range fires {
		if strings.Count(pieces[1].text, f.from) != 1 {
			t.Fatalf("G0 holds %q %d time(s), want 1", f.from, strings.Count(pieces[1].text, f.from))
		}
		results, err := write14559Run(ctx, t, conn, pieces, "G0", f.from, f.to)
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "22P02" || !strings.Contains(pgErr.Message, "STOP-G0") || !strings.Contains(pgErr.Message, f.want) {
			t.Errorf("GATE %s -> %s: err = %v, want 22P02 STOP-G0 with %q", f.from, f.to, err, f.want)
			continue
		}
		if len(results) != 0 {
			t.Errorf("GATE %s: statements ran before the gate", f.from)
		}
	}
}

// FORM-14559-G0-1 as pm runs it: every WANT on the synthetic world before the write, each statement's org filter and
// key filters bite (mutants), and the gate fires on each cap, the index and the org count.
func TestFormExec14559G0FindsTheWriteTarget(t *testing.T) {
	ctx := context.Background()
	db := startDatabase(t)
	write14559Seed(ctx, t, db.conn)
	pieces := write14559Prepare(t, "G0", write14559G0SQL, write14559G0SHA256, regexp.MustCompile(`^(G0|S[12])$`), "BEGIN,G0,S1,S2,ROLLBACK")
	results, err := write14559Run(ctx, t, db.conn, pieces, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{
		"G0": "cost_gate_ok=0",
		"S1": "configs=1 config_id=" + write14559Config + " integration_prefix=" + write14559Integration[:8],
		"S2": "integrations=1 integration_key_set=false configs_of_integration=2 configs_with_key_in_options=0",
	} {
		if got := write14559Rows(results, id); got != want {
			t.Errorf("%s = %s, want %s", id, got, want)
		}
	}
	for _, mutant := range []struct{ name, id, from, to string }{
		{"s1-other-org", "S1", "starts_with(c.org_id::text, '" + write14559Org[:8] + "')", "starts_with(c.org_id::text, '" + write14559OtherOrg[:8] + "')"},
		{"s1-provider", "S1", "lower(c.provider) = 'github'", "lower(c.provider) = 'gitlab'"},
		{"s1-parent-filter", "S1", "c.planner_managed AND c.parent_id IS NULL", "NOT c.planner_managed"},
		{"s2-other-org", "S2", "starts_with(i.org_id::text, '" + write14559Org[:8] + "')", "starts_with(i.org_id::text, '" + write14559OtherOrg[:8] + "')"},
		{"s2-integration-key", "S2", `strpos(i.config::text, '"ci_artifact_exclude_prefixes"')`, `strpos(i.config::text, '"fetch_comments"')`},
		{"s2-config-key", "S2", `strpos(c.sync_options::text, '"ci_artifact_exclude_prefixes"')`, `strpos(c.sync_options::text, '"all_repos"')`},
		{"s2-config-org", "S2", "starts_with(c.org_id::text, '" + write14559Org[:8] + "')", "starts_with(c.org_id::text, '" + write14559OtherOrg[:8] + "')"},
	} {
		mutated, err := write14559Run(ctx, t, db.conn, pieces, mutant.id, mutant.from, mutant.to)
		if err != nil {
			t.Fatalf("MUTANT %s: %v", mutant.name, err)
		}
		if write14559Rows(mutated, mutant.id) == write14559Rows(results, mutant.id) {
			t.Errorf("MUTANT %s SURVIVED: %s", mutant.name, write14559Rows(mutated, mutant.id))
		}
	}
	write14559Gates(ctx, t, db.conn, pieces, []string{"100000::bigint", "268435456::bigint", "150000::bigint", "402653184::bigint", "200000::bigint", "536870912::bigint"},
		map[string]string{"ix_sync_configurations_integration_id": "index_ready=0"})
}

// FORM-14559-READBACK-1 as pm runs it, after the card's write: R1 shows the exact list in the integration and no copy in
// a config; R2 counts the source's GitHub tests units created after T0 (the integration's updated_at) and their
// excluded_non_report_prefix (a unit before T0 is not counted; a result holding "\u0000" reads without error); C1 sees
// the source's units. Before the write R1 misses its WANT. Each statement's filters bite; the gate fires.
func TestFormExec14559ReadbackReadsTheWrittenList(t *testing.T) {
	ctx := context.Background()
	db := startDatabase(t)
	write14559Seed(ctx, t, db.conn)
	pieces := write14559Prepare(t, "READBACK", write14559ReadbackSQL, write14559ReadbackSHA256, regexp.MustCompile(`^(G0|R[12]|C1)$`), "BEGIN,G0,R1,R2,C1,ROLLBACK")
	results, err := write14559Run(ctx, t, db.conn, pieces, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := write14559Rows(results, "R1"); !strings.Contains(got, "integration_key_set=false list_exact=false") {
		t.Errorf("R1 before the write = %s, want the WANT to miss", got)
	}
	if code, stdout, stderr := write14559Verb(t, db, "sync-configs", "artifact-excludes", "set", "--config-id", write14559Config,
		"--prefix", "go-images-", "--prefix", "go-test-timings-", "--prefix", "go-junit-part-", "--reason", "readback exec"); code != 0 {
		t.Fatalf("write: %d %s %s", code, stdout, stderr)
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := db.conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for index, result := range []string{`{"excluded_non_report_prefix": 3, "x": "\u0000"}`, `{"excluded_non_report_prefix": 0}`, `null`} {
		exec(`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, result, created_at, updated_at)
VALUES ($1::uuid, $2, '5c455b06-1455-4000-8000-000000000001'::uuid, $3::uuid, $4::uuid, 'github', 'tests', 'heavy', 'incremental', 'success', 1,
        $5::json, now() + interval '1 minute', now() + interval '1 minute')`,
			fmt.Sprintf("0e455b06-1455-4000-8000-0000000001%02d", index), write14559Org, write14559Integration, write14559Source, result)
	}
	exec(`ANALYZE sync_run_units`)
	results, err = write14559Run(ctx, t, db.conn, pieces, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := write14559Rows(results, "R1"); !strings.HasPrefix(got, "integrations=1 integration_key_set=true list_exact=true configs_of_integration=2 configs_with_key_in_options=0 t0_minute=") {
		t.Errorf("R1 = %s", got)
	}
	if got, want := write14559Rows(results, "R2"), "units_after_t0=3 success_after_t0=3 with_prefix_exclusions=1 max_excluded_prefix=3"; !strings.HasPrefix(got, want) {
		t.Errorf("R2 = %s, want prefix %s", got, want)
	}
	if got := write14559Rows(results, "C1"); got != "source_units=4" {
		t.Errorf("C1 = %s, want source_units=4", got)
	}
	for _, mutant := range []struct{ name, id, from, to string }{
		{"r1-other-org", "R1", "starts_with(i.org_id::text, '" + write14559Org[:8] + "')", "starts_with(i.org_id::text, '" + write14559OtherOrg[:8] + "')"},
		{"r1-list-order", "R1", `["go-images-", "go-test-timings-", "go-junit-part-"]`, `["go-test-timings-", "go-images-", "go-junit-part-"]`},
		{"r1-config-key", "R1", `strpos(c.sync_options::text, '"ci_artifact_exclude_prefixes"')`, `strpos(c.sync_options::text, '"all_repos"')`},
		{"r2-other-org", "R2", "starts_with(u.org_id::text, '" + write14559Org[:8] + "')", "starts_with(u.org_id::text, '" + write14559OtherOrg[:8] + "')"},
		{"r2-before-t0", "R2", "u.created_at > src.updated_at", "u.created_at <= src.updated_at"},
		{"r2-other-repo", "R2", "lower(s.external_id) = '" + write14559Repo + "' OR lower(s.full_name) = '" + write14559Repo + "'", "lower(s.external_id) = 'acme/other'"},
		{"r2-counter-name", "R2", "'excluded_non_report_prefix') = 'number'", "'excluded_non_report_suffix') = 'number'"},
		{"c1-other-org", "C1", "starts_with(u.org_id::text, '" + write14559Org[:8] + "')", "starts_with(u.org_id::text, '" + write14559OtherOrg[:8] + "')"},
	} {
		mutated, err := write14559Run(ctx, t, db.conn, pieces, mutant.id, mutant.from, mutant.to)
		if err != nil {
			t.Fatalf("MUTANT %s: %v", mutant.name, err)
		}
		if write14559Rows(mutated, mutant.id) == write14559Rows(results, mutant.id) {
			t.Errorf("MUTANT %s SURVIVED: %s", mutant.name, write14559Rows(mutated, mutant.id))
		}
	}
	write14559Gates(ctx, t, db.conn, pieces,
		[]string{"100000::bigint", "268435456::bigint", "150000::bigint", "402653184::bigint", "200000::bigint", "536870912::bigint",
			"3000000::bigint", "2147483648::bigint", "5000000::bigint", "4294967296::bigint"},
		map[string]string{"ix_sync_configurations_integration_id": "indexes_ready=1", "ix_sync_run_units_source_id": "indexes_ready=1"})
}
