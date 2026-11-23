//go:build integration

package adminops

import (
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
)

// DO NOT MERGE. This file executes FORM-14559-CONFIGS-3.sql in one READ ONLY transaction on a fresh database at the
// head chain. The text below is the .sql with its org prefix replaced by {{ORG_PREFIX}} and its quoted repository name
// by '{{REPO}}', nothing else (its sha256 is checked); the cell puts synthetic values in at run time and checks that
// every starts_with of every statement got the prefix. With synthetic integrations it wants S1 to list exactly the
// GitHub integrations of the org that hold a source of the repository, with their dataset switches, their parent
// configuration and whether the list is written, and the catalog gate to fire when a cap is crossed (pm-guard
// FORM-TEMPLATE-3, PostgreSQL form).
const (
	formExec14559SHA256      = "dbd31add597aa82dab62bd43c43a4d79d855ace3e918ef5fc32e143f471c8165"
	formExec14559Placeholder = "{{ORG_PREFIX}}"
	formExec14559RepoSlot    = "{{REPO}}"
	formExec14559SQL         = `-- FORM-14559-CONFIGS-3 (lane-14559, 2026-11-23, live pin a7febdf50002; key owner PR1 #6953 head ea45078a8823). PostgreSQL
-- prod, READ ONLY, CHAOS-14559. Which GitHub integrations of org {{ORG_PREFIX}} sync the repository full-chaos/dev-health-ops
-- (whose CI uploads the go-images-, go-test-timings- and go-junit-part- artifacts), whether their tests or cicd dataset
-- is on, which sync configurations each one has, and whether the exclude list is written already. The list is stored
-- once per integration (integrations.config; it applies to every sync config of the integration).
-- Replaces FORM-14559-CONFIGS-2 (269bed6625d7, acp NOT OK): no jsonb cast (the key test is strpos on the stored text),
-- S1 reads each table once (MATERIALIZED CTEs), orgs = 1 is a G0 STOP, github_configs replaces the parent STOP.
-- WANT text and work math: FORM-14559-CONFIGS-3.md.
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a), PostgreSQL form (b). Run line:
-- psql -X -v ON_ERROR_STOP=1 -f FORM-14559-CONFIGS-3.sql (on the primary; one transaction, ends ROLLBACK).
-- No SET, EXPLAIN, cast to jsonb or write. Output: counts, booleans and 8-char id prefixes only; no stored text is
-- selected or parsed. Org prefix {{ORG_PREFIX}} by starts_with on every FROM with org_id.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog cost gate (reads pg_catalog) and ORG-1 (one count of organizations). WANT cost_gate_ok = 0. Caps
-- (reltuples / heap + TOAST bytes): organizations 100000 / 268435456, integrations 150000 / 402653184,
-- sync_configurations 200000 / 536870912, integration_datasets 2000000 / 1073741824, integration_sources 3000000 /
-- 2147483648. A table missing, never analyzed (reltuples -1), over a cap, or orgs <> 1 = the named cast error STOP-G0
-- (ON_ERROR_STOP ends the file); the text holds table sizes and the org count only.
WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
rel AS (
    SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
           c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    CROSS JOIN bs
    LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
    WHERE n.nspname = 'public' AND c.relkind = 'r'
      AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets', 'integration_sources')
),
caps (relname, max_rows, max_bytes) AS (
    VALUES ('organizations', 100000::bigint, 268435456::bigint),
           ('integrations', 150000::bigint, 402653184::bigint),
           ('sync_configurations', 200000::bigint, 536870912::bigint),
           ('integration_datasets', 2000000::bigint, 1073741824::bigint),
           ('integration_sources', 3000000::bigint, 2147483648::bigint)
),
org AS (
    SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, '{{ORG_PREFIX}}')
)
SELECT CASE WHEN count(rel.relname) = 5
             AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
             AND max(org.orgs) = 1
            THEN 0
            ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                           || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                  || ' orgs=' || max(org.orgs)::text)::int
       END AS cost_gate_ok
FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN org;

-- C0 INFO the sizes the gate read (pg_catalog only), for the work math. WANT 5 rows.
SELECT c.relname, c.reltuples::bigint AS reltuples, c.relpages::bigint AS heap_pages
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r'
  AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets', 'integration_sources')
ORDER BY c.relname;

-- S0 INFO ORG-1 (G0 stops unless it is 1). WANT orgs = 1.
SELECT count(*) AS orgs
FROM public.organizations o
WHERE starts_with(o.id::text, '{{ORG_PREFIX}}');

-- S1 one row per GitHub integration of the org that holds a source of the repository (external_id or full_name,
-- case-insensitive). Each table is read ONCE (one MATERIALIZED CTE per table, then hash joins on integration id).
-- WANT >= 1 row; per row github_configs >= 1 (0 = STOP: no configuration the write can name). Every row with
-- integration_key_set false is a write of the card (any dataset switch: a manual trigger plans a dataset with no row).
-- configs_with_key_in_options WANT 0. The key tests are strpos on the stored text: nothing is parsed, nothing is selected.
WITH ints AS MATERIALIZED (
    SELECT i.id, i.is_active,
           strpos(i.config::text, '"ci_artifact_exclude_prefixes"') > 0 AS integration_key_set
    FROM public.integrations i
    WHERE starts_with(i.org_id::text, '{{ORG_PREFIX}}') AND lower(i.provider) = 'github'
),
src AS MATERIALIZED (
    SELECT s.integration_id, count(*) FILTER (WHERE s.is_enabled) AS repo_sources_enabled
    FROM public.integration_sources s
    WHERE starts_with(s.org_id::text, '{{ORG_PREFIX}}')
      AND (lower(s.external_id) = '{{REPO}}' OR lower(s.full_name) = '{{REPO}}')
    GROUP BY s.integration_id
),
ds AS MATERIALIZED (
    SELECT d.integration_id,
           bool_or(d.dataset_key = 'tests' AND d.is_enabled) AS tests_on,
           bool_or(d.dataset_key = 'cicd' AND d.is_enabled) AS cicd_on
    FROM public.integration_datasets d
    WHERE starts_with(d.org_id::text, '{{ORG_PREFIX}}') AND d.dataset_key IN ('tests', 'cicd')
    GROUP BY d.integration_id
),
cfg AS MATERIALIZED (
    SELECT c.integration_id,
           count(*) FILTER (WHERE lower(c.provider) = 'github') AS github_configs,
           count(*) FILTER (WHERE c.planner_managed AND c.parent_id IS NULL) AS parent_configs,
           count(*) FILTER (WHERE c.planner_managed AND c.parent_id IS NULL AND c.is_active) AS parent_configs_active,
           COALESCE(string_agg(left(c.id::text, 8), ' ' ORDER BY c.id) FILTER (WHERE c.planner_managed AND c.parent_id IS NULL), '') AS parent_config_prefixes,
           count(*) FILTER (WHERE strpos(c.sync_options::text, '"ci_artifact_exclude_prefixes"') > 0) AS configs_with_key_in_options
    FROM public.sync_configurations c
    WHERE starts_with(c.org_id::text, '{{ORG_PREFIX}}') AND c.integration_id IS NOT NULL
    GROUP BY c.integration_id
)
SELECT left(ints.id::text, 8) AS integration_prefix,
       ints.is_active AS integration_active,
       COALESCE(ds.tests_on, false) AS tests_on,
       COALESCE(ds.cicd_on, false) AS cicd_on,
       src.repo_sources_enabled,
       COALESCE(cfg.github_configs, 0) AS github_configs,
       COALESCE(cfg.parent_configs, 0) AS parent_configs,
       COALESCE(cfg.parent_configs_active, 0) AS parent_configs_active,
       COALESCE(cfg.parent_config_prefixes, '') AS parent_config_prefixes,
       COALESCE(cfg.configs_with_key_in_options, 0) AS configs_with_key_in_options,
       ints.integration_key_set
FROM ints
JOIN src ON src.integration_id = ints.id
LEFT JOIN ds ON ds.integration_id = ints.id
LEFT JOIN cfg ON cfg.integration_id = ints.id
ORDER BY integration_prefix;

-- C1 control: GitHub sources and integrations of the org, any repository. WANT github_sources > 0 and
-- github_integrations >= the S1 row count.
SELECT count(*) AS github_sources, count(DISTINCT s.integration_id) AS github_integrations
FROM public.integration_sources s
WHERE starts_with(s.org_id::text, '{{ORG_PREFIX}}') AND lower(s.provider) = 'github';

ROLLBACK;
`
)

type formExec14559Piece struct {
	id, text string
}

// formExec14559Pieces splits the FORM as psql runs it: a statement ends at a SQL line (not a "--" comment line) that
// ends with ";". A data statement is named by its first comment line "-- <id> "; BEGIN and ROLLBACK by their own word.
func formExec14559Pieces(t *testing.T, text string) []formExec14559Piece {
	t.Helper()
	statementID := regexp.MustCompile(`^(S[01]|C[01]|G0)$`)
	var pieces []formExec14559Piece
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
		for _, line := range strings.Split(statement, "\n") {
			if fields := strings.Fields(line); strings.HasPrefix(line, "--") && id == "" && len(fields) > 1 && statementID.MatchString(fields[1]) {
				id = fields[1]
			}
		}
		sqlText := strings.TrimSpace(formExec14559SQLLines(statement))
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
		pieces = append(pieces, formExec14559Piece{id: id, text: statement})
	}
	if strings.TrimSpace(formExec14559SQLLines(strings.Join(current, "\n"))) != "" {
		t.Fatalf("SQL after the last statement:\n%s", strings.Join(current, "\n"))
	}
	return pieces
}

// formExec14559SQLLines is the SQL of a piece without its comment lines.
func formExec14559SQLLines(text string) string {
	var out strings.Builder
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "--") {
			out.WriteString(line + "\n")
		}
	}
	return out.String()
}

type formExec14559Result struct {
	columns []string
	rows    [][]string
}

func formExec14559Query(ctx context.Context, conn *pgx.Conn, statement string) (formExec14559Result, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return formExec14559Result{}, err
	}
	defer rows.Close()
	var result formExec14559Result
	for _, field := range rows.FieldDescriptions() {
		result.columns = append(result.columns, field.Name)
	}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return formExec14559Result{}, err
		}
		row := make([]string, len(values))
		for index, value := range values {
			row[index] = fmt.Sprint(value)
		}
		result.rows = append(result.rows, row)
	}
	return result, rows.Err()
}

// formExec14559Run runs the pieces in order in one transaction, as pm runs the file. replaceID/from/to change one
// statement in a test copy (every occurrence of from, which must occur). It returns the results by id, or the first
// error (then the transaction is rolled back, as ON_ERROR_STOP ends the file).
func formExec14559Run(ctx context.Context, t *testing.T, conn *pgx.Conn, pieces []formExec14559Piece, replaceID, from, to string) (map[string]formExec14559Result, error) {
	t.Helper()
	results := map[string]formExec14559Result{}
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
		result, err := formExec14559Query(ctx, conn, text)
		if err != nil {
			if _, rollbackErr := conn.Exec(ctx, "ROLLBACK"); rollbackErr != nil {
				t.Fatalf("ROLLBACK after %s: %v", piece.id, rollbackErr)
			}
			return results, fmt.Errorf("%s: %w", piece.id, err)
		}
		for index, row := range result.rows {
			t.Logf("FORM-14559-CONFIGS-3 %s row %d %v %v", piece.id, index, result.columns, row)
		}
		results[piece.id] = result
	}
	return results, nil
}

// formExec14559Rows renders a statement's rows as "col=value" lists, one string per row.
func formExec14559Rows(results map[string]formExec14559Result, id string) []string {
	var out []string
	for _, row := range results[id].rows {
		var cells []string
		for index, name := range results[id].columns {
			cells = append(cells, name+"="+row[index])
		}
		out = append(out, strings.Join(cells, " "))
	}
	return out
}

// formExec14559PlanLoops reads an EXPLAIN (ANALYZE, FORMAT JSON) plan (run by the TEST only, never by the FORM) and
// returns, per table, the scan nodes and their largest actual loop count.
func formExec14559PlanLoops(t *testing.T, raw []byte) map[string][2]float64 {
	t.Helper()
	var plans []map[string]any
	if err := json.Unmarshal(raw, &plans); err != nil {
		t.Fatalf("plan: %v", err)
	}
	out := map[string][2]float64{}
	var walk func(node map[string]any)
	walk = func(node map[string]any) {
		if relation, ok := node["Relation Name"].(string); ok {
			loops, _ := node["Actual Loops"].(float64)
			seen := out[relation]
			seen[0]++
			if loops > seen[1] {
				seen[1] = loops
			}
			out[relation] = seen
		}
		children, _ := node["Plans"].([]any)
		for _, child := range children {
			if c, ok := child.(map[string]any); ok {
				walk(c)
			}
		}
	}
	for _, plan := range plans {
		if root, ok := plan["Plan"].(map[string]any); ok {
			walk(root)
		}
	}
	return out
}

// formExec14559S1Correlated is the S1 of FORM-14559-CONFIGS-2 (correlated subqueries per integration row), kept here
// only to prove the plan-shape check sees a per-row rescan.
const formExec14559S1Correlated = `SELECT left(i.id::text, 8) AS integration_prefix,
       (SELECT count(*) FROM public.integration_datasets d
        WHERE d.integration_id = i.id AND starts_with(d.org_id::text, 'f1455aaa')
          AND d.dataset_key = 'tests' AND d.is_enabled) > 0 AS tests_on,
       (SELECT count(*) FROM public.integration_sources s
        WHERE s.integration_id = i.id AND starts_with(s.org_id::text, 'f1455aaa') AND s.is_enabled
          AND (lower(s.external_id) = 'acme/ci-repo' OR lower(s.full_name) = 'acme/ci-repo')) AS repo_sources_enabled
FROM public.integrations i
WHERE starts_with(i.org_id::text, 'f1455aaa') AND lower(i.provider) = 'github'
  AND EXISTS (SELECT 1 FROM public.integration_sources s
              WHERE s.integration_id = i.id AND starts_with(s.org_id::text, 'f1455aaa')
                AND (lower(s.external_id) = 'acme/ci-repo' OR lower(s.full_name) = 'acme/ci-repo'))`

// TestFormExec14559Configs3ListsTheIntegrationsThatSyncTheRepository. Synthetic org prefix f1455aaa, other org e1455aaa,
// repository acme/ci-repo. In the org: integration a01 (github; an enabled source of the repository; tests on; one
// active parent config without the list; its config holds a "\u0000" value and a fake secret), a02 (github; a disabled
// source of the repository spelled in upper case; cicd on; the list on the integration config, a parent config, and a
// child config whose own sync_options holds a copy, a "\u0000" value and a fake secret), a06 (github; the repository;
// no dataset row; only a child config, no parent), a03 (github; another repository only), a04 (gitlab; a source of the
// repository). The other org: a05 (github; the repository). S1 WANT exactly a01, a02 and a06 with their values, and no
// stored text in any output or error; C1 WANT 4 github sources over 4 integrations.
func TestFormExec14559Configs3ListsTheIntegrationsThatSyncTheRepository(t *testing.T) {
	ctx := context.Background()
	db := startDatabase(t)
	conn := db.conn

	sum := sha256.Sum256([]byte(formExec14559SQL))
	if got := hex.EncodeToString(sum[:]); got != formExec14559SHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (FORM-14559-CONFIGS-3.sql with the org prefix as %s and the repository as %s)",
			got, formExec14559SHA256, formExec14559Placeholder, formExec14559RepoSlot)
	}
	const prefix, repo = "f1455aaa", "acme/ci-repo"
	pieces := formExec14559Pieces(t, formExec14559SQL)
	var ids []string
	for index, piece := range pieces {
		ids = append(ids, piece.id)
		sqlLines := formExec14559SQLLines(piece.text)
		placeholders, filters := strings.Count(sqlLines, formExec14559Placeholder), strings.Count(sqlLines, "starts_with(")
		if placeholders != filters {
			t.Errorf("%s: %d org prefix(es) for %d starts_with filter(s)", piece.id, placeholders, filters)
		}
		switch piece.id {
		case "G0", "S0", "S1", "C1":
			if placeholders == 0 {
				t.Errorf("%s: no org prefix", piece.id)
			}
		default:
			if placeholders != 0 {
				t.Errorf("%s: an org prefix in a statement that reads no org table", piece.id)
			}
		}
		wantRepo := 0
		if piece.id == "S1" {
			wantRepo = 2
		}
		if got := strings.Count(sqlLines, formExec14559RepoSlot); got != wantRepo {
			t.Errorf("%s: %d repository slot(s), want %d", piece.id, got, wantRepo)
		}
		pieces[index].text = strings.ReplaceAll(strings.ReplaceAll(piece.text, formExec14559Placeholder, prefix), formExec14559RepoSlot, repo)
		if strings.Contains(pieces[index].text, "{{") {
			t.Errorf("%s: a placeholder is left", piece.id)
		}
		upper := strings.ToUpper(formExec14559SQLLines(piece.text))
		for _, banned := range []string{"SET ", "INSERT ", "UPDATE ", "DELETE ", "EXPLAIN", "COPY ", "CREATE ", "DROP ", "ALTER ", "::JSONB", "->", "#>"} {
			if strings.Contains(upper, banned) {
				t.Errorf("%s holds %q", piece.id, banned)
			}
		}
	}
	if got := strings.Join(ids, ","); got != "BEGIN,G0,C0,S0,S1,C1,ROLLBACK" {
		t.Fatalf("pieces = %s", got)
	}

	const org, other = prefix + "-1455-4000-8000-0000000000e1", "e1455aaa-1455-4000-8000-0000000000e2"
	const secret = "zz9-fake-secret-token-zz9"
	integrationID := func(n int) string { return fmt.Sprintf("f1455a%02d-1455-4000-8000-0000000000a%d", n, n) }
	configID := func(n int) string { return fmt.Sprintf("c1455a%02d-1455-4000-8000-0000000000c%d", n, n) }
	sequence := 0
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for _, o := range []string{org, other} {
		exec(`INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1::uuid, 'org-' || $1::text, 'org', 'enterprise', true, now(), now())`, o)
	}
	integration := func(n int, orgID, provider, config string) {
		exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, $3, 'fx', $4::json, true, now(), now())`, integrationID(n), orgID, provider, config)
	}
	source := func(n int, orgID, provider, externalID string, enabled bool) {
		sequence++
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, $4, 'repo', $5, $5, $5, '{}'::json, $6, now(), now())`,
			fmt.Sprintf("5a455a%02d-1455-4000-8000-%012d", n, sequence), orgID, integrationID(n), provider, externalID, enabled)
	}
	dataset := func(n int, orgID, key string, enabled bool) {
		sequence++
		exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1::uuid, $2, $3::uuid, $4, $5, '{}'::json)`,
			fmt.Sprintf("da455a%02d-1455-4000-8000-%012d", n, sequence), orgID, integrationID(n), key, enabled)
	}
	config := func(id string, n int, orgID, provider, options string, parent *string) {
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, parent_id, created_at, updated_at)
VALUES ($1::uuid, $2, 'fx-' || $1::text, $3, '["tests"]'::json, $4::json, true, $5, $6::uuid, $7::uuid, now(), now())`,
			id, orgID, provider, options, parent == nil, integrationID(n), parent)
	}
	// A NUL escape the json type stores and jsonb refuses (22P05), next to a fake secret: no text of it may reach any
	// output or error of the FORM.
	nul := `"x": "\u0000", "token": "` + secret + `"`
	integration(1, org, "github", `{"fetch_comments": true, `+nul+`}`)
	source(1, org, "github", repo, true)
	dataset(1, org, "tests", true)
	dataset(1, org, "cicd", false)
	config(configID(1), 1, org, "github", `{"all_repos": true, `+nul+`}`, nil)
	integration(2, org, "github", `{"ci_artifact_exclude_prefixes": ["ci-images-"]}`)
	source(2, org, "github", strings.ToUpper(repo), false)
	dataset(2, org, "cicd", true)
	config(configID(2), 2, org, "github", `{"all_repos": true}`, nil)
	parent := configID(2)
	config(configID(7), 2, org, "github", `{"all_repos": true, "ci_artifact_exclude_prefixes": ["ci-images-"], `+nul+`}`, &parent)
	integration(6, org, "github", `{}`)
	source(6, org, "github", repo, true)
	orphanParent := configID(1)
	config(configID(6), 6, org, "github", `{}`, &orphanParent)
	integration(3, org, "github", `{}`)
	source(3, org, "github", "acme/other", true)
	dataset(3, org, "tests", true)
	config(configID(3), 3, org, "github", `{}`, nil)
	integration(4, org, "gitlab", `{}`)
	source(4, org, "gitlab", repo, true)
	dataset(4, org, "tests", true)
	integration(5, other, "github", `{}`)
	source(5, other, "github", repo, true)
	dataset(5, other, "tests", true)
	config(configID(5), 5, other, "github", `{}`, nil)
	for _, table := range []string{"organizations", "integrations", "sync_configurations", "integration_datasets", "integration_sources"} {
		exec(`ANALYZE ` + table)
	}

	// Every row of every statement as psql would print it, and every error text, must hold none of the stored text.
	leak := func(label string, results map[string]formExec14559Result, err error) {
		t.Helper()
		var all strings.Builder
		for _, result := range results {
			all.WriteString(strings.Join(result.columns, " "))
			for _, row := range result.rows {
				all.WriteString("\n" + strings.Join(row, " | "))
			}
		}
		if err != nil {
			all.WriteString("\n" + err.Error())
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				all.WriteString("\n" + pgErr.Detail + "\n" + pgErr.Where + "\n" + pgErr.Hint)
			}
		}
		for _, stored := range []string{secret, "zz9", `\u0000`, "fetch_comments", "all_repos", "ci-images-"} {
			if strings.Contains(all.String(), stored) {
				t.Errorf("%s: the output or an error holds stored text %q", label, stored)
			}
		}
	}

	results, err := formExec14559Run(ctx, t, conn, pieces, "", "", "")
	leak("phase1", results, err)
	if err != nil {
		t.Fatal(err)
	}
	if got := formExec14559Rows(results, "G0"); strings.Join(got, ";") != "cost_gate_ok=0" {
		t.Errorf("G0 = %v, want cost_gate_ok=0", got)
	}
	if got := len(results["C0"].rows); got != 5 {
		t.Errorf("C0: %d rows, want 5", got)
	}
	if got := formExec14559Rows(results, "S0"); strings.Join(got, ";") != "orgs=1" {
		t.Errorf("S0 = %v, want orgs=1", got)
	}
	wantS1 := []string{
		"integration_prefix=f1455a01 integration_active=true tests_on=true cicd_on=false repo_sources_enabled=1 github_configs=1 parent_configs=1 parent_configs_active=1 parent_config_prefixes=c1455a01 configs_with_key_in_options=0 integration_key_set=false",
		"integration_prefix=f1455a02 integration_active=true tests_on=false cicd_on=true repo_sources_enabled=0 github_configs=2 parent_configs=1 parent_configs_active=1 parent_config_prefixes=c1455a02 configs_with_key_in_options=1 integration_key_set=true",
		"integration_prefix=f1455a06 integration_active=true tests_on=false cicd_on=false repo_sources_enabled=1 github_configs=1 parent_configs=0 parent_configs_active=0 parent_config_prefixes= configs_with_key_in_options=0 integration_key_set=false",
	}
	if got := formExec14559Rows(results, "S1"); strings.Join(got, "\n") != strings.Join(wantS1, "\n") {
		t.Errorf("S1 =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(wantS1, "\n"))
	}
	if got := formExec14559Rows(results, "C1"); strings.Join(got, ";") != "github_sources=4 github_integrations=4" {
		t.Errorf("C1 = %v, want github_sources=4 github_integrations=4", got)
	}

	// Mutation cells on the same rows: each changes S1 or C1.
	for _, mutant := range []struct {
		name, id, from, to string
	}{
		{"s1-case-sensitive-repo", "S1", "lower(s.external_id) = '" + repo + "' OR lower(s.full_name) = '" + repo + "'", "s.external_id = '" + repo + "'"},
		{"s1-children-counted-as-parents", "S1", "count(*) FILTER (WHERE c.planner_managed AND c.parent_id IS NULL) AS parent_configs,", "count(*) AS parent_configs,"},
		{"s1-config-key-name", "S1", `strpos(c.sync_options::text, '"ci_artifact_exclude_prefixes"')`, `strpos(c.sync_options::text, '"ci_artifact_exclude_prefix"')`},
		{"s1-integration-key-name", "S1", `strpos(i.config::text, '"ci_artifact_exclude_prefixes"')`, `strpos(i.config::text, '"ci_artifact_exclude_prefix"')`},
		{"s1-any-provider", "S1", "AND lower(i.provider) = 'github'", ""},
		{"s1-other-org", "S1", prefix, "e1455aaa"},
		{"s1-dataset-ignores-switch", "S1", "bool_or(d.dataset_key = 'cicd' AND d.is_enabled)", "bool_or(d.dataset_key = 'cicd')"},
		{"s1-github-configs-any-provider", "S1", "count(*) FILTER (WHERE lower(c.provider) = 'github') AS github_configs", "count(*) FILTER (WHERE lower(c.provider) = 'gitlab') AS github_configs"},
		{"c1-no-provider", "C1", "lower(s.provider) = 'github'", "lower(s.provider) = 'none'"},
	} {
		mutated, err := formExec14559Run(ctx, t, conn, pieces, mutant.id, mutant.from, mutant.to)
		leak("MUTANT "+mutant.name, mutated, err)
		if err != nil {
			t.Fatalf("MUTANT %s: %v", mutant.name, err)
		}
		before, after := strings.Join(formExec14559Rows(results, mutant.id), "\n"), strings.Join(formExec14559Rows(mutated, mutant.id), "\n")
		t.Logf("MUTANT %s: %s =\n%s", mutant.name, mutant.id, after)
		if after == before {
			t.Errorf("MUTANT %s SURVIVED", mutant.name)
		}
	}

	// The jsonb cast the -2 text held is what the strpos test avoids: on these rows it errors (22P05).
	jsonbCast, err := formExec14559Run(ctx, t, conn, pieces, "S1",
		`strpos(i.config::text, '"ci_artifact_exclude_prefixes"') > 0`, `(i.config::jsonb -> 'ci_artifact_exclude_prefixes') IS NOT NULL`)
	var castErr *pgconn.PgError
	if err == nil || !errors.As(err, &castErr) || castErr.Code != "22P05" {
		t.Errorf("MUTANT restore-jsonb-cast: err = %v (results %v), want 22P05: the rows must hold what the cast refuses", err, jsonbCast)
	} else {
		t.Logf("MUTANT restore-jsonb-cast: fired %s", castErr.Code)
	}

	// The gate fires: each cap literal lowered to 0 in a test copy, and a prefix that names no organization. WANT the
	// named cast error STOP-G0 (SQLSTATE 22P02), raised by G0 before any later statement runs, its text sizes and the
	// org count only.
	for _, cell := range []struct{ from, to, want string }{
		{"100000::bigint", "0::bigint", "STOP-G0"}, {"268435456::bigint", "0::bigint", "STOP-G0"},
		{"150000::bigint", "0::bigint", "STOP-G0"}, {"402653184::bigint", "0::bigint", "STOP-G0"},
		{"200000::bigint", "0::bigint", "STOP-G0"}, {"536870912::bigint", "0::bigint", "STOP-G0"},
		{"2000000::bigint", "0::bigint", "STOP-G0"}, {"1073741824::bigint", "0::bigint", "STOP-G0"},
		{"3000000::bigint", "0::bigint", "STOP-G0"}, {"2147483648::bigint", "0::bigint", "STOP-G0"},
		{"starts_with(o.id::text, '" + prefix + "')", "starts_with(o.id::text, 'f1455aab')", "orgs=0"},
	} {
		if strings.Count(pieces[1].text, cell.from) != 1 {
			t.Fatalf("G0 holds %q %d time(s), want 1", cell.from, strings.Count(pieces[1].text, cell.from))
		}
		results, err := formExec14559Run(ctx, t, conn, pieces, "G0", cell.from, cell.to)
		leak("GATE "+cell.from, results, err)
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "22P02" || !strings.Contains(pgErr.Message, "STOP-G0") || !strings.Contains(pgErr.Message, cell.want) {
			t.Errorf("GATE %s -> %s: err = %v, want 22P02 STOP-G0 with %q", cell.from, cell.to, err, cell.want)
			continue
		}
		if len(results) != 0 {
			t.Errorf("GATE %s: statements ran before the gate: %v", cell.from, results)
		}
		t.Logf("GATE %s -> %s: fired: %s", cell.from, cell.to, pgErr.Message)
	}

	// Pass path: S1 reads each table once whatever the number of integrations (3 rows here). EXPLAIN ANALYZE runs in this
	// TEST only. The -2 correlated form, planted, shows the per-row rescan the check exists to catch.
	explain := func(text string) map[string][2]float64 {
		t.Helper()
		var raw []byte
		if err := conn.QueryRow(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+text).Scan(&raw); err != nil {
			t.Fatalf("explain: %v", err)
		}
		return formExec14559PlanLoops(t, raw)
	}
	for table, seen := range explain(formExec14559SQLLines(pieces[4].text)) {
		t.Logf("S1 plan: %s scanned by %v node(s), max loops %v", table, seen[0], seen[1])
		if seen[0] != 1 || seen[1] != 1 {
			t.Errorf("S1 reads %s with %v scan node(s) and up to %v loops, want one scan of one loop", table, seen[0], seen[1])
		}
	}
	rescans := 0
	for table, seen := range explain(formExec14559S1Correlated) {
		t.Logf("planted -2 S1 plan: %s scanned by %v node(s), max loops %v", table, seen[0], seen[1])
		if seen[1] > 1 {
			rescans++
		}
	}
	if rescans == 0 {
		t.Error("the planted correlated S1 shows no per-row rescan: the plan-shape check cannot see one")
	}
}
