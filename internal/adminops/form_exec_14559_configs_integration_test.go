//go:build integration

package adminops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DO NOT MERGE. This file executes FORM-14559-CONFIGS-1.sql in one READ ONLY transaction on a fresh database at the
// head chain. The text below is the .sql with its org prefix replaced by {{ORG_PREFIX}} and its quoted repository name
// by '{{REPO}}', nothing else (its sha256 is checked); the cell puts synthetic values in at run time and checks that
// every starts_with of every statement got the prefix. With synthetic integrations it wants S1 to list exactly the
// GitHub integrations of the org that hold a source of the repository, with their dataset switches, their parent
// configuration and whether the list is written, and the catalog gate to fire when a cap is crossed (pm-guard
// FORM-TEMPLATE-3, PostgreSQL form).
const (
	formExec14559SHA256      = "e67a0580398e58d9b53d0318a06a73727f70cd9f7ce13e9009988b88bb722539"
	formExec14559Placeholder = "{{ORG_PREFIX}}"
	formExec14559RepoSlot    = "{{REPO}}"
	formExec14559SQL         = `-- FORM-14559-CONFIGS-1 (lane-14559, 2026-11-23, ops pin fbcb83f002e7). PostgreSQL prod, READ ONLY, CHAOS-14559.
-- Which GitHub integrations of org {{ORG_PREFIX}} sync the repository full-chaos/dev-health-ops (whose CI uploads the
-- go-images-, go-test-timings- and go-junit-part- artifacts), whether their tests or cicd dataset is on, which parent
-- sync configuration each one has (the config the artifact-exclude write targets), and whether the list is written
-- already. WANT text and work math: FORM-14559-CONFIGS-1.md.
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a), PostgreSQL form (b). Run line:
-- psql -X -v ON_ERROR_STOP=1 -f FORM-14559-CONFIGS-1.sql (on the primary; one transaction, ends ROLLBACK).
-- No SET, EXPLAIN or write. Output: counts, booleans and 8-char id prefixes only. Org prefix {{ORG_PREFIX}} by starts_with
-- on every FROM with org_id.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog cost gate (reads pg_catalog only). WANT cost_gate_ok = 0. Caps (reltuples / heap + TOAST bytes):
-- organizations 100000 / 268435456, integrations 150000 / 402653184, sync_configurations 200000 / 536870912,
-- integration_datasets 2000000 / 1073741824, integration_sources 3000000 / 2147483648.
-- A table missing, never analyzed (reltuples -1) or over a cap = the named cast error STOP-G0 (ON_ERROR_STOP ends the file).
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
)
SELECT CASE WHEN count(rel.relname) = 5
             AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
            THEN 0
            ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                           || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname))::int
       END AS cost_gate_ok
FROM caps LEFT JOIN rel ON rel.relname = caps.relname;

-- C0 INFO the sizes the gate read (pg_catalog only), for the work math. WANT 5 rows.
SELECT c.relname, c.reltuples::bigint AS reltuples, c.relpages::bigint AS heap_pages
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r'
  AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets', 'integration_sources')
ORDER BY c.relname;

-- S0 ORG-1. WANT orgs = 1.
SELECT count(*) AS orgs
FROM public.organizations o
WHERE starts_with(o.id::text, '{{ORG_PREFIX}}');

-- S1 one row per GitHub integration of the org that holds a source of the repository (external_id or full_name,
-- case-insensitive). WANT >= 1 row; per row parent_configs = 1 (the write target: planner-managed, no parent_id).
-- A row with tests_on or cicd_on and parent_configs_with_key = 0 is a config the card writes; integration_key_set
-- says the integration config already holds the list.
SELECT left(i.id::text, 8) AS integration_prefix,
       i.is_active AS integration_active,
       (SELECT count(*) FROM public.integration_datasets d
        WHERE d.integration_id = i.id AND starts_with(d.org_id::text, '{{ORG_PREFIX}}')
          AND d.dataset_key = 'tests' AND d.is_enabled) > 0 AS tests_on,
       (SELECT count(*) FROM public.integration_datasets d
        WHERE d.integration_id = i.id AND starts_with(d.org_id::text, '{{ORG_PREFIX}}')
          AND d.dataset_key = 'cicd' AND d.is_enabled) > 0 AS cicd_on,
       (SELECT count(*) FROM public.integration_sources s
        WHERE s.integration_id = i.id AND starts_with(s.org_id::text, '{{ORG_PREFIX}}') AND s.is_enabled
          AND (lower(s.external_id) = '{{REPO}}' OR lower(s.full_name) = '{{REPO}}')) AS repo_sources_enabled,
       (SELECT count(*) FROM public.sync_configurations c
        WHERE c.integration_id = i.id AND starts_with(c.org_id::text, '{{ORG_PREFIX}}')
          AND c.planner_managed AND c.parent_id IS NULL) AS parent_configs,
       (SELECT count(*) FROM public.sync_configurations c
        WHERE c.integration_id = i.id AND starts_with(c.org_id::text, '{{ORG_PREFIX}}')
          AND c.planner_managed AND c.parent_id IS NULL AND c.is_active) AS parent_configs_active,
       (SELECT COALESCE(string_agg(left(c.id::text, 8), ' ' ORDER BY c.id), '') FROM public.sync_configurations c
        WHERE c.integration_id = i.id AND starts_with(c.org_id::text, '{{ORG_PREFIX}}')
          AND c.planner_managed AND c.parent_id IS NULL) AS parent_config_prefixes,
       (SELECT count(*) FROM public.sync_configurations c
        WHERE c.integration_id = i.id AND starts_with(c.org_id::text, '{{ORG_PREFIX}}')
          AND c.planner_managed AND c.parent_id IS NULL
          AND (c.sync_options::jsonb -> 'ci_artifact_exclude_prefixes') IS NOT NULL) AS parent_configs_with_key,
       (i.config::jsonb -> 'ci_artifact_exclude_prefixes') IS NOT NULL AS integration_key_set
FROM public.integrations i
WHERE starts_with(i.org_id::text, '{{ORG_PREFIX}}') AND lower(i.provider) = 'github'
  AND EXISTS (SELECT 1 FROM public.integration_sources s
              WHERE s.integration_id = i.id AND starts_with(s.org_id::text, '{{ORG_PREFIX}}')
                AND (lower(s.external_id) = '{{REPO}}' OR lower(s.full_name) = '{{REPO}}'))
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
			t.Logf("FORM-14559-CONFIGS-1 %s row %d %v %v", piece.id, index, result.columns, row)
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

// TestFormExec14559ConfigsListsTheIntegrationsThatSyncTheRepository. Synthetic org prefix f1455aaa, other org e1455aaa,
// repository acme/ci-repo. In the org: integration a01 (github; an enabled source of the repository; tests on; one
// active parent config without the list), a02 (github; a disabled source of the repository spelled in upper case;
// cicd on; a parent config with the list, the integration config with the list, and a child config), a03 (github;
// another repository only), a04 (gitlab; a source of the repository). The other org: a05 (github; the repository).
// S1 WANT exactly a01 and a02 with their values; C1 WANT 3 github sources over 3 integrations.
func TestFormExec14559ConfigsListsTheIntegrationsThatSyncTheRepository(t *testing.T) {
	ctx := context.Background()
	db := startDatabase(t)
	conn := db.conn

	sum := sha256.Sum256([]byte(formExec14559SQL))
	if got := hex.EncodeToString(sum[:]); got != formExec14559SHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (FORM-14559-CONFIGS-1.sql with the org prefix as %s and the repository as %s)",
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
		case "S0", "S1", "C1":
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
			wantRepo = 4
		}
		if got := strings.Count(sqlLines, formExec14559RepoSlot); got != wantRepo {
			t.Errorf("%s: %d repository slot(s), want %d", piece.id, got, wantRepo)
		}
		pieces[index].text = strings.ReplaceAll(strings.ReplaceAll(piece.text, formExec14559Placeholder, prefix), formExec14559RepoSlot, repo)
		if strings.Contains(pieces[index].text, "{{") {
			t.Errorf("%s: a placeholder is left", piece.id)
		}
		upper := strings.ToUpper(formExec14559SQLLines(piece.text))
		for _, banned := range []string{"SET ", "INSERT ", "UPDATE ", "DELETE ", "EXPLAIN", "COPY ", "CREATE ", "DROP ", "ALTER "} {
			if strings.Contains(upper, banned) {
				t.Errorf("%s holds %q", piece.id, banned)
			}
		}
	}
	if got := strings.Join(ids, ","); got != "BEGIN,G0,C0,S0,S1,C1,ROLLBACK" {
		t.Fatalf("pieces = %s", got)
	}

	const org, other = prefix + "-1455-4000-8000-0000000000e1", "e1455aaa-1455-4000-8000-0000000000e2"
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
	withKey := `{"all_repos": true, "ci_artifact_exclude_prefixes": ["ci-images-"]}`
	integration(1, org, "github", `{"fetch_comments": true}`)
	source(1, org, "github", repo, true)
	dataset(1, org, "tests", true)
	dataset(1, org, "cicd", false)
	config(configID(1), 1, org, "github", `{"all_repos": true}`, nil)
	integration(2, org, "github", `{"ci_artifact_exclude_prefixes": ["ci-images-"]}`)
	source(2, org, "github", strings.ToUpper(repo), false)
	dataset(2, org, "cicd", true)
	config(configID(2), 2, org, "github", withKey, nil)
	parent := configID(2)
	config(configID(7), 2, org, "github", `{}`, &parent)
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

	results, err := formExec14559Run(ctx, t, conn, pieces, "", "", "")
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
		"integration_prefix=f1455a01 integration_active=true tests_on=true cicd_on=false repo_sources_enabled=1 parent_configs=1 parent_configs_active=1 parent_config_prefixes=c1455a01 parent_configs_with_key=0 integration_key_set=false",
		"integration_prefix=f1455a02 integration_active=true tests_on=false cicd_on=true repo_sources_enabled=0 parent_configs=1 parent_configs_active=1 parent_config_prefixes=c1455a02 parent_configs_with_key=1 integration_key_set=true",
	}
	if got := formExec14559Rows(results, "S1"); strings.Join(got, "\n") != strings.Join(wantS1, "\n") {
		t.Errorf("S1 =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(wantS1, "\n"))
	}
	if got := formExec14559Rows(results, "C1"); strings.Join(got, ";") != "github_sources=3 github_integrations=3" {
		t.Errorf("C1 = %v, want github_sources=3 github_integrations=3", got)
	}

	// Mutation cells on the same rows: each changes S1 or C1.
	for _, mutant := range []struct {
		name, id, from, to string
	}{
		{"s1-case-sensitive-repo", "S1", "lower(s.external_id) = '" + repo + "' OR lower(s.full_name) = '" + repo + "'", "s.external_id = '" + repo + "'"},
		{"s1-children-counted", "S1", "AND c.planner_managed AND c.parent_id IS NULL) AS parent_configs,", ") AS parent_configs,"},
		{"s1-key-name", "S1", "'ci_artifact_exclude_prefixes') IS NOT NULL) AS parent_configs_with_key", "'ci_artifact_exclude_prefix') IS NOT NULL) AS parent_configs_with_key"},
		{"s1-any-provider", "S1", "AND lower(i.provider) = 'github'", ""},
		{"s1-other-org", "S1", prefix, "e1455aaa"},
		{"s1-dataset-ignores-switch", "S1", "AND d.dataset_key = 'cicd' AND d.is_enabled", "AND d.dataset_key = 'cicd'"},
		{"c1-no-provider", "C1", "lower(s.provider) = 'github'", "lower(s.provider) = 'none'"},
	} {
		mutated, err := formExec14559Run(ctx, t, conn, pieces, mutant.id, mutant.from, mutant.to)
		if err != nil {
			t.Fatalf("MUTANT %s: %v", mutant.name, err)
		}
		before, after := strings.Join(formExec14559Rows(results, mutant.id), "\n"), strings.Join(formExec14559Rows(mutated, mutant.id), "\n")
		t.Logf("MUTANT %s: %s =\n%s", mutant.name, mutant.id, after)
		if after == before {
			t.Errorf("MUTANT %s SURVIVED", mutant.name)
		}
	}

	// The gate fires: each cap literal lowered to 0 in a test copy. WANT the named cast error STOP-G0 (SQLSTATE 22P02),
	// raised by G0 before any later statement runs.
	for _, literal := range []string{"100000::bigint", "268435456::bigint", "150000::bigint", "402653184::bigint", "200000::bigint", "536870912::bigint",
		"2000000::bigint", "1073741824::bigint", "3000000::bigint", "2147483648::bigint"} {
		if strings.Count(pieces[1].text, literal) != 1 {
			t.Fatalf("G0 holds %q %d time(s), want 1", literal, strings.Count(pieces[1].text, literal))
		}
		results, err := formExec14559Run(ctx, t, conn, pieces, "G0", literal, "0::bigint")
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "22P02" || !strings.Contains(pgErr.Message, "STOP-G0") {
			t.Errorf("GATE %s lowered to 0: err = %v, want 22P02 STOP-G0", literal, err)
			continue
		}
		if len(results) != 0 {
			t.Errorf("GATE %s: statements ran before the gate: %v", literal, results)
		}
		t.Logf("GATE %s lowered to 0: fired: %s", literal, pgErr.Message)
	}
}
