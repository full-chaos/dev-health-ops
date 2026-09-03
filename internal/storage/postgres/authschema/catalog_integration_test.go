//go:build integration

package authschema

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestNoPlaintextCredentialColumnInTheAppliedSchema enforces CHAOS-4882's
// "no plaintext credential/secret column" criterion against the CATALOG of the
// database the migrations actually built.
//
// It replaces a regexp over the migration text, which two review rounds walked
// through four times: a quoted table name, a four-space-only indentation rule,
// a one-line `CREATE TABLE t (api_token text NOT NULL);`, and `CREATE UNLOGGED
// TABLE`. Every one of those was a way of writing a real column that the TEXT
// scanner did not see. None of them exists here, because a column in
// information_schema is a column however it was written — the evasion class is
// gone rather than enumerated.
//
// It also retires the old `checked > 0` vacuity proxy. The count is now the
// real column count of the applied schema, so "did this examine anything" is
// answered by the same query that does the examining.
func TestNoPlaintextCredentialColumnInTheAppliedSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := newAuthFixture(t, ctx)

	rows, err := env.admin.Query(ctx, `
		SELECT table_name, column_name
		FROM information_schema.columns
		WHERE table_schema = $1
		ORDER BY table_name, ordinal_position`, env.schema)
	if err != nil {
		t.Fatalf("read the applied schema's columns: %v", err)
	}
	defer rows.Close()

	total, candidates := 0, 0
	var offenders []string
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			t.Fatalf("read a column: %v", err)
		}
		total++
		if !secretRoot.MatchString(column) {
			continue
		}
		candidates++
		if _, approved := approvedSecretColumns[column]; approved {
			continue
		}
		if hasApprovedSuffix(column) {
			continue
		}
		offenders = append(offenders, table+"."+column)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the applied schema's columns: %v", err)
	}

	sort.Strings(offenders)
	t.Logf("examined %d column(s) in the applied schema, %d matched the secret-name rule", total, candidates)
	if total == 0 {
		t.Fatal("the applied schema has no columns; the query is wrong, not the schema clean")
	}
	if len(offenders) > 0 {
		t.Fatalf(
			"column(s) whose name suggests credential material: %s\n"+
				"Store a hash or a reference, or add the column to approvedSecretColumns with the reason it is safe.",
			strings.Join(offenders, ", "),
		)
	}
}

// TestCatalogSecretRuleCatchesWhatTheTextScannerMissed is the control: each
// construction below defeated the retired regexp guard, and each is a REAL
// column once applied, so the catalog check must see all of them.
func TestCatalogSecretRuleCatchesWhatTheTextScannerMissed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := newAuthFixture(t, ctx)

	// Every one of these is a shape the text scanner could not see.
	for _, ddl := range []string{
		`CREATE TABLE auth.inline_secret (api_token text NOT NULL)`,                // one-liner
		`CREATE TABLE auth."QuotedSecret" (client_secret text NOT NULL)`,           // quoted name
		`CREATE UNLOGGED TABLE auth.unlogged_secret (session_token text NOT NULL)`, // UNLOGGED
		"CREATE TABLE auth.tabbed_secret (\n\tprivate_material text NOT NULL\n)",   // tab indent
	} {
		if _, err := env.migration.Exec(ctx, ddl); err != nil {
			t.Fatalf("seed %q: %v", ddl, err)
		}
	}

	rows, err := env.admin.Query(ctx, `
		SELECT table_name, column_name
		FROM information_schema.columns
		WHERE table_schema = $1`, env.schema)
	if err != nil {
		t.Fatalf("read columns: %v", err)
	}
	defer rows.Close()

	found := map[string]bool{}
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if secretRoot.MatchString(column) &&
			!hasApprovedSuffix(column) {
			if _, approved := approvedSecretColumns[column]; !approved {
				found[column] = true
			}
		}
	}
	for _, column := range []string{"api_token", "client_secret", "session_token", "private_material"} {
		if !found[column] {
			t.Errorf("the catalog secret rule did not flag %q, which the text scanner also missed", column)
		}
	}
	t.Logf("catalog rule flagged all four shapes the retired text scanner accepted")
}
