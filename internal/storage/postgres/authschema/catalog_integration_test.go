//go:build integration

package authschema

import (
	"context"
	"errors"
	"fmt"
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

// TestDefaultPrivilegesAreRefused covers the one check in posture.go that had
// no test and, predictably, shipped a bug in its first draft (an unreferenced
// query parameter, caught only because the whole suite went red).
//
// It is also the check that answers the tense problem: ALTER DEFAULT
// PRIVILEGES grants on objects created LATER, so a rule can sit in the catalog
// while every current-grant check passes, and the escalation appears on the
// next migration. The invariant is EMPTY for the auth schema rather than
// "contains only the expected entries", because a manifest of expected ACLs
// would be another hand-written list beside a source of truth.
func TestDefaultPrivilegesAreRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := newAuthFixture(t, ctx)

	// Clean first: the fixture just applied, so the posture must be clean, or
	// the assertion below could pass for an unrelated reason.
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	options := Options{Schema: env.schema, RuntimeRole: env.runtimeRole}

	before, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
	if err != nil {
		t.Fatalf("VerifyRuntimePosture: %v", err)
	}
	if len(before) != 0 {
		t.Fatalf("posture is not clean before the mutation: %v", before)
	}

	// A rule that grants NOTHING today: the auth schema already has all its
	// tables, so this changes only what a FUTURE table would carry.
	if _, err := env.migration.Exec(ctx, fmt.Sprintf(
		`ALTER DEFAULT PRIVILEGES FOR ROLE %q IN SCHEMA %s GRANT SELECT ON TABLES TO %q`,
		env.migrationRole, env.schema, env.runtimeRole,
	)); err != nil {
		t.Fatalf("set default privileges: %v", err)
	}

	after, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
	if err != nil {
		t.Fatalf("VerifyRuntimePosture after: %v", err)
	}
	var sawDefaultACL bool
	for _, violation := range after {
		if violation.Kind == "default_acl" {
			sawDefaultACL = true
			t.Logf("detected: %s", violation)
		}
	}
	if !sawDefaultACL {
		t.Fatal(
			"a default-privileges rule for the auth schema was not reported. " +
				"It grants nothing today, which is exactly why a current-grant check misses it.",
		)
	}

	// And Apply must refuse, not merely report.
	if _, err := Apply(ctx, env.migration, options); !errors.Is(err, ErrRuntimeRoleCanEscalate) {
		t.Fatalf("Apply = %v, want ErrRuntimeRoleCanEscalate", err)
	}
}
