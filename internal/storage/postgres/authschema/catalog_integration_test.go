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

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
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

// TestSecurityDefinerFunctionOutsideTheAuthSchemaIsReported reproduces
// lane-auth-contracts' executed P1 and proves the fix.
//
// A SECURITY DEFINER function owned by the MIGRATION role, living in another
// schema, is reachable by the runtime role without any grant (PostgreSQL gives
// EXECUTE to PUBLIC by default) and runs as its owner — so calling it executes
// DDL on the auth schema that every direct attempt correctly refuses. The
// first version of functionViolations scoped to the auth schema by LOCATION
// and reported nothing.
//
// The sharpest part, and the reason ownership rather than location is the
// right axis: the route SURVIVES revoking the grant that created the function.
// Once it exists, tidying up the migration role's CREATE on that schema
// changes nothing — the same temporal shape as a default ACL.
func TestSecurityDefinerFunctionOutsideTheAuthSchemaIsReported(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := newAuthFixture(t, ctx)

	// PostgreSQL 15+ revoked CREATE on `public` from PUBLIC, so this route
	// needs an operator to have granted it at some point. Stated because it
	// bounds how alarming the finding is — and because assuming otherwise is
	// what made the reviewer's first attempt fail.
	if _, err := env.admin.Exec(ctx, fmt.Sprintf(
		`GRANT CREATE ON SCHEMA public TO %q`, env.migrationRole)); err != nil {
		t.Fatalf("grant CREATE on public: %v", err)
	}
	if _, err := env.migration.Exec(ctx,
		`CREATE FUNCTION public.auth_maintenance(stmt text) RETURNS void
		 LANGUAGE plpgsql SECURITY DEFINER AS 'BEGIN EXECUTE stmt; END'`); err != nil {
		t.Fatalf("create the SECURITY DEFINER function: %v", err)
	}

	// The escalation is REAL: prove the runtime can reach it before claiming
	// the detection matters. A direct CREATE must still be refused.
	if _, err := env.runtime.Exec(ctx, `CREATE TABLE auth.attacker_direct (id int)`); err == nil {
		t.Fatal("the runtime could create a table directly; the fixture is not the posture under test")
	}
	if _, err := env.runtime.Exec(ctx,
		`SELECT public.auth_maintenance('CREATE TABLE auth.attacker_via_function (id int)')`); err != nil {
		t.Fatalf("the runtime could not reach the function, so this proves nothing: %v", err)
	}
	var owner string
	if err := env.admin.QueryRow(ctx, `
		SELECT pg_get_userbyid(relowner) FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'auth' AND c.relname = 'attacker_via_function'`).Scan(&owner); err != nil {
		t.Fatalf("the function did not create the table: %v", err)
	}
	t.Logf("escalation confirmed: runtime created auth.attacker_via_function, owned by %s", owner)

	// The route outlives the grant that enabled it.
	if _, err := env.admin.Exec(ctx, fmt.Sprintf(
		`REVOKE CREATE ON SCHEMA public FROM %q`, env.migrationRole)); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()

	violations, err := VerifyRuntimePosture(ctx, conn.Conn(), Options{
		Schema: env.schema, RuntimeRole: env.runtimeRole,
	})
	if err != nil {
		t.Fatalf("VerifyRuntimePosture: %v", err)
	}
	var sawFunction bool
	for _, violation := range violations {
		if violation.Kind == "function" && violation.Schema == "public" {
			sawFunction = true
			t.Logf("detected after the enabling grant was revoked: %s", violation)
		}
	}
	if !sawFunction {
		t.Fatalf(
			"the cross-schema SECURITY DEFINER function was not reported; violations were %v",
			violations,
		)
	}
}

// TestPrivilegedPredefinedRoleMembershipIsReported covers codex round 3's P1,
// which was a direct consequence of a design decision made one round earlier.
//
// Membership had been demoted to context-only, on the reasoning that inherited
// OBJECT privileges are already caught by the effective-privilege checks. That
// holds for ordinary roles and fails for PostgreSQL's predefined roles, which
// confer capabilities OUTSIDE the object-privilege model — `has_*_privilege`
// sees nothing, so the posture reported `extra_privileges: 0` while the
// runtime could run `COPY (...) TO PROGRAM`.
func TestPrivilegedPredefinedRoleMembershipIsReported(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := newAuthFixture(t, ctx)
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
		t.Fatalf("posture not clean before the mutation: %v", before)
	}

	if _, err := env.admin.Exec(ctx, fmt.Sprintf(
		`GRANT pg_execute_server_program TO %q`, env.runtimeRole)); err != nil {
		t.Fatalf("grant predefined role: %v", err)
	}

	// The capability is REAL and confers NO surveyed object privilege — that
	// combination is the entire finding, so both halves are asserted.
	after, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
	if err != nil {
		t.Fatalf("VerifyRuntimePosture after: %v", err)
	}
	var sawMembership bool
	var otherKinds int
	for _, violation := range after {
		if violation.Kind == "role_membership" && violation.Object == "pg_execute_server_program" {
			sawMembership = true
			t.Logf("detected: %s", violation)
			continue
		}
		otherKinds++
	}
	if otherKinds != 0 {
		t.Fatalf(
			"the membership brought %d surveyed object privilege(s) with it, so this test would "+
				"pass even with the membership detector removed: %v", otherKinds, after)
	}
	if !sawMembership {
		t.Fatal(
			"membership in pg_execute_server_program was not reported, and it grants no object " +
				"privilege for the effective-privilege checks to catch")
	}
	if _, err := Apply(ctx, env.migration, options); !errors.Is(err, ErrRuntimeRoleCanEscalate) {
		t.Fatalf("Apply = %v, want ErrRuntimeRoleCanEscalate", err)
	}
}

// TestRuntimeOwnedDomainIsReported covers codex round 3's P2: ownership read
// pg_class and schema ownership but not pg_type, so a runtime-owned DOMAIN was
// accepted while its owner could still ALTER DOMAIN ... ADD CONSTRAINT.
func TestRuntimeOwnedDomainIsReported(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := newAuthFixture(t, ctx)
	if _, err := env.admin.Exec(ctx, fmt.Sprintf(
		`CREATE DOMAIN auth.runtime_owned AS text; ALTER DOMAIN auth.runtime_owned OWNER TO %q`,
		env.runtimeRole)); err != nil {
		t.Fatalf("create the runtime-owned domain: %v", err)
	}

	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()

	violations, err := VerifyRuntimePosture(ctx, conn.Conn(), Options{
		Schema: env.schema, RuntimeRole: env.runtimeRole,
	})
	if err != nil {
		t.Fatalf("VerifyRuntimePosture: %v", err)
	}
	var sawDomain bool
	for _, violation := range violations {
		if violation.Kind == "ownership" && violation.Object == "runtime_owned" {
			sawDomain = true
			t.Logf("detected: %s", violation)
		}
	}
	if !sawDomain {
		t.Fatalf("a runtime-owned domain was not reported; violations were %v", violations)
	}
}

// TestUnrelatedGlobalDefaultACLDoesNotBlockTheMigration covers codex round 3's
// other P2, which is an OVER-correction rather than a miss — the third on this
// file, after TEMPORARY.
//
// A database-wide default ACL belonging to a role that cannot create anything
// in the auth schema can never affect this lineage, and rejecting the
// deployment over it teaches an operator to ignore the check.
func TestUnrelatedGlobalDefaultACLDoesNotBlockTheMigration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := newAuthFixture(t, ctx)

	bystander, err := containers.RoleName("auth_bystander", env.instance)
	if err != nil {
		t.Fatalf("derive role: %v", err)
	}
	if _, err := env.admin.Exec(ctx, fmt.Sprintf("CREATE ROLE %q", bystander)); err != nil {
		t.Fatalf("create bystander: %v", err)
	}
	t.Cleanup(func() { containers.DropRole(env.admin, bystander, t.Logf) })

	// A database-wide rule owned by a role with no CREATE on auth.
	if _, err := env.admin.Exec(ctx, fmt.Sprintf(
		`ALTER DEFAULT PRIVILEGES FOR ROLE %q GRANT SELECT ON TABLES TO %q`,
		bystander, env.runtimeRole)); err != nil {
		t.Fatalf("set the unrelated default privileges: %v", err)
	}

	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	options := Options{Schema: env.schema, RuntimeRole: env.runtimeRole}

	violations, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
	if err != nil {
		t.Fatalf("VerifyRuntimePosture: %v", err)
	}
	for _, violation := range violations {
		if violation.Kind == "default_acl" {
			t.Fatalf(
				"an unrelated global default ACL was reported: %s\n"+
					"Its grantor cannot create in the auth schema, so the rule can never fire here.",
				violation)
		}
	}
	if _, err := Apply(ctx, env.migration, options); err != nil {
		t.Fatalf("Apply refused a deployment carrying an unrelated global default ACL: %v", err)
	}

	// The accepting row must not be achieved by making the check blind: a
	// RELEVANT rule (grantor = the migration role, which can create here) must
	// still be reported.
	if _, err := env.migration.Exec(ctx, fmt.Sprintf(
		`ALTER DEFAULT PRIVILEGES FOR ROLE %q IN SCHEMA %s GRANT SELECT ON TABLES TO %q`,
		env.migrationRole, env.schema, env.runtimeRole)); err != nil {
		t.Fatalf("set the relevant default privileges: %v", err)
	}
	relevant, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
	if err != nil {
		t.Fatalf("VerifyRuntimePosture: %v", err)
	}
	var sawRelevant bool
	for _, violation := range relevant {
		if violation.Kind == "default_acl" {
			sawRelevant = true
			t.Logf("relevant rule still detected: %s", violation)
		}
	}
	if !sawRelevant {
		t.Fatal("the relevant default ACL was not reported; the fix blinded the check instead of scoping it")
	}
}
