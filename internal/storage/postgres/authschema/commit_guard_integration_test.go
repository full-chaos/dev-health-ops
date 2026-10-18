//go:build integration

package authschema

import (
	"context"
	"sort"
	"testing"
	"time"
)

// guardedTables carry the audit_commit_guard trigger (migration 0006): a write
// is refused unless it runs inside an internal/audit.Commit transaction.
var guardedTables = []string{
	"auth_outbox_events", "security_audit_events", "resource_grants", "memberships",
	"platform_role_assignments", "policy_revisions", "entitlement_snapshots",
	"credential_registry", "service_accounts", "workloads", "external_identities",
	"organizations",
}

// unguardedTables are deliberately outside the guard, each with its reason.
var unguardedTables = map[string]string{
	"sessions":            "high-rate session lifecycle rows; a per-write outbox event and audit row would be noise",
	"refresh_credentials": "high-rate rotation lifecycle rows, same reason as sessions",
	"users":               "profile and lifecycle columns written at high rate; identity linking is guarded on external_identities",
	"principals":          "identity root row, created with its user or service account; carries no grant",
	"actions":             "closed vocabulary; the runtime role has no write privilege, so only the migration role writes it",
	"roles":               "closed vocabulary; the runtime role has no write privilege",
	"role_actions":        "closed vocabulary; the runtime role has no write privilege",
	"signing_keys":        "public metadata only; the runtime role has no write privilege",
	versionTable:          "auth-migrate bookkeeping, unreachable by the runtime role",
}

// TestEveryAuthTableIsClassified makes a new table fail CI until someone says
// whether security writes to it must go through audit.Commit.
func TestEveryAuthTableIsClassified(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)

	rows, err := env.admin.Query(ctx,
		`SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'`, env.schema)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	rows.Close()
	if len(tables) == 0 {
		t.Fatal("no tables found in the applied schema; this control proves nothing")
	}

	guarded := make(map[string]bool, len(guardedTables))
	for _, name := range guardedTables {
		guarded[name] = true
	}
	var unclassified, both []string
	for _, name := range tables {
		_, un := unguardedTables[name]
		switch {
		case guarded[name] && un:
			both = append(both, name)
		case !guarded[name] && !un:
			unclassified = append(unclassified, name)
		}
	}
	sort.Strings(unclassified)
	if len(unclassified) > 0 || len(both) > 0 {
		t.Fatalf("tables must be in exactly one list: unclassified=%v in-both=%v", unclassified, both)
	}

	// The lists must match the database: a guarded table has the trigger and
	// an unguarded one does not.
	trig, err := env.admin.Query(ctx, `
		SELECT c.relname FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_proc p ON p.oid = t.tgfoid
		WHERE n.nspname = $1 AND p.proname = 'audit_commit_guard' AND NOT t.tgisinternal`, env.schema)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for trig.Next() {
		var name string
		if err := trig.Scan(&name); err != nil {
			t.Fatal(err)
		}
		have[name] = true
	}
	trig.Close()
	for _, name := range guardedTables {
		if !have[name] {
			t.Errorf("guarded table %s has no audit_commit_guard trigger", name)
		}
	}
	for name := range unguardedTables {
		if have[name] {
			t.Errorf("table %s is listed unguarded but carries the trigger", name)
		}
	}
}

// TestOnlyTheRuntimeAndMigrationRolesCanWriteGuardedTables bounds the guard's
// threat model. The commit guard is a transaction-local setting, so any role
// that can write a guarded table can also set it; the guard is therefore
// exactly as strong as the table privileges. This asserts that no role other
// than the auth runtime role and the migration/owner role (PUBLIC included,
// so an ops application role on the shared instance cannot hold a grant)
// has INSERT, UPDATE or DELETE on any guarded table.
func TestOnlyTheRuntimeAndMigrationRolesCanWriteGuardedTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)

	rows, err := env.admin.Query(ctx, `
		SELECT c.relname, CASE a.grantee WHEN 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END, a.privilege_type
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL aclexplode(c.relacl) a
		WHERE n.nspname = $1 AND c.relname = ANY($2)
		  AND a.privilege_type IN ('INSERT', 'UPDATE', 'DELETE')`, env.schema, guardedTables)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	sawRuntime := false
	for rows.Next() {
		var table, grantee, privilege string
		if err := rows.Scan(&table, &grantee, &privilege); err != nil {
			t.Fatal(err)
		}
		switch grantee {
		case env.runtimeRole:
			sawRuntime = true
		case env.migrationRole:
		default:
			t.Errorf("role %s holds %s on guarded table %s; only %s and %s may write it",
				grantee, privilege, table, env.runtimeRole, env.migrationRole)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !sawRuntime {
		t.Fatal("the runtime role holds no write grant on any guarded table; the control read nothing")
	}
}
