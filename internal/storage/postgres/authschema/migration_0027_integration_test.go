//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

const (
	workloadsMigration = 27
	wlPrincipalA       = "00000000-0000-4000-8000-00000010980a"
	wlPrincipalB       = "00000000-0000-4000-8000-00000010980b"
)

func applyThroughWorkloads(t *testing.T, ctx context.Context, seed func(*migrationOnlyFixture)) (*migrationOnlyFixture, error) {
	t.Helper()
	env := newAuthMigrationOnly(t, ctx)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.Version == workloadsMigration {
			if seed != nil {
				seed(env)
			}
			return env, applyOne(ctx, env.conn, env.options, m)
		}
		if err := applyOne(ctx, env.conn, env.options, m); err != nil {
			t.Fatalf("apply %04d: %v", m.Version, err)
		}
	}
	t.Fatal("the lineage lacks migration 0027")
	return nil, nil
}

func TestMigration0027RefusesAWorkloadsTableThatHoldsARow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env, err := applyThroughWorkloads(t, ctx, func(env *migrationOnlyFixture) {
		if err := domainOwnedExec(t, ctx, env,
			`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+wlPrincipalA+`', 'workload', 'old')`,
			`INSERT INTO auth.workloads (principal_id, name, trust_domain, issuer, namespace, service_account_name, audience)
			 VALUES ('`+wlPrincipalA+`', 'old', 'cluster.local', 'https://i', 'ns', 'sa', 'auth-service')`); err != nil {
			t.Fatalf("seed: %v", err)
		}
	})
	if err == nil || !strings.Contains(err.Error(), "workloads holds 1 row(s)") {
		t.Fatalf("migration over a row = %v, want the named count", err)
	}
	var recorded bool
	if err := env.conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM auth.schema_migrations WHERE version = $1)`, workloadsMigration).Scan(&recorded); err != nil || recorded {
		t.Fatalf("a refused migration was recorded: %v, %v", recorded, err)
	}
}

func TestMigration0027ShapeAndGuards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env, err := applyThroughWorkloads(t, ctx, nil)
	if err != nil {
		t.Fatalf("migration over an empty table: %v", err)
	}
	if err := domainOwnedExec(t, ctx, env,
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+wlPrincipalA+`', 'workload', 'a')`,
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+wlPrincipalB+`', 'workload', 'b')`,
		`INSERT INTO auth.workloads (principal_id, name, audiences) VALUES ('`+wlPrincipalA+`', 'a', ARRAY['auth-service'])`,
		`INSERT INTO auth.workloads (principal_id, name) VALUES ('`+wlPrincipalB+`', 'b')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for _, col := range []string{"audience", "trust_domain", "issuer", "namespace", "service_account_name"} {
		var n int
		if err := env.conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema = 'auth' AND table_name = 'workloads' AND column_name = $1`, col).Scan(&n); err != nil || n != 0 {
			t.Errorf("workloads.%s still exists (%d, %v)", col, n, err)
		}
	}
	binding := func(workload, ns, extra string) string {
		return `INSERT INTO auth.workload_bindings (id, workload_principal_id, environment, trust_domain, issuer, namespace, service_account_name` + extra + `)
			VALUES (gen_random_uuid(), '` + workload + `', 'prod', 'cluster.local', 'https://i', '` + ns + `', 'sa'` + map[bool]string{true: ", '6f1c3c2e-0b8e-4d2a-9d7e-2f4a5b6c7d8e'", false: ""}[extra != ""] + `)`
	}
	if err := domainOwnedExec(t, ctx, env, binding(wlPrincipalA, "ns1", "")); err != nil {
		t.Fatalf("first binding: %v", err)
	}
	if err := domainOwnedExec(t, ctx, env, binding(wlPrincipalB, "ns1", "")); err == nil || !strings.Contains(err.Error(), "workload_bindings_federation_key") {
		t.Errorf("a second workload bound the same service account tuple: %v", err)
	}
	if err := domainOwnedExec(t, ctx, env, binding(wlPrincipalB, "ns2", ", service_account_uid")); err != nil {
		t.Errorf("a binding with a uid was refused: %v", err)
	}
	refused := map[string]string{
		"empty namespace":     `UPDATE auth.workload_bindings SET namespace = '' WHERE namespace = 'ns2'`,
		"bad uid":             `UPDATE auth.workload_bindings SET service_account_uid = 'not-a-uid' WHERE namespace = 'ns2'`,
		"bad environment":     `UPDATE auth.workload_bindings SET environment = 'Prod' WHERE namespace = 'ns2'`,
		"duplicate audiences": `UPDATE auth.workloads SET audiences = ARRAY['x1','x1'] WHERE name = 'b'`,
		"bad audience":        `UPDATE auth.workloads SET audiences = ARRAY['Bad Audience'] WHERE name = 'b'`,
		"17 audiences":        `UPDATE auth.workloads SET audiences = ARRAY(SELECT 'a' || g FROM generate_series(1, 17) g) WHERE name = 'b'`,
	}
	for name, stmt := range refused {
		if err := domainOwnedExec(t, ctx, env, stmt); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if _, err := env.conn.Exec(ctx, `UPDATE auth.workload_bindings SET disabled_at = now()`); err == nil || !strings.Contains(err.Error(), "audit.Commit") {
		t.Errorf("a binding write outside audit.Commit = %v", err)
	}
	if _, err := env.conn.Exec(ctx, `TRUNCATE auth.workload_bindings`); err == nil || !strings.Contains(err.Error(), "audit.Commit") {
		t.Errorf("TRUNCATE outside audit.Commit = %v", err)
	}
	if err := domainOwnedExec(t, ctx, env, `DELETE FROM auth.workloads WHERE name = 'a'`); err == nil {
		t.Error("a workload that still has a binding was deleted")
	}
}
