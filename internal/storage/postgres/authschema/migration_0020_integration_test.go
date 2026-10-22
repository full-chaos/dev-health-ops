//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

const (
	m20Org       = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	m20Grantee   = "11111111-1111-4111-8111-111111111111"
	m20Owner     = "22222222-2222-4222-8222-222222222222"
	m20SvcPrin   = "33333333-3333-4333-8333-333333333333"
	m20WlPrin    = "44444444-4444-4444-8444-444444444444"
	m20WlPrin2   = "55555555-5555-4555-8555-555555555555"
	m20WlPrin3   = "66666666-6666-4666-8666-666666666666"
	m20WlPrin4   = "77777777-7777-4777-8777-777777777777"
	m20WlPrin5   = "88888888-8888-4888-8888-888888888888"
	m20RoleKey   = "m20.role"
	m20GrantHead = `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ('` + m20Grantee + `', '` + m20Org + `', '` + m20RoleKey + `', `
)

func m20Seed(t *testing.T, ctx context.Context, env *authFixture) {
	t.Helper()
	for id, kind := range map[string]string{m20Grantee: "user", m20Owner: "user", m20SvcPrin: "service_account", m20WlPrin: "workload", m20WlPrin2: "workload", m20WlPrin3: "workload", m20WlPrin4: "workload", m20WlPrin5: "workload"} {
		if _, err := env.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+id+`', '`+kind+`', 'm20')`); err != nil {
			t.Fatal(err)
		}
	}
	if err := guardedExec(ctx, env.admin, `INSERT INTO auth.organizations (id, slug, name) VALUES ('`+m20Org+`', 'm20-org', 'M20')`); err != nil {
		t.Fatal(err)
	}
	if _, err := env.admin.Exec(ctx, `INSERT INTO auth.roles (role_key, name, scope) VALUES ('`+m20RoleKey+`', 'm20', 'organization')`); err != nil {
		t.Fatal(err)
	}
}

func m20Workload(principal, org, name, namespace, actions string) string {
	return `INSERT INTO auth.workloads (principal_id, organization_id, name, trust_domain, issuer, namespace, service_account_name, audience, allowed_actions)
		VALUES ('` + principal + `', ` + org + `, '` + name + `', 'td', 'iss', '` + namespace + `', 'sa', 'aud', ` + actions + `)`
}

// TestMigration0020GrantServiceAccountWorkloadShape: repository grants carry a
// canonical key or the explicit wildcard, a revoked grant stays as history and
// frees its tuple, a service account has a named owner and a closed status, a
// workload lists only well-formed actions and a unique name, and deleting an
// organization that owns any of them is refused.
func TestMigration0020GrantServiceAccountWorkloadShape(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)
	m20Seed(t, ctx, env)
	exec := func(sql string) error { return guardedExec(ctx, env.runtime, sql) }

	for _, key := range []string{"acme/widgets", "*"} {
		if err := exec(m20GrantHead + `'repository', '` + key + `')`); err != nil {
			t.Fatalf("repository grant %s: %v", key, err)
		}
	}
	for _, key := range []string{"Acme/Widgets", "acme", "acme/a/b", "/widgets", "acme/", "acme/wid gets", "**", "acme/widgets*"} {
		m10Fail(t, exec(m20GrantHead+`'repository', '`+key+`')`), "repository grant key "+key)
	}
	m10Fail(t, exec(m20GrantHead+`'repo', 'acme/other')`), "a grant of kind repo")
	m10Fail(t, exec(m20GrantHead+`'repository', 'acme/widgets')`), "a second active grant of the same tuple")

	if err := exec(`UPDATE auth.resource_grants SET revoked_at = now(), revoked_by = '` + m20Owner + `' WHERE resource_id = 'acme/widgets'`); err != nil {
		t.Fatalf("revoking a grant: %v", err)
	}
	if err := exec(m20GrantHead + `'repository', 'acme/widgets')`); err != nil {
		t.Fatalf("re-granting after a revoke: %v", err)
	}
	var rows int
	if err := env.admin.QueryRow(ctx, `SELECT count(*) FROM auth.resource_grants WHERE resource_id = 'acme/widgets'`).Scan(&rows); err != nil || rows != 2 {
		t.Fatalf("grant rows for acme/widgets = %d (%v), want 2 (revoked row kept)", rows, err)
	}
	m10Fail(t, exec(`UPDATE auth.resource_grants SET revoked_by = '`+m20Owner+`' WHERE resource_id = '*'`), "revoked_by without revoked_at")
	revoked := `WHERE resource_id = 'acme/widgets' AND revoked_at IS NOT NULL`
	for _, change := range []string{
		`SET revoked_at = NULL, revoked_by = NULL`,
		`SET revoked_at = now() + interval '1 day'`,
		`SET revoked_by = '` + m20Grantee + `'`,
		`SET granted_by = '` + m20Owner + `'`,
		`SET resource_id = 'acme/other'`,
		`SET created_at = created_at - interval '1 day'`,
		`SET role_key = '` + m20RoleKey + `', expires_at = now() + interval '1 day'`,
	} {
		m10Fail(t, exec(`UPDATE auth.resource_grants `+change+` `+revoked), "changing a revoked grant: "+change)
	}
	if err := exec(`UPDATE auth.resource_grants SET updated_at = now() ` + revoked); err != nil {
		t.Fatalf("touching updated_at of a revoked grant: %v", err)
	}

	saHead := `INSERT INTO auth.service_accounts (principal_id, organization_id, name, owner_principal_id, status, disabled_at) VALUES ('` + m20SvcPrin + `', '` + m20Org + `', 'svc', `
	m10Fail(t, exec(saHead+`NULL, 'active', NULL)`), "a service account with no named owner")
	m10Fail(t, exec(saHead+`'`+m20Owner+`', 'gone', NULL)`), "a service account with status gone")
	m10Fail(t, exec(saHead+`'`+m20Owner+`', 'disabled', NULL)`), "a disabled service account with no disabled_at")
	m10Fail(t, exec(saHead+`'`+m20Owner+`', 'active', now())`), "an active service account with disabled_at")
	if err := exec(saHead + `'` + m20Owner + `', 'active', NULL)`); err != nil {
		t.Fatalf("a service account with a named owner: %v", err)
	}

	org := `'` + m20Org + `'`
	for _, bad := range []string{`'{*}'`, `'{Acr.Read}'`, `'{acr}'`, `'{acr.read,acr.read}'`, `'{"acr.read","acr.write "}'`} {
		m10Fail(t, exec(m20Workload(m20WlPrin, org, "wl", "ns-wl", bad)), "workload allowed_actions "+bad)
	}
	if err := exec(m20Workload(m20WlPrin, org, "wl", "ns-wl", `'{acr.context.read,acr.context.write}'`)); err != nil {
		t.Fatalf("workload with well-formed actions: %v", err)
	}
	var actions int
	if err := exec(m20Workload(m20WlPrin2, "NULL", "platform-wl", "ns-platform-1", `DEFAULT`)); err != nil {
		t.Fatalf("workload with the default (empty) action list: %v", err)
	}
	if err := env.admin.QueryRow(ctx, `SELECT cardinality(allowed_actions) FROM auth.workloads WHERE name = 'platform-wl'`).Scan(&actions); err != nil || actions != 0 {
		t.Fatalf("default allowed_actions has %d entries (%v), want 0", actions, err)
	}
	m10Fail(t, exec(m20Workload(m20WlPrin3, org, "wl", "ns-other", `'{acr.read}'`)), "a second workload named wl in the same organization (distinct federation tuple)")
	m10Fail(t, exec(m20Workload(m20WlPrin4, "NULL", "platform-wl", "ns-platform-2", `DEFAULT`)), "a second platform workload with the same name (distinct federation tuple)")
	if err := exec(m20Workload(m20WlPrin5, "NULL", "platform-wl-b", "ns-platform-2", `DEFAULT`)); err != nil {
		t.Fatalf("a platform workload with another name: %v", err)
	}

	err := guardedExec(ctx, env.admin, `DELETE FROM auth.organizations WHERE id = '`+m20Org+`'`)
	if err == nil || !strings.Contains(err.Error(), "foreign key") {
		t.Errorf("deleting an organization that owns grants, a service account and a workload: %v, want a foreign key refusal", err)
	}
	for _, fk := range []string{"resource_grants_principal_id_fkey", "resource_grants_organization_id_fkey", "service_accounts_organization_id_fkey", "workloads_organization_id_fkey"} {
		var def string
		if err := env.admin.QueryRow(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = $1 AND connamespace = 'auth'::regnamespace`, fk).Scan(&def); err != nil || !strings.Contains(def, "ON DELETE RESTRICT") {
			t.Errorf("%s = %q (%v), want ON DELETE RESTRICT", fk, def, err)
		}
	}
}
