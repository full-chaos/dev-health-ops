//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

const (
	m19Org       = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	m19Principal = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
)

func m19Seed(t *testing.T, ctx context.Context, env *authFixture) {
	t.Helper()
	if _, err := env.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+m19Principal+`', 'user', 'm19')`); err != nil {
		t.Fatal(err)
	}
	if err := guardedExec(ctx, env.admin, `INSERT INTO auth.organizations (id, slug, name) VALUES ('`+m19Org+`', 'm19-org', 'M19')`); err != nil {
		t.Fatal(err)
	}
}

func m19Insert(ctx context.Context, env *authFixture, status, role string) error {
	roleSQL := "NULL"
	if role != "" {
		roleSQL = "'" + role + "'"
	}
	return guardedExec(ctx, env.runtime, `INSERT INTO auth.memberships (organization_id, principal_id, status, role)
		VALUES ('`+m19Org+`', '`+m19Principal+`', '`+status+`', `+roleSQL+`)`)
}

// TestMigration0019MembershipRoleAndStatus: memberships.role is a nullable
// closed organization-role column, status admits removed, an active row needs a
// role, and no organization or principal delete reaches a membership.
func TestMigration0019MembershipRoleAndStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)
	m19Seed(t, ctx, env)

	var dataType, nullable string
	if err := env.admin.QueryRow(ctx, `SELECT data_type, is_nullable FROM information_schema.columns
		WHERE table_schema = 'auth' AND table_name = 'memberships' AND column_name = 'role'`).Scan(&dataType, &nullable); err != nil {
		t.Fatalf("memberships.role: %v", err)
	}
	if dataType != "text" || nullable != "YES" {
		t.Fatalf("memberships.role is %s nullable=%s, want text nullable", dataType, nullable)
	}

	for _, role := range []string{"platform_admin", "superuser", "root", "Owner"} {
		m10Fail(t, m19Insert(ctx, env, "active", role), "an active membership with role "+role)
	}
	m10Fail(t, m19Insert(ctx, env, "active", ""), "an active membership with no role")
	m10Fail(t, m19Insert(ctx, env, "banned", "viewer"), "a membership with status banned")

	if err := m19Insert(ctx, env, "invited", ""); err != nil {
		t.Fatalf("an invited membership with no role: %v", err)
	}
	for _, step := range []string{
		`UPDATE auth.memberships SET status = 'active', role = 'owner'`,
		`UPDATE auth.memberships SET role = 'admin'`,
		`UPDATE auth.memberships SET status = 'suspended'`,
		`UPDATE auth.memberships SET status = 'removed'`,
	} {
		if err := guardedExec(ctx, env.runtime, step); err != nil {
			t.Fatalf("%s: %v", step, err)
		}
	}
	m10Fail(t, guardedExec(ctx, env.runtime, `UPDATE auth.memberships SET status = 'active', role = NULL`), "an active membership losing its role")

	if _, err := env.runtime.Exec(ctx, `DELETE FROM auth.memberships WHERE false`); err == nil {
		t.Error("the runtime role holds DELETE on memberships")
	}

	err := guardedExec(ctx, env.admin, `DELETE FROM auth.organizations WHERE id = '`+m19Org+`'`)
	if err == nil || !strings.Contains(err.Error(), "foreign key") {
		t.Errorf("deleting an organization with a membership: %v, want a foreign key refusal", err)
	}
	err = guardedExec(ctx, env.admin, `DELETE FROM auth.principals WHERE id = '`+m19Principal+`'`)
	if err == nil || !strings.Contains(err.Error(), "foreign key") {
		t.Errorf("deleting a principal with a membership: %v, want a foreign key refusal", err)
	}
}
