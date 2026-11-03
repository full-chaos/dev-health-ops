//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"
)

const (
	ownerMigrationName = "platform_owner_role"
	mOwnerHolder       = "00000000-0000-4000-8000-000000122941"
)

// TestMigrationPlatformOwnerRoleWidensTheRoleCheck (CHAOS-12294): before the
// migration a platform_owner row is refused; after it the row inserts, the
// CHECK is validated, every other value (organization roles, superuser, a
// wrong-case platform role) is still refused, and a stored platform_admin row
// survives. The migration is found by name, so a renumber needs no edit here.
func TestMigrationPlatformOwnerRoleWidensTheRoleCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newAuthMigrationOnly(t, ctx)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	insert := func(role string) string {
		return `INSERT INTO auth.platform_role_assignments (principal_id, role_key) VALUES ('` + mOwnerHolder + `', '` + role + `')`
	}
	applied := false
	for _, m := range migrations {
		if m.Name == ownerMigrationName {
			if err := domainOwnedExec(t, ctx, env,
				`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+mOwnerHolder+`', 'user', 'owner holder')`,
				insert("platform_admin")); err != nil {
				t.Fatalf("seed: %v", err)
			}
			m10Fail(t, domainOwnedExec(t, ctx, env, insert("platform_owner")), "a platform_owner row before the migration")
		}
		if err := applyOne(ctx, env.conn, env.options, m); err != nil {
			t.Fatalf("apply %04d_%s: %v", m.Version, m.Name, err)
		}
		if m.Name == ownerMigrationName {
			applied = true
			break
		}
	}
	if !applied {
		t.Fatalf("the lineage lacks the migration named %s", ownerMigrationName)
	}
	if err := domainOwnedExec(t, ctx, env, insert("platform_owner")); err != nil {
		t.Fatalf("a platform_owner row after the migration: %v", err)
	}
	var validated bool
	if err := env.conn.QueryRow(ctx, `SELECT convalidated FROM pg_constraint
		WHERE conname = 'platform_role_assignments_role_closed' AND conrelid = 'auth.platform_role_assignments'::regclass`).Scan(&validated); err != nil || !validated {
		t.Fatalf("role CHECK validated = %v (%v), want true", validated, err)
	}
	for _, role := range []string{"owner", "admin", "member", "viewer", "superuser", "Platform_Owner", "platform_owner "} {
		m10Fail(t, domainOwnedExec(t, ctx, env, insert(role)), "a platform role assignment with role "+role)
	}
	var held int
	if err := env.conn.QueryRow(ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1`, mOwnerHolder).Scan(&held); err != nil || held != 2 {
		t.Fatalf("rows held after the migration = %d (%v), want 2 (platform_admin kept, platform_owner added)", held, err)
	}
}
