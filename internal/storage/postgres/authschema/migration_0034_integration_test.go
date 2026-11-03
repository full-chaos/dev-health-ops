//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

const (
	platformRoleMigration = 34
	m34Holder             = "00000000-0000-4000-8000-000000109791"
	m34Granter            = "00000000-0000-4000-8000-000000109792"
	m34Refused            = "00000000-0000-4000-8000-0000001097a1"
)

func applyThroughPlatformRoles(t *testing.T, ctx context.Context, seed func(*migrationOnlyFixture)) (*migrationOnlyFixture, error) {
	t.Helper()
	env := newAuthMigrationOnly(t, ctx)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.Version == platformRoleMigration {
			if seed != nil {
				seed(env)
			}
			return env, applyOne(ctx, env.conn, env.options, m)
		}
		if err := applyOne(ctx, env.conn, env.options, m); err != nil {
			t.Fatalf("apply %04d: %v", m.Version, err)
		}
	}
	t.Fatalf("the lineage lacks migration %04d", platformRoleMigration)
	return nil, nil
}

func m34Principals(t *testing.T, ctx context.Context, env *migrationOnlyFixture) {
	t.Helper()
	if err := domainOwnedExec(t, ctx, env,
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+m34Holder+`', 'user', 'holder')`,
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+m34Granter+`', 'user', 'granter')`); err != nil {
		t.Fatalf("seed principals: %v", err)
	}
}

// The pre-flight names, by id only, an assignment whose role is not a platform
// role, and nothing is applied or recorded.
func TestMigration0034RefusesANonPlatformRoleRowByID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env, err := applyThroughPlatformRoles(t, ctx, func(env *migrationOnlyFixture) {
		m34Principals(t, ctx, env)
		if err := domainOwnedExec(t, ctx, env,
			`INSERT INTO auth.platform_role_assignments (id, principal_id, role_key) VALUES ('`+m34Refused+`', '`+m34Holder+`', 'owner')`); err != nil {
			t.Fatalf("seed: %v", err)
		}
	})
	if err == nil || !strings.Contains(err.Error(), m34Refused) || strings.Contains(err.Error(), "'owner'") {
		t.Fatalf("migration over an organization role row = %v, want the row id named and no role value", err)
	}
	var recorded bool
	if err := env.conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM auth.schema_migrations WHERE version = $1)`, platformRoleMigration).Scan(&recorded); err != nil || recorded {
		t.Fatalf("a refused migration was recorded: %v, %v", recorded, err)
	}
}

// After the migration: only catalog platform roles are values, the CHECK is
// validated, and neither the holder nor the granter of an assignment can be
// deleted.
func TestMigration0034ClosesTheRoleAndRestrictsDeletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env, err := applyThroughPlatformRoles(t, ctx, func(env *migrationOnlyFixture) {
		m34Principals(t, ctx, env)
		if err := domainOwnedExec(t, ctx, env,
			`INSERT INTO auth.platform_role_assignments (principal_id, role_key, granted_by) VALUES ('`+m34Holder+`', 'platform_admin', '`+m34Granter+`')`); err != nil {
			t.Fatalf("seed: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("migration over a platform_admin row: %v", err)
	}
	var validated bool
	if err := env.conn.QueryRow(ctx, `SELECT convalidated FROM pg_constraint
		WHERE conname = 'platform_role_assignments_role_closed' AND conrelid = 'auth.platform_role_assignments'::regclass`).Scan(&validated); err != nil || !validated {
		t.Fatalf("role CHECK validated = %v (%v), want true", validated, err)
	}
	for _, fk := range []string{"platform_role_assignments_principal_id_fkey", "platform_role_assignments_granted_by_fkey"} {
		var action string
		if err := env.conn.QueryRow(ctx, `SELECT confdeltype::text FROM pg_constraint
			WHERE conname = $1 AND conrelid = 'auth.platform_role_assignments'::regclass`, fk).Scan(&action); err != nil || action != "r" {
			t.Errorf("%s ON DELETE = %q (%v), want r (RESTRICT)", fk, action, err)
		}
	}
	for _, role := range []string{"owner", "admin", "member", "viewer", "superuser", "Platform_Admin"} {
		m10Fail(t, domainOwnedExec(t, ctx, env,
			`INSERT INTO auth.platform_role_assignments (principal_id, role_key) VALUES ('`+m34Granter+`', '`+role+`')`),
			"a platform role assignment with role "+role)
	}
	for _, who := range []string{m34Holder, m34Granter} {
		err := domainOwnedExec(t, ctx, env, `DELETE FROM auth.principals WHERE id = '`+who+`'`)
		if err == nil || !strings.Contains(err.Error(), "foreign key") {
			t.Errorf("deleting principal %s of an assignment: %v, want a foreign key refusal", who, err)
		}
	}
}
