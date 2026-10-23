//go:build integration

package authschema

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func m24Apply(t *testing.T, ctx context.Context, violate func(env *migrationOnlyFixture)) (*migrationOnlyFixture, error) {
	t.Helper()
	env := newAuthMigrationOnly(t, ctx)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.Version == 24 {
			if violate != nil {
				violate(env)
			}
			return env, applyOne(ctx, env.conn, env.options, migration)
		}
		if err := applyOne(ctx, env.conn, env.options, migration); err != nil {
			t.Fatalf("apply %04d: %v", migration.Version, err)
		}
	}
	t.Fatal("the lineage has no migration 0024")
	return nil, nil
}

func m24Exec(t *testing.T, ctx context.Context, env *migrationOnlyFixture, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		tx, err := env.conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true)`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatalf("seed %q: %v", statement, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

const (
	m24Org       = "00000000-0000-4000-8000-0000000024a1"
	m24Principal = "00000000-0000-4000-8000-0000000024a2"
	m24Owner     = "00000000-0000-4000-8000-0000000024a3"
	m24Svc       = "00000000-0000-4000-8000-0000000024a4"
	m24Svc2      = "00000000-0000-4000-8000-0000000024a5"
)

// m24Base inserts the principals and organization every violating row needs.
func m24Base(t *testing.T, ctx context.Context, env *migrationOnlyFixture) {
	t.Helper()
	m24Exec(t, ctx, env,
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+m24Principal+`', 'user', 'm24'), ('`+m24Owner+`', 'user', 'm24 owner'), ('`+m24Svc+`', 'service_account', 'm24'), ('`+m24Svc2+`', 'service_account', 'm24')`,
		`INSERT INTO auth.organizations (id, slug, name) VALUES ('`+m24Org+`', 'm24', 'M24')`,
		`INSERT INTO auth.roles (role_key, name, scope) VALUES ('m24.role', 'm24', 'organization')`)
}

func m24Violations(t *testing.T) map[string]func(*testing.T, context.Context, *migrationOnlyFixture) {
	return map[string]func(*testing.T, context.Context, *migrationOnlyFixture){
		"memberships_active_has_role": func(t *testing.T, ctx context.Context, env *migrationOnlyFixture) {
			m24Exec(t, ctx, env,
				`ALTER TABLE auth.memberships DROP CONSTRAINT memberships_active_has_role`,
				`INSERT INTO auth.memberships (organization_id, principal_id, status) VALUES ('`+m24Org+`', '`+m24Principal+`', 'active')`,
				`ALTER TABLE auth.memberships ADD CONSTRAINT memberships_active_has_role CHECK (status <> 'active' OR role IS NOT NULL) NOT VALID`)
		},
		"resource_grants_kind_closed": func(t *testing.T, ctx context.Context, env *migrationOnlyFixture) {
			m24Exec(t, ctx, env,
				`ALTER TABLE auth.resource_grants DROP CONSTRAINT resource_grants_kind_closed`,
				`INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ('`+m24Principal+`', '`+m24Org+`', 'm24.role', 'repo', 'acme/widgets'), ('`+m24Principal+`', '`+m24Org+`', 'm24.role', 'repo', 'acme/gadgets')`,
				`ALTER TABLE auth.resource_grants ADD CONSTRAINT resource_grants_kind_closed CHECK (resource_kind IN ('repository')) NOT VALID`)
		},
		"resource_grants_repository_key_canonical": func(t *testing.T, ctx context.Context, env *migrationOnlyFixture) {
			m24Exec(t, ctx, env,
				`ALTER TABLE auth.resource_grants DROP CONSTRAINT resource_grants_repository_key_canonical`,
				`INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ('`+m24Principal+`', '`+m24Org+`', 'm24.role', 'repository', 'Acme/Widgets')`,
				`ALTER TABLE auth.resource_grants ADD CONSTRAINT resource_grants_repository_key_canonical CHECK (resource_kind <> 'repository' OR resource_id ~ '^([a-z0-9][a-z0-9._-]{0,99}/[a-z0-9][a-z0-9._-]{0,99}|\*)$') NOT VALID`)
		},
		"service_accounts_named_owner": func(t *testing.T, ctx context.Context, env *migrationOnlyFixture) {
			m24Exec(t, ctx, env,
				`ALTER TABLE auth.service_accounts DROP CONSTRAINT service_accounts_named_owner`,
				`INSERT INTO auth.service_accounts (principal_id, organization_id, name, owner_principal_id) VALUES ('`+m24Svc+`', '`+m24Org+`', 'm24 sa', NULL)`,
				`ALTER TABLE auth.service_accounts ADD CONSTRAINT service_accounts_named_owner CHECK (owner_principal_id IS NOT NULL) NOT VALID`)
		},
	}
}

// TestMigration0024RefusesAViolatingRow: a row that breaks one of the four
// constraints makes 0024 refuse with the constraint name and the count, never
// a raw VALIDATE error, and nothing is recorded or validated.
func TestMigration0024RefusesAViolatingRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for name, violate := range m24Violations(t) {
		t.Run(name, func(t *testing.T) {
			env, err := m24Apply(t, ctx, func(env *migrationOnlyFixture) {
				m24Base(t, ctx, env)
				violate(t, ctx, env)
			})
			rows := 1
			if name == "resource_grants_kind_closed" {
				rows = 2
			}
			want := fmt.Sprintf("migration 0024 refused: %s has %d violating row(s)", name, rows)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("0024 over a violating row = %v, want %q", err, want)
			}
			if strings.Contains(err.Error(), "is violated by some row") {
				t.Fatalf("raw VALIDATE error leaked: %v", err)
			}
			var recorded bool
			if err := env.conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM auth.schema_migrations WHERE version = 24)`).Scan(&recorded); err != nil || recorded {
				t.Fatalf("a refused 0024 was recorded: %v, %v", recorded, err)
			}
			if validated := m24Validated(t, ctx, env); validated != 0 {
				t.Fatalf("%d constraint(s) validated after a refused 0024, want 0", validated)
			}
		})
	}
}

// TestMigration0024ValidatesCleanTables: over empty and over valid rows every
// constraint ends validated, and a later bad write is still refused.
func TestMigration0024ValidatesCleanTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env, err := m24Apply(t, ctx, func(env *migrationOnlyFixture) {
		m24Base(t, ctx, env)
		m24Exec(t, ctx, env,
			`INSERT INTO auth.memberships (organization_id, principal_id, status, role) VALUES ('`+m24Org+`', '`+m24Principal+`', 'active', 'member')`,
			`INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ('`+m24Principal+`', '`+m24Org+`', 'm24.role', 'repository', 'acme/widgets')`)
	})
	if err != nil {
		t.Fatalf("0024 over valid rows = %v", err)
	}
	if validated := m24Validated(t, ctx, env); validated != 4 {
		t.Fatalf("validated after 0024 = %d, want 4", validated)
	}
	tx, err := env.conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true)`); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO auth.service_accounts (principal_id, organization_id, name, owner_principal_id) VALUES ('`+m24Svc2+`', '`+m24Org+`', 'm24 sa 2', NULL)`)
	if err == nil || !strings.Contains(err.Error(), "service_accounts_named_owner") {
		t.Fatalf("a service account without an owner after 0024 = %v, want the constraint refusal", err)
	}
}

// m24Validated counts how many of the four constraints are validated.
func m24Validated(t *testing.T, ctx context.Context, env *migrationOnlyFixture) int {
	t.Helper()
	var validated int
	if err := env.conn.QueryRow(ctx, `SELECT count(*) FROM pg_constraint WHERE connamespace = 'auth'::regnamespace AND convalidated
		AND conname IN ('memberships_active_has_role', 'resource_grants_kind_closed', 'resource_grants_repository_key_canonical', 'service_accounts_named_owner')`).Scan(&validated); err != nil {
		t.Fatal(err)
	}
	return validated
}
