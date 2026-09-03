//go:build integration

package authschema

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// fixturePassword is a throwaway used only inside a per-test container. It is
// not a credential: the container is created and destroyed by the test, is
// reachable only on a loopback ephemeral port, and never outlives the run.
const fixturePassword = "posture-test-not-a-real-credential"

// authFixture is a migrated auth schema with an admin, a migration and a
// runtime connection, in a container of its own.
//
// It exists so the capability suite and the posture suite build the same
// world. Two hand-built setups would be two chances to differ, and a
// difference between them would be invisible until one suite proved something
// the other could not.
type authFixture struct {
	instance      *containers.Instance
	admin         *pgxpool.Pool
	migration     *pgxpool.Pool
	runtime       *pgxpool.Pool
	migrationRole string
	runtimeRole   string
	database      string
	schema        string
}

func newAuthFixture(t *testing.T, ctx context.Context) *authFixture {
	t.Helper()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})

	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(admin.Close)

	migrationRole, err := containers.RoleName("auth_cap_migrator", instance)
	if err != nil {
		t.Fatalf("derive migration role: %v", err)
	}
	runtimeRole, err := containers.RoleName("auth_cap_runtime", instance)
	if err != nil {
		t.Fatalf("derive runtime role: %v", err)
	}
	database, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatalf("read database name: %v", err)
	}

	for _, statement := range []string{
		fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", migrationRole, fixturePassword),
		fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", runtimeRole, fixturePassword),
		fmt.Sprintf("GRANT CREATE, CONNECT ON DATABASE %q TO %q", database, migrationRole),
		// The runtime role gets CONNECT and nothing else. Every privilege it
		// ends up with must come from ApplyRuntimeGrants, or the capability
		// suite would be proving the setup rather than the code.
		fmt.Sprintf("GRANT CONNECT ON DATABASE %q TO %q", database, runtimeRole),
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("setup %q: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		containers.DropRole(admin, runtimeRole, t.Logf)
		containers.DropRole(admin, migrationRole, t.Logf)
	})

	migration, err := pgxpool.New(ctx, fixtureDSN(t, instance.URI, migrationRole))
	if err != nil {
		t.Fatalf("migration pool: %v", err)
	}
	t.Cleanup(migration.Close)

	const schema = "auth"
	if _, err := Apply(ctx, migration, Options{Schema: schema, RuntimeRole: runtimeRole}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Seed the policy vocabulary through the MIGRATION connection. The
	// manifest grants the runtime SELECT-only on actions/roles/role_actions
	// precisely so a service cannot widen the authority it is checked
	// against, which means a resource_grant's role_key FK can only ever be
	// satisfied by a row an operator or a migration put there. Seeding it here
	// is not a convenience: it is the deployment step the read-only posture
	// implies, and the capability suite found its absence by failing.
	for _, statement := range []string{
		`INSERT INTO auth.actions (action_key, resource_kind) VALUES ('cap.read', 'repo')`,
		`INSERT INTO auth.roles (role_key, name, scope) VALUES ('cap.role', 'Cap Role', 'organization')`,
		`INSERT INTO auth.role_actions (role_key, action_key) VALUES ('cap.role', 'cap.read')`,
	} {
		if _, err := migration.Exec(ctx, statement); err != nil {
			t.Fatalf("seed policy vocabulary %q: %v", statement, err)
		}
	}

	runtime, err := pgxpool.New(ctx, fixtureDSN(t, instance.URI, runtimeRole))
	if err != nil {
		t.Fatalf("runtime pool: %v", err)
	}
	t.Cleanup(runtime.Close)

	var whoami string
	if err := runtime.QueryRow(ctx, `SELECT current_user`).Scan(&whoami); err != nil {
		t.Fatalf("read current_user: %v", err)
	}
	if whoami != runtimeRole {
		t.Fatalf("connected as %q, want %q; every assertion below would be about the wrong role", whoami, runtimeRole)
	}

	return &authFixture{
		instance: instance, admin: admin, migration: migration, runtime: runtime,
		migrationRole: migrationRole, runtimeRole: runtimeRole,
		database: database, schema: schema,
	}
}

// firstColumn reads a column name from the catalog rather than hard-coding one
// per table. A hard-coded list would be a third parallel list beside the
// manifest and the migrations -- the exact defect this package spent two
// review rounds removing.
func (f *authFixture) firstColumn(t *testing.T, ctx context.Context, table string) string {
	t.Helper()
	var column string
	if err := f.admin.QueryRow(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
		LIMIT 1`, f.schema, table,
	).Scan(&column); err != nil {
		t.Fatalf("read a column of %s.%s: %v", f.schema, table, err)
	}
	return column
}

func fixtureDSN(t *testing.T, adminURI, role string) string {
	t.Helper()
	parsed, err := url.Parse(adminURI)
	if err != nil {
		t.Fatalf("parse admin URI: %v", err)
	}
	parsed.User = url.UserPassword(role, fixturePassword)
	return parsed.String()
}
