//go:build integration

package rivermigrate_test

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// The api role's memberships and organizations grants follow AUTH_ORGS_OWNED
// through the real migrate command, over the real ops schema, as the real
// login: on, memberships is read only, no organization is inserted, and the
// organization columns auth-service writes stay out of an UPDATE; off again,
// a migrate run grants everything back (the rollback). The exact-posture
// check go-api runs at readiness agrees with each state and refuses the other.
func TestMigrateRiverOrgsOwnedRevokesThenRegrantsTheExactPosture(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)

	domain, queue, coordinator, api := randomName(t, "oo_domain_"), randomName(t, "oo_queue_"), randomName(t, "oo_coord_"), randomName(t, "oo_api_")
	for _, role := range []string{domain, queue, coordinator, api} {
		if _, err := admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'x'"); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, role := range []string{domain, queue, coordinator, api} {
			_, _ = admin.Exec(context.Background(), "DROP OWNED BY "+role)
			_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
		}
	})
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(api, "x")
	apiPool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(apiPool.Close)

	migrate := func(orgs string) (int, string) {
		settings := map[string]string{
			"MIGRATION_DATABASE_URI":          instance.URI,
			"RIVER_DOMAIN_DATABASE_ROLE":      domain,
			"RIVER_QUEUE_DATABASE_ROLE":       queue,
			"RIVER_COORDINATOR_DATABASE_ROLE": coordinator,
			"API_DATABASE_ROLE":               api,
		}
		if orgs != "" {
			settings["AUTH_ORGS_OWNED"] = orgs
		}
		var stdout, stderr bytes.Buffer
		lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
		return rivermigrate.Execute(ctx, "dho", nil, lookup, &stdout, &stderr), stderr.String()
	}
	// probe runs one statement as the api login in a transaction it rolls back, and returns the SQLSTATE ("" on success).
	probe := func(statement string, args ...any) string {
		t.Helper()
		tx, err := apiPool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		_, err = tx.Exec(ctx, statement, args...)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return pgErr.Code
		}
		if err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		return ""
	}
	const denied = "42501"
	wantMemberships := func(label string, writable bool) {
		t.Helper()
		want := denied
		if writable {
			want = ""
		}
		for _, statement := range []string{
			`INSERT INTO memberships (id, user_id, org_id) VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid())`,
			`UPDATE memberships SET role = 'member' WHERE false`,
			`DELETE FROM memberships WHERE false`,
			`SELECT 1 FROM memberships WHERE false FOR UPDATE`,
			`SELECT 1 FROM memberships WHERE false FOR KEY SHARE`,
		} {
			got := probe(statement)
			if got != want && !(writable && got == "23503") {
				t.Errorf("%s: %s = %q, want %q", label, statement, got, want)
			}
		}
		if got := probe(`TRUNCATE memberships`); got != denied {
			t.Errorf("%s: TRUNCATE memberships = %q, want %q", label, got, denied)
		}
		if got := probe(`SELECT count(*) FROM memberships`); got != "" {
			t.Errorf("%s: SELECT on memberships = %q, want success", label, got)
		}
	}
	wantOrganizations := func(label string, owned bool) {
		t.Helper()
		write, other := "", ""
		if owned {
			write = denied
		}
		for _, statement := range []string{
			`INSERT INTO organizations (id, slug, name) VALUES (gen_random_uuid(), 'x', 'x')`,
			`UPDATE organizations SET name = 'x' WHERE false`,
			`UPDATE organizations SET slug = 'x' WHERE false`,
			`UPDATE organizations SET is_active = false WHERE false`,
		} {
			if got := probe(statement); got != write {
				t.Errorf("%s: %s = %q, want %q", label, statement, got, write)
			}
		}
		for _, statement := range []string{
			`UPDATE organizations SET description = 'x', tier = 'free', managed_by = 'stripe', updated_at = now() WHERE false`,
			`UPDATE organizations SET onboarding_integration_skipped_at = now() WHERE false`,
			`DELETE FROM organizations WHERE false`,
			`SELECT 1 FROM organizations WHERE false FOR NO KEY UPDATE`,
		} {
			if got := probe(statement); got != other {
				t.Errorf("%s: %s = %q, want success", label, statement, got)
			}
		}
	}
	exact := func(label string, orgs bool) {
		t.Helper()
		if err := postgresstore.CheckAPIAuthorizationFor(ctx, apiPool, api, "river", false, orgs); err != nil {
			t.Fatalf("%s: the api role does not hold the orgsOwned=%v manifest: %v", label, orgs, err)
		}
		if err := postgresstore.CheckAPIAuthorizationFor(ctx, apiPool, api, "river", false, !orgs); !errors.Is(err, postgresstore.ErrPostureRefused) {
			t.Fatalf("%s: the orgsOwned=%v manifest was accepted: %v", label, !orgs, err)
		}
	}

	if code, stderr := migrate(""); code != cli.ExitOK {
		t.Fatalf("flag unset: exit %d\n%s", code, stderr)
	}
	exact("flag unset", false)
	wantMemberships("flag unset", true)
	wantOrganizations("flag unset", false)

	if code, stderr := migrate("true"); code != cli.ExitOK {
		t.Fatalf("flag on: exit %d\n%s", code, stderr)
	}
	exact("flag on", true)
	wantMemberships("flag on", false)
	wantOrganizations("flag on", true)

	// A held membership write is refused by the readiness check while the flag is on.
	if _, err := admin.Exec(ctx, "GRANT INSERT ON memberships TO "+api); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckAPIAuthorizationFor(ctx, apiPool, api, "river", false, true); !errors.Is(err, postgresstore.ErrPostureRefused) {
		t.Fatalf("a held memberships INSERT was accepted: %v", err)
	}
	if code, stderr := migrate("true"); code != cli.ExitOK {
		t.Fatalf("flag on again: exit %d\n%s", code, stderr)
	}
	exact("flag on again", true)

	// Org purge works with the revoked role: the row goes, and the membership with it.
	orgID, userID := uuid.New(), uuid.New()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name) VALUES ($1, $2, 'purge')`, []any{orgID, "purge-" + orgID.String()}},
		{`INSERT INTO users (id, email) VALUES ($1, $2)`, []any{userID, userID.String() + "@example.invalid"}},
		{`INSERT INTO memberships (id, user_id, org_id) VALUES (gen_random_uuid(), $1, $2)`, []any{userID, orgID}},
	} {
		if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := apiPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM organizations WHERE id = $1 FOR NO KEY UPDATE`, orgID); err != nil {
		t.Fatalf("org lock: %v", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM organizations WHERE id = $1`, orgID); err != nil {
		t.Fatalf("org delete with the revoked role: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM memberships WHERE org_id = $1`, orgID).Scan(&left); err != nil || left != 0 {
		t.Fatalf("memberships left after the purge = %d (err %v), want 0", left, err)
	}

	if code, stderr := migrate("yes"); code != 1 || !strings.Contains(stderr, "AUTH_ORGS_OWNED") {
		t.Fatalf("an unparsable flag: exit %d, stderr:\n%s", code, stderr)
	}
	exact("after a refused run", true)

	if code, stderr := migrate("false"); code != cli.ExitOK {
		t.Fatalf("flag off again: exit %d\n%s", code, stderr)
	}
	exact("flag off again (rollback)", false)
	wantMemberships("flag off again (rollback)", true)
	wantOrganizations("flag off again (rollback)", false)
}
