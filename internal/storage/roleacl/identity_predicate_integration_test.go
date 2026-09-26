//go:build integration

package roleacl

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// IdentityPredicateSQL is the ONE definition of "this pool IS the role" (CHAOS-6862,
// CHAOS-6804 D2616). Every readiness check embeds it, and the site guards only prove the
// text is present; the postgres package's per-role tests prove each check refuses a
// planted login. This test pins the DEFINITION itself, executed on a real server against
// every session shape that must and must not count as "the role", so a mutation of the
// predicate (a dropped clause, `TRUE OR (...)`) fails HERE, at the source, whichever
// checks embed it.
func TestIdentityPredicateAcceptsOnlyAPoolAuthenticatedAsAnUnprivilegedMembershipFreeLogin(t *testing.T) {
	t.Parallel()
	results := runIdentityCases(t, IdentityPredicateSQL)
	for _, c := range results {
		if c.got != c.want {
			t.Errorf("%s: predicate = %v, want %v", c.name, c.got, c.want)
		}
	}
	if len(results) < 12 {
		t.Fatalf("only %d session shapes were exercised; the matrix shrank", len(results))
	}
}

// The test can fail: each single-clause mutation of the predicate is refused by at
// least one case (the mutants a reviewer would try: authenticated user dropped,
// attributes dropped, membership dropped, `TRUE OR (...)`, always false).
//
// TWO clauses are redundant defence in depth and their mutants are EQUIVALENT, stated
// and asserted rather than hidden: `session_user = $1` and `current_user = $1`. Given the
// authenticated user IS the role, the role is an unprivileged login (so it is no
// superuser and cannot SET SESSION AUTHORIZATION) and a member of no role (so it has no
// role to SET ROLE to), session_user and current_user cannot differ from it. If a session
// shape that separates them is ever found, the equivalence assertion below fails and the
// clause becomes load-bearing.
func TestIdentityPredicateMutantsAreEachCaughtByTheMatrix(t *testing.T) {
	t.Parallel()
	original := IdentityPredicateSQL
	mutants := map[string]string{
		"authenticated-user clause dropped": strings.Replace(original,
			"(SELECT usename FROM pg_catalog.pg_stat_activity WHERE pid = pg_backend_pid()) = $1\n\tAND ", "", 1),
		"attributes clause dropped": strings.Replace(original, "AND "+RoleAttributesSQL+"\n\t", "", 1),
		"membership clause dropped": strings.Replace(original, "AND "+MembershipFreeSQL+"\n", "", 1),
		"TRUE OR (...)":             "(TRUE OR " + original + ")",
		"always false":              "(FALSE AND " + original + ")",
	}
	equivalent := map[string]string{
		"session_user clause dropped": strings.Replace(original, "AND session_user = $1\n\t", "", 1),
		"current_user clause dropped": strings.Replace(original, "AND current_user = $1\n\t", "", 1),
	}
	for name, mutant := range equivalent {
		if mutant == original {
			t.Fatalf("%s: the mutation did not apply (the predicate text changed shape)", name)
		}
		for _, c := range runIdentityCases(t, mutant) {
			if c.got != c.want {
				t.Errorf("%s is NOT equivalent any more: case %q now separates it from the definition; the clause is load-bearing, remove it from the equivalent list", name, c.name)
			}
		}
	}
	for name, mutant := range mutants {
		if mutant == original {
			t.Fatalf("%s: the mutation did not apply (the predicate text changed shape)", name)
		}
		caught := false
		for _, c := range runIdentityCases(t, mutant) {
			if c.got != c.want {
				caught = true
			}
		}
		if !caught {
			t.Errorf("%s: no session shape distinguishes this mutant from the definition", name)
		}
	}
}

type identityCase struct {
	name string
	want bool
	got  bool
}

// runIdentityCases opens one real PostgreSQL and evaluates `SELECT <predicate>` in every
// session shape, binding $1 to the role under test.
func runIdentityCases(t *testing.T, predicate string) []identityCase {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	const password = "identity-pw"
	for _, statement := range []string{
		"CREATE ROLE idn_plain LOGIN PASSWORD '" + password + "'",
		"CREATE ROLE idn_other LOGIN PASSWORD '" + password + "'",
		"CREATE ROLE idn_group NOLOGIN",
		"CREATE ROLE idn_member LOGIN PASSWORD '" + password + "' IN ROLE idn_group",
		"CREATE ROLE idn_super LOGIN SUPERUSER PASSWORD '" + password + "'",
		"CREATE ROLE idn_createdb LOGIN CREATEDB PASSWORD '" + password + "'",
		"CREATE ROLE idn_createrole LOGIN CREATEROLE PASSWORD '" + password + "'",
		"CREATE ROLE idn_replication LOGIN REPLICATION PASSWORD '" + password + "'",
		"CREATE ROLE idn_bypass LOGIN BYPASSRLS PASSWORD '" + password + "'",
		// A wider login that is a member of idn_plain, able to act as it.
		"CREATE ROLE idn_actor LOGIN SUPERUSER PASSWORD '" + password + "' IN ROLE idn_plain",
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	database, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"idn_plain", "idn_other", "idn_member", "idn_super", "idn_createdb", "idn_createrole", "idn_replication", "idn_bypass", "idn_actor"} {
		if _, err := admin.Exec(ctx, "GRANT CONNECT ON DATABASE "+database+" TO "+role); err != nil {
			t.Fatal(err)
		}
	}

	pool := func(login string, mutate func(*pgxpool.Config)) *pgxpool.Pool {
		config, err := pgxpool.ParseConfig(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		config.ConnConfig.User, config.ConnConfig.Password = login, password
		config.MaxConns = 1
		if mutate != nil {
			mutate(config)
		}
		p, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			t.Fatalf("connect as %s: %v", login, err)
		}
		t.Cleanup(p.Close)
		return p
	}
	evaluate := func(p *pgxpool.Pool, role string) bool {
		var ok bool
		if err := p.QueryRow(ctx, "SELECT "+predicate, role).Scan(&ok); err != nil {
			t.Fatalf("evaluate %q: %v", role, err)
		}
		return ok
	}
	actingAs := func(login, role string) *pgxpool.Pool {
		return pool(login, func(c *pgxpool.Config) { c.ConnConfig.RuntimeParams["role"] = role })
	}
	sessionAuthorization := func(login, role string) *pgxpool.Pool {
		return pool(login, func(c *pgxpool.Config) {
			c.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
				_, err := conn.Exec(ctx, "SET SESSION AUTHORIZATION "+role)
				return err
			}
		})
	}
	setRole := func(login, role string) *pgxpool.Pool {
		return pool(login, func(c *pgxpool.Config) {
			c.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
				_, err := conn.Exec(ctx, "SET ROLE "+role)
				return err
			}
		})
	}

	var cases []identityCase
	add := func(name string, want bool, p *pgxpool.Pool, role string) {
		cases = append(cases, identityCase{name: name, want: want, got: evaluate(p, role)})
	}
	plain := pool("idn_plain", nil)
	add("the unprivileged login asked about itself", true, plain, "idn_plain")
	add("the unprivileged login asked about a different role", false, plain, "idn_other")
	add("a login acting as the role through a startup option (session_user is the login)", false, actingAs("idn_actor", "idn_plain"), "idn_plain")
	add("a superuser login with SET SESSION AUTHORIZATION to the role (both users read as the role)", false, sessionAuthorization("idn_actor", "idn_plain"), "idn_plain")
	add("a superuser login that ran SET ROLE to the role", false, setRole("idn_actor", "idn_plain"), "idn_plain")
	add("the superuser login asked about itself (SUPERUSER)", false, pool("idn_super", nil), "idn_super")
	add("a CREATEDB login asked about itself", false, pool("idn_createdb", nil), "idn_createdb")
	add("a CREATEROLE login asked about itself", false, pool("idn_createrole", nil), "idn_createrole")
	add("a REPLICATION login asked about itself", false, pool("idn_replication", nil), "idn_replication")
	add("a BYPASSRLS login asked about itself", false, pool("idn_bypass", nil), "idn_bypass")
	add("a login that is a member of another role asked about itself", false, pool("idn_member", nil), "idn_member")
	add("the acting login asked about ITSELF (a SUPERUSER)", false, pool("idn_actor", nil), "idn_actor")
	add("a role that does not exist", false, plain, "idn_missing")
	// The unprivileged login again after the others ran (no cross-talk between pools).
	add("the unprivileged login asked about itself, again", true, plain, "idn_plain")
	return cases
}
