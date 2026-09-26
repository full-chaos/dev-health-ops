//go:build integration

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-6862 (found by the CHAOS-6804 r1 review): rolePostureQuery bound the
// login with `current_user = $1` only. A startup option (`-c role=<role>`) or a
// role setting in the DSN lets a DIFFERENT, wider login (session_user) act as
// the runtime role: current_user reads as the role, the posture passes, and the
// wide credential can RESET ROLE. Every runtime role's readiness must require
// that the pool AUTHENTICATED as the role.

const actingAsLoginPass = "acting_as_login_password"

// actingAsPool returns a pool authenticated as a fresh SUPERUSER login that is a
// member of role and connects with `-c role=<role>`, so current_user is role and
// session_user is the login. It fails the test if that plant did not take.
func actingAsPool(t *testing.T, ctx context.Context, admin *pgxpool.Pool, uri, dbName, role string, inst *containers.Instance) (*pgxpool.Pool, string) {
	t.Helper()
	login, err := containers.RoleName("acting_as_session", inst)
	if err != nil {
		t.Fatal(err)
	}
	return actingAsPoolWithLogin(t, ctx, admin, uri, dbName, role, login)
}

// actingAsPoolNamed is actingAsPool for roles a harness created without an
// Instance handle: the login name derives from the role and a label.
func actingAsPoolNamed(t *testing.T, ctx context.Context, admin *pgxpool.Pool, uri, dbName, role, label string) (*pgxpool.Pool, string) {
	t.Helper()
	login := role + "_as_" + label
	if len(login) > 60 {
		login = login[len(login)-60:]
	}
	return actingAsPoolWithLogin(t, ctx, admin, uri, dbName, role, login)
}

func actingAsPoolWithLogin(t *testing.T, ctx context.Context, admin *pgxpool.Pool, uri, dbName, role, login string) (*pgxpool.Pool, string) {
	t.Helper()
	for _, statement := range []string{
		"CREATE ROLE " + login + " LOGIN SUPERUSER PASSWORD '" + actingAsLoginPass + "'",
		"GRANT " + role + " TO " + login,
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + login,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(admin, login, t.Logf) })
	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = login, actingAsLoginPass
	config.ConnConfig.RuntimeParams["role"] = role
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var sessionUser, currentUser string
	if err := pool.QueryRow(ctx, "SELECT session_user, current_user").Scan(&sessionUser, &currentUser); err != nil {
		t.Fatal(err)
	}
	if sessionUser != login || currentUser != role {
		t.Fatalf("the plant did not produce a session acting as the role: session_user=%q current_user=%q", sessionUser, currentUser)
	}
	return pool, login
}

// sessionAuthorizationPool returns a pool authenticated as a fresh SUPERUSER login
// that runs `SET SESSION AUTHORIZATION <role>` on every new connection, so BOTH
// session_user and current_user read as the role while the authenticated user (the
// credential the DSN carries, still the superuser's) is the login. It fails the
// test if that plant did not take.
func sessionAuthorizationPool(t *testing.T, ctx context.Context, admin *pgxpool.Pool, uri, role, login string) *pgxpool.Pool {
	t.Helper()
	if _, err := admin.Exec(ctx, "CREATE ROLE "+login+" LOGIN SUPERUSER PASSWORD '"+actingAsLoginPass+"'"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, login, t.Logf) })
	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = login, actingAsLoginPass
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET SESSION AUTHORIZATION "+role)
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var sessionUser, currentUser string
	if err := pool.QueryRow(ctx, "SELECT session_user, current_user").Scan(&sessionUser, &currentUser); err != nil {
		t.Fatal(err)
	}
	if sessionUser != role || currentUser != role {
		t.Fatalf("the plant did not spoof both identities: session_user=%q current_user=%q", sessionUser, currentUser)
	}
	return pool
}

// The shared mechanism, alone: a role whose posture passes, then the same role
// reached through an acting-as login.
func TestCheckRolePostureRefusesALoginThatOnlyActsAsTheRole(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	role, err := containers.RoleName("grant_posture_session", instance)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	bootstrapAPIRole(t, ctx, admin, dbName, role)

	direct := connectAs(t, ctx, instance.URI, role, apiAuthorizationPass)
	if err := CheckRolePosture(ctx, direct, role, "river", RolePosture{}); err != nil {
		t.Fatalf("the control (a direct login as the role) failed: %v", err)
	}
	actingAs, login := actingAsPool(t, ctx, admin, instance.URI, dbName, role, instance)
	err = CheckRolePosture(ctx, actingAs, role, "river", RolePosture{})
	if !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("a login that only acts as the role (session_user=%s) passed the posture check: error %v", login, err)
	}
	if !strings.Contains(err.Error(), "session_user") || strings.Contains(err.Error(), actingAsLoginPass) {
		t.Fatalf("the refusal must name the cause (session_user) and never a credential: %v", err)
	}
}

// Every runtime role's own readiness function, each through its real posture:
// the direct login passes, the acting-as login is refused.
func TestEveryRuntimeRoleCheckRefusesALoginThatOnlyActsAsTheRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)
	dbName, err := containers.DatabaseName(uri)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		role  string
		pass  string
		check func(context.Context, *pgxpool.Pool, string, string) error
	}{
		{"domain", roles.domain, grantDomainPass, CheckDomainAuthorization},
		{"queue", roles.queue, grantQueuePass, CheckQueueAuthorization},
		{"coordinator", roles.coordinator, grantCoordinatorPass, CheckCoordinatorAuthorization},
	} {
		direct := connectAs(t, ctx, uri, test.role, test.pass)
		if err := test.check(ctx, direct, test.role, grantSchema); err != nil {
			t.Fatalf("%s: the control (a direct login) failed readiness: %v", test.name, err)
		}
		actingAs, login := actingAsPoolNamed(t, ctx, admin, uri, dbName, test.role, test.name)
		err := test.check(ctx, actingAs, test.role, grantSchema)
		if !errors.Is(err, ErrPostureRefused) {
			t.Errorf("%s: a login that only acts as the role (session_user=%s) passed readiness: error %v", test.name, login, err)
		} else if !strings.Contains(err.Error(), "session_user") {
			t.Errorf("%s: the refusal must name the cause (session_user): %v", test.name, err)
		}
	}
}

func TestCheckAPIAuthorizationRefusesALoginThatOnlyActsAsTheRole(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	role, err := containers.RoleName(apiAuthorizationRole, instance)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	bootstrapAPIRole(t, ctx, admin, dbName, role)
	acrEntitlementTables(t, ctx, admin, role)
	externalIngestTables(t, ctx, admin, role)

	direct := connectAs(t, ctx, instance.URI, role, apiAuthorizationPass)
	if err := CheckAPIAuthorization(ctx, direct, role, grantSchema); err != nil {
		t.Fatalf("the control (a direct login as the api role) failed: %v", err)
	}
	actingAs, login := actingAsPool(t, ctx, admin, instance.URI, dbName, role, instance)
	if err := CheckAPIAuthorization(ctx, actingAs, role, grantSchema); !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("api: a login that only acts as the role (session_user=%s) passed readiness: error %v", login, err)
	}
}

// A session that AUTHENTICATED as the expected role but then `SET ROLE`s to a
// different role with the same posture must not pass: current_user is who the
// session acts as, and it is bound separately from session_user. (Not an
// equivalent mutation: dropping `current_user = $1` from the query lets this pass.)
func TestCheckRolePostureRefusesASessionThatSwitchedToAnotherRoleWithTheSamePosture(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := containers.RoleName("grant_posture_expected", instance)
	if err != nil {
		t.Fatal(err)
	}
	alternate, err := containers.RoleName("grant_posture_alternate", instance)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, expected, t.Logf); containers.DropRole(admin, alternate, t.Logf) })
	bootstrapAPIRole(t, ctx, admin, dbName, expected)
	bootstrapAPIRole(t, ctx, admin, dbName, alternate)
	if _, err := admin.Exec(ctx, "GRANT "+alternate+" TO "+expected); err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = expected, apiAuthorizationPass
	config.ConnConfig.RuntimeParams["role"] = alternate
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var sessionUser, currentUser string
	if err := pool.QueryRow(ctx, "SELECT session_user, current_user").Scan(&sessionUser, &currentUser); err != nil {
		t.Fatal(err)
	}
	if sessionUser != expected || currentUser != alternate {
		t.Fatalf("the plant did not switch roles: session_user=%q current_user=%q", sessionUser, currentUser)
	}
	// The alternate role, connecting directly, passes the same (empty) posture, so
	// the refusal below can only come from the identity binding.
	if err := CheckRolePosture(ctx, connectAs(t, ctx, instance.URI, alternate, apiAuthorizationPass), alternate, "river", RolePosture{}); err != nil {
		t.Fatalf("the alternate role must pass the posture on its own: %v", err)
	}
	if err := CheckRolePosture(ctx, pool, expected, "river", RolePosture{}); !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("a session authenticated as %s but acting as %s passed the check for %s: error %v", expected, alternate, expected, err)
	}
}

// The authenticated user (pg_stat_activity.usename of THIS backend) survives
// `SET SESSION AUTHORIZATION`, which rewrites BOTH session_user and current_user:
// a superuser pool that authorizes itself as the runtime role passed a check that
// read only those two, while still holding the credential that can restore its
// identity (CHAOS-6862 r1 P1).
func TestEveryRuntimeRoleCheckRefusesASuperuserThatSetSessionAuthorizationToTheRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)
	for _, test := range []struct {
		name  string
		role  string
		check func(context.Context, *pgxpool.Pool, string, string) error
	}{
		{"domain", roles.domain, CheckDomainAuthorization},
		{"queue", roles.queue, CheckQueueAuthorization},
		{"coordinator", roles.coordinator, CheckCoordinatorAuthorization},
	} {
		login := test.role + "_sa_" + test.name
		if len(login) > 60 {
			login = login[len(login)-60:]
		}
		pool := sessionAuthorizationPool(t, ctx, admin, uri, test.role, login)
		err := test.check(ctx, pool, test.role, grantSchema)
		if !errors.Is(err, ErrPostureRefused) {
			t.Errorf("%s: a superuser pool with SET SESSION AUTHORIZATION %s passed readiness: error %v", test.name, test.role, err)
		} else if !strings.Contains(err.Error(), "authenticated") {
			t.Errorf("%s: the refusal must name the authenticated user: %v", test.name, err)
		}
	}
}

func TestCheckRolePostureRefusesASuperuserThatSetSessionAuthorizationToTheRole(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	role, err := containers.RoleName("grant_posture_sessauth", instance)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	bootstrapAPIRole(t, ctx, admin, dbName, role)
	if err := CheckRolePosture(ctx, connectAs(t, ctx, instance.URI, role, apiAuthorizationPass), role, "river", RolePosture{}); err != nil {
		t.Fatalf("the control (a direct login as the role) failed: %v", err)
	}
	pool := sessionAuthorizationPool(t, ctx, admin, instance.URI, role, "sessauth_login_"+role[len(role)-10:])
	if err := CheckRolePosture(ctx, pool, role, "river", RolePosture{}); !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("a superuser pool with SET SESSION AUTHORIZATION %s passed the posture check: error %v", role, err)
	}
}
