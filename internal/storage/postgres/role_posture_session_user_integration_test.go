//go:build integration

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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
