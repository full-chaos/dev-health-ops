//go:build integration

package authschema

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// bindingUpgrade is a database with a NOSUPERUSER migration login that is a
// member of the NOLOGIN push lifecycle writer role, the production form
// (CHAOS-10352). The login owns the auth schema and holds no other privilege.
type bindingUpgrade struct {
	admin   *pgxpool.Pool
	uri     string
	options Options
	login   string
	writer  string
	caller  string
}

// bindingMigration finds the binding migration by name, so a renumber at merge
// is a file rename only.
func bindingMigration(t *testing.T) Migration {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.Name == "external_push_lifecycle_binding" {
			return migration
		}
	}
	t.Fatal("no migration named external_push_lifecycle_binding")
	return Migration{}
}

func newBindingUpgrade(t *testing.T, ctx context.Context, inherit bool) *bindingUpgrade {
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

	names := map[string]string{}
	for _, prefix := range []string{"auth_bind_login", "auth_bind_runtime", "auth_bind_writer", "auth_bind_caller"} {
		name, err := containers.RoleName(prefix, instance)
		if err != nil {
			t.Fatal(err)
		}
		names[prefix] = name
	}
	database, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	login, runtime, writer, caller := names["auth_bind_login"], names["auth_bind_runtime"], names["auth_bind_writer"], names["auth_bind_caller"]
	loginID, runtimeID := mustValidatedIdentifier(login), mustValidatedIdentifier(runtime)
	writerID, callerID := mustValidatedIdentifier(writer), mustValidatedIdentifier(caller)
	membership := fmt.Sprintf("GRANT %s TO %s WITH INHERIT TRUE, SET TRUE", Quote(writerID), Quote(loginID))
	if !inherit {
		membership = fmt.Sprintf("GRANT %s TO %s WITH INHERIT FALSE, SET TRUE", Quote(writerID), Quote(loginID))
	}
	for _, statement := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN NOSUPERUSER PASSWORD '%s'", Quote(loginID), fixturePassword),
		fmt.Sprintf("CREATE ROLE %s", Quote(runtimeID)),
		fmt.Sprintf("CREATE ROLE %s NOLOGIN", Quote(writerID)),
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", Quote(callerID), fixturePassword),
		fmt.Sprintf("GRANT CREATE, CONNECT ON DATABASE %s TO %s", Quote(mustValidatedIdentifier(database)), Quote(loginID)),
		membership,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("setup %q: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		containers.DropRole(admin, caller, t.Logf)
		containers.DropRole(admin, writer, t.Logf)
		containers.DropRole(admin, runtime, t.Logf)
		containers.DropRole(admin, login, t.Logf)
	})
	return &bindingUpgrade{
		admin: admin, uri: fixtureDSN(t, instance.URI, login), login: login, writer: writer, caller: caller,
		options: Options{
			Schema: "auth", RuntimeRole: runtime, SessionReaderRoles: []string{caller},
			PushLifecycleWriterRole: writer, PushLifecycleCallerRoles: []string{caller},
		},
	}
}

// installBeforeBinding applies the lineage up to the binding migration as the login,
// then hands the function to the writer the way a grants run does: the state
// production is in before 0045 rolls.
func (u *bindingUpgrade) installBeforeBinding(t *testing.T, ctx context.Context) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, u.uri)
	if err != nil {
		t.Fatalf("connect as the migration login: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if err := ensureSchemaAndVersionTable(ctx, conn, u.options); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.Version >= bindingMigration(t).Version {
			break
		}
		if err := applyOne(ctx, conn, u.options, migration); err != nil {
			t.Fatalf("apply %04d_%s: %v", migration.Version, migration.Name, err)
		}
	}
	writer := Quote(mustValidatedIdentifier(u.writer))
	for _, statement := range []string{
		`GRANT CREATE ON SCHEMA auth TO ` + writer,
		`ALTER FUNCTION auth.` + PushLifecycleFunction + PushLifecycleFunctionSignature + ` OWNER TO ` + writer,
		`REVOKE CREATE ON SCHEMA auth FROM ` + writer,
	} {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("hand-over %q: %v", statement, err)
		}
	}
	return conn
}

func (u *bindingUpgrade) requireBindingFunction(t *testing.T, ctx context.Context) {
	t.Helper()
	var owner, source string
	var definer bool
	var config []string
	var public bool
	var callerRuns bool
	if err := u.admin.QueryRow(ctx, `SELECT o.rolname, p.prosecdef, coalesce(p.proconfig, ARRAY[]::text[]), p.prosrc,
			EXISTS (SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a WHERE a.grantee = 0),
			has_function_privilege($1, p.oid, 'EXECUTE')
		FROM pg_proc p JOIN pg_roles o ON o.oid = p.proowner
		WHERE p.oid = 'auth.append_external_push_lifecycle_event(text,text,jsonb,jsonb)'::regprocedure`, u.caller).
		Scan(&owner, &definer, &config, &source, &public, &callerRuns); err != nil {
		t.Fatalf("read the function: %v", err)
	}
	if owner != u.writer {
		t.Errorf("owner = %q, want the writer role %q", owner, u.writer)
	}
	if !definer {
		t.Error("the function is not SECURITY DEFINER")
	}
	pinned := false
	for _, entry := range config {
		pinned = pinned || entry == "search_path=auth, pg_temp"
	}
	if !pinned {
		t.Errorf("search_path is not pinned to \"auth, pg_temp\": proconfig = %v", config)
	}
	if public {
		t.Error("PUBLIC can execute the function")
	}
	if !callerRuns {
		t.Error("the caller cannot execute the function")
	}
	if !strings.Contains(source, "'binding'") {
		t.Error("the function body is still the 0044 definition (no binding)")
	}
}

// TestPushLifecycleBindingUpgradeRunsAsAnInheritingMemberOfTheWriter is the
// previous head -> binding roll in the production form: a NOSUPERUSER login that is a member
// of the writer role replaces a function the writer owns.
func TestPushLifecycleBindingUpgradeRunsAsAnInheritingMemberOfTheWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	u := newBindingUpgrade(t, ctx, true)
	u.installBeforeBinding(t, ctx)

	var before string
	if err := u.admin.QueryRow(ctx, `SELECT o.rolname FROM pg_proc p JOIN pg_roles o ON o.oid = p.proowner
		WHERE p.oid = 'auth.append_external_push_lifecycle_event(text,text,jsonb,jsonb)'::regprocedure`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before != u.writer {
		t.Fatalf("before 0045 the function is owned by %q, want the writer %q", before, u.writer)
	}

	pool, err := pgxpool.New(ctx, u.uri)
	if err != nil {
		t.Fatalf("migration pool: %v", err)
	}
	t.Cleanup(pool.Close)
	result, err := Apply(ctx, pool, u.options)
	if err != nil {
		t.Fatalf("Apply 0044 -> head as a non-superuser member of the writer: %v", err)
	}
	head, err := HeadVersion()
	if err != nil {
		t.Fatal(err)
	}
	if result.CurrentVersion != head || len(result.AppliedVersions) == 0 || result.AppliedVersions[0] != bindingMigration(t).Version {
		t.Fatalf("result = %+v, want the run to start at 45 and reach head %d", result, head)
	}
	u.requireBindingFunction(t, ctx)
}

// TestPushLifecycleBindingFreshInstallReachesHeadInOneApply is the fresh row:
// 0044 creates the function as the login and 0045 replaces it in the same run.
func TestPushLifecycleBindingFreshInstallReachesHeadInOneApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	u := newBindingUpgrade(t, ctx, true)
	pool, err := pgxpool.New(ctx, u.uri)
	if err != nil {
		t.Fatalf("migration pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := Apply(ctx, pool, u.options); err != nil {
		t.Fatalf("Apply on an empty database: %v", err)
	}
	u.requireBindingFunction(t, ctx)
}

// TestPushLifecycleBindingNeedsAnInheritingMembership is the control for the
// deployment note: CREATE OR REPLACE needs the owner's privileges, so a
// membership that can SET ROLE but does not INHERIT is refused.
func TestPushLifecycleBindingNeedsAnInheritingMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	u := newBindingUpgrade(t, ctx, false)
	conn := u.installBeforeBinding(t, ctx)
	err := applyOne(ctx, conn, u.options, bindingMigration(t))
	if err == nil {
		t.Fatal("0045 was applied by a login that does not inherit the writer's privileges")
	}
	var pgErr *pgconn.PgError
	if !(errors.As(err, &pgErr) && pgErr.Code == "42501") && !strings.Contains(err.Error(), "must be owner of function") {
		t.Fatalf("err = %v, want a permission error (42501, must be owner of function)", err)
	}
}
