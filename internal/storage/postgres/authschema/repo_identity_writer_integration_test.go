//go:build integration

package authschema

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestRepoIdentityWriterProvisioningAndPosture: with the privileged step done, a non-superuser migrator hands the 0056
// function to its own writer role, EXECUTE reaches the worker login only, the writer holds nothing on
// repository_identities, and a planted drift (a table grant, an extra EXECUTE, a foreign owner) is a posture
// violation.
func TestRepoIdentityWriterProvisioningAndPosture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	migrationRole, _ := containers.RoleName("ri_migrator", instance)
	runtimeRole, _ := containers.RoleName("ri_runtime", instance)
	pushWriter, _ := containers.RoleName("ri_push_writer", instance)
	writerRole, _ := containers.RoleName("ri_writer", instance)
	workerRole, _ := containers.RoleName("ri_worker", instance)
	goAPIRole, _ := containers.RoleName("ri_goapi", instance)
	database, _ := containers.DatabaseName(instance.URI)
	const password = "repo-writer-test-not-a-real-credential"
	for _, s := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", Quote(mustValidatedIdentifier(migrationRole)), password),
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", Quote(mustValidatedIdentifier(runtimeRole)), password),
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", Quote(mustValidatedIdentifier(workerRole)), password),
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", Quote(mustValidatedIdentifier(goAPIRole)), password),
		fmt.Sprintf("CREATE ROLE %s NOLOGIN", Quote(mustValidatedIdentifier(pushWriter))),
		fmt.Sprintf("GRANT %s TO %s", Quote(mustValidatedIdentifier(pushWriter)), Quote(mustValidatedIdentifier(migrationRole))),
		fmt.Sprintf("GRANT CREATE, CONNECT ON DATABASE %s TO %s", Quote(mustValidatedIdentifier(database)), Quote(mustValidatedIdentifier(migrationRole))),
	} {
		mustExec(t, ctx, admin, s)
	}
	t.Cleanup(func() {
		for _, role := range []string{writerRole, pushWriter, goAPIRole, workerRole, runtimeRole, migrationRole} {
			containers.DropRole(admin, role, t.Logf)
		}
	})
	mig, err := pgxpool.New(ctx, dsnAs(t, instance.URI, migrationRole, password))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mig.Close)
	options := Options{Schema: "auth", RuntimeRole: runtimeRole, SessionReaderRoles: []string{goAPIRole},
		PushLifecycleWriterRole: pushWriter, PushLifecycleCallerRoles: []string{goAPIRole, workerRole},
		RepoIdentityWriterRole: writerRole, RepoIdentityCallerRoles: []string{workerRole}}
	const fn = "auth.append_repository_identity_event(uuid, text, text, text, timestamptz)"

	t.Run("a missing writer role stops the run and names the step", func(t *testing.T) {
		_, err := Apply(ctx, mig, options)
		if !errors.Is(err, ErrRepoIdentityWriter) || !strings.Contains(err.Error(), "create it first") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a migrator that is not a member is refused", func(t *testing.T) {
		mustExec(t, ctx, admin, fmt.Sprintf("CREATE ROLE %s NOLOGIN", Quote(mustValidatedIdentifier(writerRole))))
		_, err := Apply(ctx, mig, options)
		if !errors.Is(err, ErrRepoIdentityWriter) || !strings.Contains(err.Error(), "not a member") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("with the privileged step done the writer owns the function and EXECUTE reaches the worker only", func(t *testing.T) {
		mustExec(t, ctx, admin, fmt.Sprintf("GRANT %s TO %s", Quote(mustValidatedIdentifier(writerRole)), Quote(mustValidatedIdentifier(migrationRole))))
		if _, err := Apply(ctx, mig, options); err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if _, err := Apply(ctx, mig, options); err != nil {
			t.Fatalf("second Apply: %v", err)
		}
		var owner string
		var worker, goAPI, runtime, pushW, identities bool
		if err := admin.QueryRow(ctx, `SELECT (SELECT rolname FROM pg_roles WHERE oid = proowner),
			has_function_privilege($1, oid, 'EXECUTE'), has_function_privilege($2, oid, 'EXECUTE'),
			has_function_privilege($3, oid, 'EXECUTE'), has_function_privilege($4, oid, 'EXECUTE'),
			has_table_privilege($5, 'auth.repository_identities', 'SELECT,INSERT,UPDATE,DELETE')
			FROM pg_proc WHERE oid = $6::regprocedure`, workerRole, goAPIRole, runtimeRole, pushWriter, writerRole, fn).
			Scan(&owner, &worker, &goAPI, &runtime, &pushW, &identities); err != nil {
			t.Fatal(err)
		}
		if owner != writerRole || !worker || goAPI || runtime || pushW || identities {
			t.Fatalf("owner %q (want %q), EXECUTE worker=%v goapi=%v runtime=%v push_writer=%v, writer on repository_identities=%v",
				owner, writerRole, worker, goAPI, runtime, pushW, identities)
		}
	})
	violationsNow := func(t *testing.T) []Violation {
		t.Helper()
		conn, err := poolAdapter{mig}.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		violations, err := functionWriterViolations(ctx, conn, options, options.repoIdentityWriter())
		if err != nil {
			t.Fatal(err)
		}
		return violations
	}
	t.Run("the applied posture has no violation", func(t *testing.T) {
		if v := violationsNow(t); len(v) != 0 {
			t.Fatalf("violations: %+v", v)
		}
	})
	for name, plant := range map[string]struct{ plant, undo, want string }{
		"a table grant on repository_identities": {
			fmt.Sprintf("GRANT SELECT ON auth.repository_identities TO %s", Quote(mustValidatedIdentifier(writerRole))),
			fmt.Sprintf("REVOKE SELECT ON auth.repository_identities FROM %s", Quote(mustValidatedIdentifier(writerRole))),
			"holds a privilege outside its declared set"},
		"EXECUTE for go-api's login": {
			fmt.Sprintf("GRANT EXECUTE ON FUNCTION %s TO %s", fn, Quote(mustValidatedIdentifier(goAPIRole))),
			fmt.Sprintf("REVOKE EXECUTE ON FUNCTION %s FROM %s", fn, Quote(mustValidatedIdentifier(goAPIRole))),
			"is not a caller"},
		"a foreign owner": {
			fmt.Sprintf("ALTER FUNCTION %s OWNER TO %s", fn, Quote(mustValidatedIdentifier(runtimeRole))),
			fmt.Sprintf("ALTER FUNCTION %s OWNER TO %s", fn, Quote(mustValidatedIdentifier(writerRole))),
			"does not own the append function"},
	} {
		t.Run("planted drift: "+name, func(t *testing.T) {
			mustExec(t, ctx, admin, plant.plant)
			defer mustExec(t, ctx, admin, plant.undo)
			for _, v := range violationsNow(t) {
				if strings.Contains(v.Detail, plant.want) {
					return
				}
			}
			t.Fatalf("no violation containing %q", plant.want)
		})
	}
}
