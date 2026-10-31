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

// TestPushLifecycleWriterProvisioningWithANonSuperuserMigrator is the roll
// order made executable: the writer role and the migrator's membership in it
// are created by a privileged step first; with them in place a migrator that is
// NOT a superuser hands the function over, the temporary CREATE leaves no
// trace, and every missing piece stops the run with a message that names the
// step.
func TestPushLifecycleWriterProvisioningWithANonSuperuserMigrator(t *testing.T) {
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
	migrationRole, _ := containers.RoleName("pw_migrator", instance)
	runtimeRole, _ := containers.RoleName("pw_runtime", instance)
	writerRole, _ := containers.RoleName("pw_writer", instance)
	callerRole, _ := containers.RoleName("pw_caller", instance)
	database, _ := containers.DatabaseName(instance.URI)
	const password = "writer-test-not-a-real-credential"
	for _, s := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", Quote(mustValidatedIdentifier(migrationRole)), password),
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", Quote(mustValidatedIdentifier(runtimeRole)), password),
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", Quote(mustValidatedIdentifier(callerRole)), password),
		fmt.Sprintf("GRANT CREATE, CONNECT ON DATABASE %s TO %s", Quote(mustValidatedIdentifier(database)), Quote(mustValidatedIdentifier(migrationRole))),
		fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s, %s", Quote(mustValidatedIdentifier(database)), Quote(mustValidatedIdentifier(runtimeRole)), Quote(mustValidatedIdentifier(callerRole))),
	} {
		if _, err := admin.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	t.Cleanup(func() {
		for _, role := range []string{writerRole, callerRole, runtimeRole, migrationRole} {
			containers.DropRole(admin, role, t.Logf)
		}
	})
	mig, err := pgxpool.New(ctx, dsnAs(t, instance.URI, migrationRole, password))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mig.Close)
	options := Options{Schema: "auth", RuntimeRole: runtimeRole, SessionReaderRoles: []string{callerRole},
		PushLifecycleWriterRole: writerRole, PushLifecycleCallerRoles: []string{callerRole}}

	t.Run("a missing writer role stops the run and names the step", func(t *testing.T) {
		_, err := Apply(ctx, mig, options)
		if !errors.Is(err, ErrPushLifecycleWriter) || !strings.Contains(err.Error(), "create it first") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a writer that can log in is refused", func(t *testing.T) {
		mustExec(t, ctx, admin, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", Quote(mustValidatedIdentifier(writerRole)), password))
		_, err := Apply(ctx, mig, options)
		if !errors.Is(err, ErrPushLifecycleWriter) || !strings.Contains(err.Error(), "NOLOGIN") {
			t.Fatalf("err = %v", err)
		}
		mustExec(t, ctx, admin, fmt.Sprintf("ALTER ROLE %s NOLOGIN", Quote(mustValidatedIdentifier(writerRole))))
	})
	t.Run("a migrator that is not a member is refused", func(t *testing.T) {
		_, err := Apply(ctx, mig, options)
		if !errors.Is(err, ErrPushLifecycleWriter) || !strings.Contains(err.Error(), "not a member") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("with the privileged step done the migrator hands the function over", func(t *testing.T) {
		mustExec(t, ctx, admin, fmt.Sprintf("GRANT %s TO %s", Quote(mustValidatedIdentifier(writerRole)), Quote(mustValidatedIdentifier(migrationRole))))
		if _, err := Apply(ctx, mig, options); err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if _, err := Apply(ctx, mig, options); err != nil {
			t.Fatalf("second Apply: %v", err)
		}
		var owner string
		var create bool
		if err := admin.QueryRow(ctx, `SELECT (SELECT rolname FROM pg_roles WHERE oid = proowner), has_schema_privilege($1, 'auth', 'CREATE')
			FROM pg_proc WHERE oid = 'auth.append_external_push_lifecycle_event(text,text,jsonb,jsonb)'::regprocedure`, writerRole).Scan(&owner, &create); err != nil {
			t.Fatal(err)
		}
		if owner != writerRole || create {
			t.Fatalf("owner %q (want %q), writer CREATE on auth = %v (want false)", owner, writerRole, create)
		}
		var acl string
		if err := admin.QueryRow(ctx, `SELECT coalesce(nspacl::text, '') FROM pg_namespace WHERE nspname = 'auth'`).Scan(&acl); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(acl, writerRole+"=C") || strings.Contains(acl, writerRole+"=UC") {
			t.Fatalf("the schema ACL shows CREATE for the writer: %s", acl)
		}
	})
}

func mustExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}
