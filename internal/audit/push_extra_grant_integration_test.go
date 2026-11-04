//go:build integration

package audit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestPushLifecycleGrantPassRevokesAnUndeclaredDirectGrant (CHAOS-12828): a
// direct EXECUTE grant to a login the options do not name (not the runtime
// role, not a reader) must not survive the grant pass. A later pass changes
// nothing.
func TestPushLifecycleGrantPassRevokesAnUndeclaredDirectGrant(t *testing.T) {
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
	names := map[string]string{}
	for _, label := range []string{"runtime", "api", "domain", "extra", "writer"} {
		name, err := containers.RoleName("xg_"+label, instance)
		if err != nil {
			t.Fatal(err)
		}
		names[label] = name
	}
	for _, label := range []string{"api", "domain", "extra"} {
		mustAdmin(t, ctx, admin, fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD 'extra-grant-test-not-a-real-credential'", names[label]))
	}
	mustAdmin(t, ctx, admin, fmt.Sprintf("CREATE ROLE %q", names["runtime"]))
	mustAdmin(t, ctx, admin, fmt.Sprintf("CREATE ROLE %q NOLOGIN", names["writer"]))
	t.Cleanup(func() {
		for _, label := range []string{"writer", "extra", "domain", "api", "runtime"} {
			containers.DropRole(admin, names[label], t.Logf)
		}
	})
	options := authschema.Options{
		Schema: "auth", RuntimeRole: names["runtime"],
		PushLifecycleWriterRole: names["writer"], PushLifecycleCallerRoles: []string{names["api"], names["domain"]},
	}
	if _, err := authschema.Apply(ctx, admin, options); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	const fn = `'auth.append_external_push_lifecycle_event(text,text,jsonb,jsonb)'::regprocedure`
	mustAdmin(t, ctx, admin, fmt.Sprintf(`GRANT USAGE ON SCHEMA auth TO %q`, names["extra"]))
	mustAdmin(t, ctx, admin, fmt.Sprintf(`GRANT EXECUTE ON FUNCTION auth.append_external_push_lifecycle_event(text,text,jsonb,jsonb) TO %q`, names["extra"]))
	if _, err := authschema.Apply(ctx, admin, options); err != nil {
		t.Fatalf("Apply after the extra grant: %v", err)
	}
	can := func(role string) bool {
		var v bool
		if err := admin.QueryRow(ctx, `SELECT has_function_privilege($1, `+fn+`, 'EXECUTE')`, role).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if can(names["extra"]) {
		t.Fatal("the undeclared login still holds EXECUTE after the grant pass")
	}
	for _, label := range []string{"api", "domain", "writer"} {
		if !can(names[label]) {
			t.Fatalf("%s lost EXECUTE", label)
		}
	}
	var before, after string
	acl := `SELECT coalesce(proacl::text, '') FROM pg_proc WHERE oid = ` + fn
	if err := admin.QueryRow(ctx, acl).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := authschema.Apply(ctx, admin, options); err != nil {
		t.Fatalf("Apply, second pass: %v", err)
	}
	if err := admin.QueryRow(ctx, acl).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("a second pass changed the ACL: %q -> %q", before, after)
	}
}
