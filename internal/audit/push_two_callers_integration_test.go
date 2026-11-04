//go:build integration

package audit

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestPushLifecycleWriterHasExactlyTheTwoDeclaredCallers is the CHAOS-12828
// grant made executable: with callers = the go-api-like login and the
// worker-domain-like login, each can call the writer and write only through
// it, and no other login (a session reader that is not a caller, a login
// with no auth privilege) can execute it. A second grant pass changes nothing.
func TestPushLifecycleWriterHasExactlyTheTwoDeclaredCallers(t *testing.T) {
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
	for _, label := range []string{"runtime", "api", "domain", "reader", "other", "writer"} {
		name, err := containers.RoleName("two_"+label, instance)
		if err != nil {
			t.Fatal(err)
		}
		names[label] = name
	}
	const password = "two-callers-test-not-a-real-credential"
	for _, label := range []string{"api", "domain", "reader", "other"} {
		mustAdmin(t, ctx, admin, fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", names[label], password))
	}
	mustAdmin(t, ctx, admin, fmt.Sprintf("CREATE ROLE %q", names["runtime"]))
	mustAdmin(t, ctx, admin, fmt.Sprintf("CREATE ROLE %q NOLOGIN", names["writer"]))
	t.Cleanup(func() {
		for _, label := range []string{"writer", "other", "reader", "domain", "api", "runtime"} {
			containers.DropRole(admin, names[label], t.Logf)
		}
	})
	options := authschema.Options{
		Schema: "auth", RuntimeRole: names["runtime"],
		// api is a session reader as go-api's login is; domain is not; reader is a session reader that is no caller.
		SessionReaderRoles:      []string{names["api"], names["reader"]},
		PushLifecycleWriterRole: names["writer"], PushLifecycleCallerRoles: []string{names["api"], names["domain"]},
	}
	for pass := 1; pass <= 2; pass++ {
		if _, err := authschema.Apply(ctx, admin, options); err != nil {
			t.Fatalf("Apply pass %d: %v", pass, err)
		}
	}

	t.Run("EXECUTE is held by the writer and exactly the two callers", func(t *testing.T) {
		rows, err := admin.Query(ctx, `SELECT coalesce(r.rolname, 'PUBLIC') FROM pg_proc p
			CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
			LEFT JOIN pg_roles r ON r.oid = a.grantee
			WHERE p.oid = 'auth.append_external_push_lifecycle_event(text,text,jsonb,jsonb)'::regprocedure AND a.privilege_type = 'EXECUTE'`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var got []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			got = append(got, name)
		}
		want := []string{names["api"], names["domain"], names["writer"]}
		sort.Strings(got)
		sort.Strings(want)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("EXECUTE holders = %v, want %v", got, want)
		}
	})
	t.Run("the callers hold no table privilege in the auth schema", func(t *testing.T) {
		for _, label := range []string{"api", "domain"} {
			var n int
			if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
				CROSS JOIN unnest(ARRAY['INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']) p
				WHERE n.nspname = 'auth' AND c.relkind IN ('r','p') AND has_table_privilege($1, c.oid, p)`, names[label]).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatalf("%s holds %d write-class table privileges in auth", label, n)
			}
			if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_class c WHERE c.relname = 'aggregate_revisions'
				AND c.relnamespace = 'auth'::regnamespace AND has_table_privilege($1, c.oid, 'SELECT')`, names[label]).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatalf("%s can SELECT auth.aggregate_revisions", label)
			}
		}
	})
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	asRole := func(t *testing.T, role string) *pgxpool.Pool {
		t.Helper()
		parsed, err := url.Parse(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		parsed.User = url.UserPassword(role, password)
		pool, err := pgxpool.New(ctx, parsed.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	for i, label := range []string{"api", "domain"} {
		t.Run("the writer call succeeds as "+label, func(t *testing.T) {
			pool := asRole(t, names[label])
			id := fmt.Sprintf("00000000-0000-4000-8000-00000000f%03d", i+1)
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if err := AppendPushLifecycle(ctx, tx, schema, []PushLifecycleEvent{pushCreated(id, "ingest:write")}); err != nil {
				t.Fatalf("AppendPushLifecycle as %s: %v", label, err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			var revision int64
			if err := admin.QueryRow(ctx, `SELECT revision FROM auth.aggregate_revisions WHERE aggregate_type = 'external_push_credential' AND aggregate_id = $1`, id).Scan(&revision); err != nil {
				t.Fatal(err)
			}
			if revision != 1 {
				t.Fatalf("revision = %d, want 1", revision)
			}
		})
	}
	for _, label := range []string{"reader", "other", "runtime"} {
		t.Run("no EXECUTE for "+label, func(t *testing.T) {
			var can bool
			if err := admin.QueryRow(ctx, `SELECT has_function_privilege($1, 'auth.append_external_push_lifecycle_event(text,text,jsonb,jsonb)'::regprocedure, 'EXECUTE')`, names[label]).Scan(&can); err != nil {
				t.Fatal(err)
			}
			if can {
				t.Fatalf("%s can execute the writer", label)
			}
		})
	}
}

func mustAdmin(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}
