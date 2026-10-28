//go:build integration

package containers

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
)

var templateOtherHex = strings.Repeat("cd", 32)

// templateBuild is a small stand-in for the migration chain: a table with a
// row, a sequence and an extension, so every kind of state the fingerprint
// covers exists in the template.
func templateBuild(ctx context.Context, uri string) error {
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, statement := range []string{
		"CREATE EXTENSION IF NOT EXISTS pg_trgm",
		"CREATE SEQUENCE public.tpl_seq",
		"CREATE TABLE public.tpl_t (id int PRIMARY KEY, note text)",
		"INSERT INTO public.tpl_t VALUES (1, 'seed')",
		"CREATE VIEW public.tpl_v AS SELECT id, note FROM public.tpl_t",
		"SELECT lo_from_bytea(424242, 'seed'::bytea)",
		"SELECT nextval('public.tpl_seq')",
	} {
		if _, err := conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("%s: %w", statement, err)
		}
	}
	return nil
}

// mustServers is the shared server set of the running shared-mode body. A nil
// here means shared mode is not active (or the PostgreSQL server never
// started): fail with a name instead of a nil dereference that would kill the
// test binary and hide every later test.
func mustServers(t *testing.T) *sharedServers {
	t.Helper()
	servers := activeSharedServers()
	if servers == nil || servers.pg == nil {
		t.Fatal("shared mode is not active or the shared PostgreSQL server did not start")
	}
	return servers
}

func mustTemplateClone(t *testing.T, ctx context.Context, key string, build func(context.Context, string) error) *Instance {
	t.Helper()
	clone, err := StartPostgresFromTemplate(ctx, key, build)
	if err != nil {
		t.Fatal(err)
	}
	if clone == nil {
		t.Fatal("StartPostgresFromTemplate returned no instance")
	}
	return clone
}

// requireFingerprint fails unless rows holds the state templateBuild made. An
// empty fingerprint on both sides of a comparison would make it pass for the
// wrong reason.
func requireFingerprint(t *testing.T, rows []string) {
	t.Helper()
	for _, want := range []string{
		"relation public.tpl_t kind=r",
		"rows public.tpl_t=1",
		"sequence public.tpl_seq last=1 called=true",
		"extension pg_trgm ",
		"view-def public.tpl_v ",
		"large-objects n=1 ",
	} {
		found := false
		for _, row := range rows {
			if strings.Contains(row, want) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("the fingerprint lacks a row with %q (%d rows)", want, len(rows))
		}
	}
}

func templateName(t *testing.T, key string) string {
	t.Helper()
	name, err := templateDatabaseName(key)
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func dsnFor(t *testing.T, servers *sharedServers, database string) string {
	t.Helper()
	dsn, err := withPostgresDatabase(servers.pg.adminDSN, database)
	if err != nil {
		t.Fatal(err)
	}
	return dsn
}

// templateRun runs a shared-mode binary that makes one clone from the key's
// template, calls plant, closes the clone, and returns the exit code and the log.
func templateRun(t *testing.T, key string, plant func(ctx context.Context, servers *sharedServers, clone *Instance)) (int, string) {
	t.Helper()
	ctx := sharedCtx(t)
	log := captureSharedLog(t)
	captureTemplateLog(t)
	ran := false
	code := RunWithSharedServers(func() int {
		clone := mustTemplateClone(t, ctx, key, templateBuild)
		plant(ctx, mustServers(t), clone)
		mustClose(t, ctx, clone)
		ran = true
		return 0
	})
	if !ran {
		t.Fatalf("the shared run did not complete; log:\n%s", log)
	}
	return code, log.String()
}

func referenceFingerprint(t *testing.T, ctx context.Context) []string {
	t.Helper()
	plain := mustStart(t, StartPostgres, ctx)
	defer mustClose(t, ctx, plain)
	if err := templateBuild(ctx, plain.URI); err != nil {
		t.Fatal(err)
	}
	rows, err := pgTemplateFingerprint(ctx, plain.URI)
	if err != nil {
		t.Fatal(err)
	}
	requireFingerprint(t, rows)
	return rows
}

func TestTemplateCloneEqualsAFreshBuild(t *testing.T) {
	ctx := sharedCtx(t)
	runShared(t, func() {
		captureTemplateLog(t)
		want := referenceFingerprint(t, ctx)
		clone := mustTemplateClone(t, ctx, templateFixtureHex, templateBuild)
		defer mustClose(t, ctx, clone)
		got, err := pgTemplateFingerprint(ctx, clone.URI)
		if err != nil {
			t.Fatal(err)
		}
		requireFingerprint(t, got)
		if diff := diffSnapshots(want, got); len(diff) != 0 {
			t.Errorf("clone differs from a fresh build: %v", diff)
		}
	})
}

func TestTemplateHasNoClientSessionAndNoConnections(t *testing.T) {
	ctx := sharedCtx(t)
	runShared(t, func() {
		captureTemplateLog(t)
		name := templateName(t, templateFixtureHex)
		clones := make([]*Instance, 0, 3)
		for range 3 {
			clone := mustTemplateClone(t, ctx, templateFixtureHex, templateBuild)
			clones = append(clones, clone)
		}
		admin := mustServers(t).pg.adminDSN
		conn, err := pgx.Connect(ctx, admin)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close(context.Background()) }()
		var isTemplate, allowConn bool
		if err := conn.QueryRow(ctx, "SELECT datistemplate, datallowconn FROM pg_database WHERE datname = $1", name).Scan(&isTemplate, &allowConn); err != nil {
			t.Fatal(err)
		}
		if !isTemplate || allowConn {
			t.Errorf("template flags: istemplate=%v allowconn=%v, want true false", isTemplate, allowConn)
		}
		rows, err := conn.Query(ctx, "SELECT backend_type FROM pg_stat_activity WHERE datname = $1", name)
		if err != nil {
			t.Fatal(err)
		}
		types, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		if got := clientSessionCount(types); got != 0 {
			t.Errorf("%d client sessions on the template", got)
		}
		if _, err := pgx.Connect(ctx, dsnFor(t, mustServers(t), name)); err == nil {
			t.Error("a connection to the template succeeded")
		}
		for _, clone := range clones {
			mustClose(t, ctx, clone)
		}
	})
}

func TestTemplateBuildLeavingASessionOpenIsRefused(t *testing.T) {
	ctx := sharedCtx(t)
	var leaked []*pgx.Conn
	t.Cleanup(func() {
		for _, conn := range leaked {
			_ = conn.Close(context.Background())
		}
	})
	runShared(t, func() {
		captureTemplateLog(t)
		_, err := StartPostgresFromTemplate(ctx, templateFixtureHex, func(ctx context.Context, uri string) error {
			conn, err := pgx.Connect(ctx, uri)
			if err != nil {
				return err
			}
			leaked = append(leaked, conn)
			return templateBuild(ctx, uri)
		})
		if err == nil || !strings.Contains(err.Error(), "client session") {
			t.Errorf("want an error naming a client session on the template, got %v", err)
		}
	})
}

func TestTemplateParallelClonesUnderRace(t *testing.T) {
	ctx := sharedCtx(t)
	runShared(t, func() {
		captureTemplateLog(t)
		want := referenceFingerprint(t, ctx)
		var mu sync.Mutex
		seen := map[string]bool{}
		t.Run("clones", func(t *testing.T) {
			for i := range 16 {
				t.Run(fmt.Sprint(i), func(t *testing.T) {
					t.Parallel()
					clone := mustTemplateClone(t, ctx, templateFixtureHex, templateBuild)
					defer mustClose(t, ctx, clone)
					mu.Lock()
					if seen[clone.URI] {
						t.Errorf("two clones share %s", clone.URI)
					}
					seen[clone.URI] = true
					mu.Unlock()
					got, err := pgTemplateFingerprint(ctx, clone.URI)
					if err != nil {
						t.Fatal(err)
					}
					requireFingerprint(t, got)
					if diff := diffSnapshots(want, got); len(diff) != 0 {
						t.Errorf("clone differs: %v", diff)
					}
				})
			}
		})
		if len(seen) != 16 {
			t.Errorf("%d distinct clones, want 16", len(seen))
		}
	})
}

func TestNewKeyMakesNewTemplateAndKeepsTheOld(t *testing.T) {
	ctx := sharedCtx(t)
	var builds atomic.Int32
	counting := func(ctx context.Context, uri string) error { builds.Add(1); return templateBuild(ctx, uri) }
	log := captureSharedLog(t)
	captureTemplateLog(t)
	code := RunWithSharedServers(func() int {
		var clones []*Instance
		for _, key := range []string{templateFixtureHex, templateOtherHex, templateFixtureHex} {
			clone := mustTemplateClone(t, ctx, key, counting)
			clones = append(clones, clone)
		}
		conn, err := pgx.Connect(ctx, mustServers(t).pg.adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close(context.Background()) }()
		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_database WHERE datname LIKE 'dh_tpl_%' AND datistemplate").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Errorf("%d template databases, want 2 (one per key)", count)
		}
		for _, clone := range clones {
			mustClose(t, ctx, clone)
		}
		return 0
	})
	if code != 0 {
		t.Errorf("exit %d, want 0; log:\n%s", code, log)
	}
	if got := builds.Load(); got != 2 {
		t.Errorf("builds = %d, want 2", got)
	}
}

func TestTemplateIsNotReportedAsANewDatabase(t *testing.T) {
	code, log := templateRun(t, templateFixtureHex, func(context.Context, *sharedServers, *Instance) {})
	if code != 0 || strings.Contains(log, "FAIL") {
		t.Errorf("a clean template run exited %d with log:\n%s", code, log)
	}
}

// writeIntoTemplate is the planted fault: open the template, change it, close it again.
func writeIntoTemplate(t *testing.T, ctx context.Context, servers *sharedServers, name string, statements ...string) {
	t.Helper()
	pgExec(t, ctx, servers.pg.adminDSN, "ALTER DATABASE "+name+" ALLOW_CONNECTIONS true")
	pgExec(t, ctx, dsnFor(t, servers, name), statements...)
	pgExec(t, ctx, servers.pg.adminDSN, "ALTER DATABASE "+name+" ALLOW_CONNECTIONS false")
}

func TestTemplateChangeIsAFailLine(t *testing.T) {
	name := templateName(t, templateFixtureHex)
	cases := []struct {
		name  string
		plant func(t *testing.T, ctx context.Context, s *sharedServers)
		want  string
	}{
		{"row inserted", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "INSERT INTO public.tpl_t VALUES (2, 'planted')")
		}, "rows public.tpl_t=2"},
		{"table added", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "CREATE TABLE public.planted (id int)")
		}, "planted"},
		{"column added", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "ALTER TABLE public.tpl_t ADD COLUMN extra int")
		}, "extra"},
		{"function added", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "CREATE FUNCTION public.planted_fn() RETURNS int LANGUAGE sql AS 'SELECT 1'")
		}, "planted_fn"},
		{"sequence advanced", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "SELECT nextval('public.tpl_seq')")
		}, "sequence public.tpl_seq"},
		{"extension added", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "CREATE EXTENSION citext")
		}, "extension citext"},
		{"row updated, same count", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "UPDATE public.tpl_t SET note = 'changed' WHERE id = 1")
		}, "content public.tpl_t"},
		{"table granted to PUBLIC", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "GRANT SELECT ON public.tpl_t TO PUBLIC")
		}, "relation public.tpl_t"},
		{"table owner changed", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "ALTER TABLE public.tpl_t OWNER TO pg_monitor")
		}, "relation public.tpl_t"},
		{"table reloption set", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "ALTER TABLE public.tpl_t SET (fillfactor = 70)")
		}, "relation public.tpl_t"},
		{"table comment set", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "COMMENT ON TABLE public.tpl_t IS 'planted'")
		}, "relation public.tpl_t"},
		{"column comment set", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "COMMENT ON COLUMN public.tpl_t.note IS 'planted'")
		}, "column public.tpl_t.note"},
		{"default privileges changed", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO PUBLIC")
		}, "default-acl"},
		{"schema granted", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "GRANT USAGE ON SCHEMA public TO pg_monitor")
		}, "schema public"},
		{"view replaced, same columns", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "CREATE OR REPLACE VIEW public.tpl_v AS SELECT id, note FROM public.tpl_t WHERE id > 0")
		}, "view-def public.tpl_v"},
		{"large object content changed", func(t *testing.T, ctx context.Context, s *sharedServers) {
			writeIntoTemplate(t, ctx, s, name, "SELECT lo_put(424242, 0, 'changed'::bytea)")
		}, "large-objects "},
		{"connections left allowed", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "ALTER DATABASE "+name+" ALLOW_CONNECTIONS true")
		}, "allowconn"},
		{"template flag cleared", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "ALTER DATABASE "+name+" IS_TEMPLATE false")
		}, "istemplate"},
		{"unregistered template", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "CREATE DATABASE dh_tpl_aaaaaaaaaaaaaaaaaaaaaaaa")
		}, "dh_tpl_aaaaaaaaaaaaaaaaaaaaaaaa"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, log := templateRun(t, templateFixtureHex, func(ctx context.Context, s *sharedServers, _ *Instance) { tc.plant(t, ctx, s) })
			if code != 1 {
				t.Errorf("exit %d, want 1; log:\n%s", code, log)
			}
			if !strings.Contains(log, "FAIL: shared server leak: postgres template") || !strings.Contains(log, tc.want) {
				t.Errorf("log lacks a template FAIL line naming %q:\n%s", tc.want, log)
			}
		})
	}
}

func TestTemplateOffCreatesNoTemplate(t *testing.T) {
	ctx := sharedCtx(t)
	t.Setenv(PostgresTemplateEnv, "off")
	var builds atomic.Int32
	counting := func(ctx context.Context, uri string) error { builds.Add(1); return templateBuild(ctx, uri) }
	runShared(t, func() {
		captureTemplateLog(t)
		for range 2 {
			clone := mustTemplateClone(t, ctx, templateFixtureHex, counting)
			defer mustClose(t, ctx, clone)
		}
		conn, err := pgx.Connect(ctx, mustServers(t).pg.adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close(context.Background()) }()
		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_database WHERE datname LIKE 'dh_tpl_%'").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Errorf("%d template databases with %s=off", count, PostgresTemplateEnv)
		}
	})
	if got := builds.Load(); got != 2 {
		t.Errorf("builds = %d, want 2 (the chain per call)", got)
	}
}

// A server this binary did not start gets no template.
func TestTemplateIsNotBuiltOnAServerThisBinaryDidNotStart(t *testing.T) {
	ctx := sharedCtx(t)
	var builds atomic.Int32
	counting := func(ctx context.Context, uri string) error { builds.Add(1); return templateBuild(ctx, uri) }
	runShared(t, func() {
		log := captureTemplateLog(t)
		first := mustStart(t, StartPostgres, ctx)
		defer mustClose(t, ctx, first)
		mustServers(t).pg.startedHere = false
		clone := mustTemplateClone(t, ctx, templateFixtureHex, counting)
		defer mustClose(t, ctx, clone)
		if want := "containers: postgres template fallback: server not started by this binary"; !strings.Contains(log.String(), want) {
			t.Errorf("log %q lacks %q", log, want)
		}
		if names := mustServers(t).templates.names(); len(names) != 0 {
			t.Errorf("templates registered on a server not started here: %v", names)
		}
		mustServers(t).pg.startedHere = true
	})
	if got := builds.Load(); got != 1 {
		t.Errorf("builds = %d, want 1 (the chain on the clone's own database)", got)
	}
}

// The session state the key reads is what a build role sees: a role default for
// search_path changes it. This test has its own server, so the server-wide role
// change stays out of a shared-server run.
func TestSessionStateSeesARoleDefaultSearchPath(t *testing.T) {
	ctx := sharedCtx(t)
	server, err := StartPostgresIsolated(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer mustClose(t, ctx, server)
	pgExec(t, ctx, server.URI, "CREATE ROLE state_probe LOGIN PASSWORD 'probe'")
	roleDSN := rewriteUser(t, server.URI, "state_probe", "probe")

	user, before, err := postgresSessionState(ctx, roleDSN)
	if err != nil {
		t.Fatal(err)
	}
	if user != "state_probe" || before == "" {
		t.Fatalf("session state = (%q, %q), want the role and a search_path", user, before)
	}
	pgExec(t, ctx, server.URI, "ALTER ROLE state_probe SET search_path = state_probe_schema")
	_, after, err := postgresSessionState(ctx, roleDSN)
	if err != nil {
		t.Fatal(err)
	}
	if after == before || !strings.Contains(after, "state_probe_schema") {
		t.Errorf("search_path before %q, after the role default %q: want a change that names the new path", before, after)
	}
}

func rewriteUser(t *testing.T, uri, user, password string) string {
	t.Helper()
	u, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(user, password)
	return u.String()
}
