//go:build integration

package containers

import (
	"context"
	"fmt"
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
		"SELECT nextval('public.tpl_seq')",
	} {
		if _, err := conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("%s: %w", statement, err)
		}
	}
	return nil
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
		clone, err := StartPostgresFromTemplate(ctx, key, templateBuild)
		if err != nil {
			t.Fatal(err)
		}
		plant(ctx, activeSharedServers(), clone)
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
	return rows
}

func TestTemplateCloneEqualsAFreshBuild(t *testing.T) {
	ctx := sharedCtx(t)
	runShared(t, func() {
		captureTemplateLog(t)
		want := referenceFingerprint(t, ctx)
		clone, err := StartPostgresFromTemplate(ctx, templateFixtureHex, templateBuild)
		if err != nil {
			t.Fatal(err)
		}
		defer mustClose(t, ctx, clone)
		got, err := pgTemplateFingerprint(ctx, clone.URI)
		if err != nil {
			t.Fatal(err)
		}
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
			clone, err := StartPostgresFromTemplate(ctx, templateFixtureHex, templateBuild)
			if err != nil {
				t.Fatal(err)
			}
			clones = append(clones, clone)
		}
		admin := activeSharedServers().pg.adminDSN
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
		if _, err := pgx.Connect(ctx, dsnFor(t, activeSharedServers(), name)); err == nil {
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
					clone, err := StartPostgresFromTemplate(ctx, templateFixtureHex, templateBuild)
					if err != nil {
						t.Fatal(err)
					}
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
			clone, err := StartPostgresFromTemplate(ctx, key, counting)
			if err != nil {
				t.Fatal(err)
			}
			clones = append(clones, clone)
		}
		conn, err := pgx.Connect(ctx, activeSharedServers().pg.adminDSN)
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
			clone, err := StartPostgresFromTemplate(ctx, templateFixtureHex, counting)
			if err != nil {
				t.Fatal(err)
			}
			defer mustClose(t, ctx, clone)
		}
		conn, err := pgx.Connect(ctx, activeSharedServers().pg.adminDSN)
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
		activeSharedServers().pg.startedHere = false
		clone, err := StartPostgresFromTemplate(ctx, templateFixtureHex, counting)
		if err != nil {
			t.Fatal(err)
		}
		defer mustClose(t, ctx, clone)
		if want := "containers: postgres template fallback: server not started by this binary"; !strings.Contains(log.String(), want) {
			t.Errorf("log %q lacks %q", log, want)
		}
		if names := activeSharedServers().templates.names(); len(names) != 0 {
			t.Errorf("templates registered on a server not started here: %v", names)
		}
		activeSharedServers().pg.startedHere = true
	})
	if got := builds.Load(); got != 1 {
		t.Errorf("builds = %d, want 1 (the chain on the clone's own database)", got)
	}
}
