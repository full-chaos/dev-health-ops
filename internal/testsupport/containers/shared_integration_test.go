//go:build integration

package containers

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/testcontainers/testcontainers-go"
)

// countDockerCalls wraps the real Docker entry point and counts container starts.
func countDockerCalls(t *testing.T) func() int32 {
	t.Helper()
	var calls atomic.Int32
	original := newGenericContainer
	newGenericContainer = func(
		ctx context.Context,
		request testcontainers.GenericContainerRequest,
	) (testcontainers.Container, error) {
		calls.Add(1)
		return original(ctx, request)
	}
	t.Cleanup(func() { newGenericContainer = original })
	return calls.Load
}

func sharedCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func mustStart(t *testing.T, start func(context.Context) (*Instance, error), ctx context.Context) *Instance {
	t.Helper()
	instance, err := start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return instance
}

func mustClose(t *testing.T, ctx context.Context, instance *Instance) {
	t.Helper()
	if err := instance.Close(ctx); err != nil {
		t.Errorf("close: %v", err)
	}
}

func pgExec(t *testing.T, ctx context.Context, dsn string, statements ...string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func chExec(t *testing.T, ctx context.Context, dsn string, statements ...string) {
	t.Helper()
	options, err := clickhousego.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhousego.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, statement := range statements {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func TestSharedServerOneContainerPerEngineManyDatabases(t *testing.T) {
	ctx := sharedCtx(t)
	calls := countDockerCalls(t)

	runShared(t, func() {
		var pgs, chs []*Instance
		names := map[string]bool{}
		for range 3 {
			pg := mustStart(t, StartPostgres, ctx)
			ch := mustStart(t, StartClickHouse, ctx)
			pgs, chs = append(pgs, pg), append(chs, ch)
			name, err := DatabaseName(pg.URI)
			if err != nil {
				t.Fatal(err)
			}
			names[name] = true
		}
		if got := calls(); got != 2 {
			t.Errorf("Docker was asked %d times for 3+3 calls; want 2", got)
		}
		if len(names) != 3 {
			t.Errorf("databases %v; want 3 distinct", names)
		}
		if _, err := ClickHouseHTTPDSN(ctx, chs[0]); err != nil {
			t.Errorf("ClickHouseHTTPDSN on a shared instance: %v", err)
		}

		pgExec(t, ctx, pgs[0].URI, "CREATE TABLE only_here (id int)")
		var visible bool
		conn, err := pgx.Connect(ctx, pgs[1].URI)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, "SELECT to_regclass('public.only_here') IS NOT NULL").Scan(&visible); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close(ctx)
		if visible {
			t.Error("a table made in one scratch database is visible in another")
		}

		chExec(t, ctx, chs[0].URI, "CREATE TABLE only_here (id UInt8) ENGINE = Memory")
		options, _ := clickhousego.ParseDSN(chs[1].URI)
		chConn, err := clickhousego.Open(options)
		if err != nil {
			t.Fatal(err)
		}
		var exists uint8
		if err := chConn.QueryRow(ctx, "EXISTS TABLE only_here").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		_ = chConn.Close()
		if exists != 0 {
			t.Error("a ClickHouse table made in one scratch database is visible in another")
		}

		for _, instance := range append(pgs, chs...) {
			mustClose(t, ctx, instance)
		}
	})
}

// plantedRun runs one shared-mode binary that starts both servers, plants a
// fault through plant, and returns the exit code and the FAIL lines.
func plantedRun(t *testing.T, plant func(ctx context.Context, servers *sharedServers)) (int, string) {
	t.Helper()
	ctx := sharedCtx(t)
	log := captureSharedLog(t)
	ran := false
	code := RunWithSharedServers(func() int {
		pg := mustStart(t, StartPostgres, ctx)
		ch := mustStart(t, StartClickHouse, ctx)
		plant(ctx, activeSharedServers())
		mustClose(t, ctx, pg)
		mustClose(t, ctx, ch)
		ran = true
		return 0
	})
	if !ran {
		t.Fatalf("the shared run did not complete; log:\n%s", log)
	}
	return code, log.String()
}

// runShared runs body in shared mode and fails unless it ran to the end and
// the run exited 0 (no leak).
func runShared(t *testing.T, body func()) {
	t.Helper()
	log := captureSharedLog(t)
	ran := false
	code := RunWithSharedServers(func() int {
		body()
		ran = true
		return 0
	})
	if !ran || code != 0 {
		t.Fatalf("shared run: ran %v code %d; log:\n%s", ran, code, log)
	}
}

func TestSharedServerLeakCheck(t *testing.T) {
	cases := []struct {
		name  string
		plant func(t *testing.T, ctx context.Context, servers *sharedServers)
		want  string
	}{
		{"clean run", func(*testing.T, context.Context, *sharedServers) {}, ""},
		{"role", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "CREATE ROLE planted_role")
		}, "postgres new role planted_role"},
		{"membership of a baseline role", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "GRANT pg_monitor TO worker_test")
		}, "postgres new membership worker_test in pg_monitor"},
		{"undropped scratch database", func(t *testing.T, ctx context.Context, s *sharedServers) {
			if _, err := StartPostgres(ctx); err != nil {
				t.Fatal(err)
			}
		}, "postgres new database dh_t_"},
		{"alter system", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "ALTER SYSTEM SET work_mem = '7MB'")
		}, "postgres new alter-system work_mem=7MB"},
		{"clickhouse user", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN, "CREATE USER planted_user IDENTIFIED WITH sha256_password BY 'planted'")
		}, "clickhouse new user planted_user"},
		// The baseline ClickHouse user lives in users_xml, which is read-only:
		// no test can grant to it or revoke from it. Grants and role grants
		// can only land on SQL-created entities, so the plants use those and
		// assert the grant row itself, apart from the entity row.
		{"clickhouse grant", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN,
				"CREATE USER planted_grantee IDENTIFIED WITH sha256_password BY 'planted'",
				"GRANT SELECT ON system.one TO planted_grantee")
		}, "clickhouse new grant planted_grantee||SELECT|system|one||0|0"},
		{"clickhouse role grant", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN,
				"CREATE ROLE planted_ch_role",
				"CREATE USER planted_member IDENTIFIED WITH sha256_password BY 'planted'",
				"GRANT planted_ch_role TO planted_member")
		}, "clickhouse new role-grant planted_member||planted_ch_role|0"},
		{"clickhouse named collection", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN, "CREATE NAMED COLLECTION planted_nc AS host = 'planted'")
		}, "clickhouse new named-collection planted_nc"},
		{"clickhouse SQL user-defined function", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN, "CREATE FUNCTION planted_udf AS (x) -> x + 1")
		}, "clickhouse new function planted_udf"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, log := plantedRun(t, func(ctx context.Context, servers *sharedServers) { tc.plant(t, ctx, servers) })
			if tc.want == "" {
				if code != 0 || strings.Contains(log, "FAIL") {
					t.Errorf("clean run: code %d, log %q; want 0 and no FAIL", code, log)
				}
				return
			}
			if code != 1 {
				t.Errorf("code %d, want 1", code)
			}
			if !strings.Contains(log, "FAIL: shared server leak: "+tc.want) {
				t.Errorf("no FAIL line with %q in:\n%s", tc.want, log)
			}
		})
	}
}

// TestSharedServerNamedCollectionCrossesDatabases shows why named collections
// are in the leak check: one made through one caller's scratch database is
// visible from another caller's, and the run exits 1 naming it.
func TestSharedServerNamedCollectionCrossesDatabases(t *testing.T) {
	ctx := sharedCtx(t)
	log := captureSharedLog(t)
	ran := false
	code := RunWithSharedServers(func() int {
		first := mustStart(t, StartClickHouse, ctx)
		second := mustStart(t, StartClickHouse, ctx)
		chExec(t, ctx, first.URI, "CREATE NAMED COLLECTION crossing_nc AS host = 'planted'")
		options, err := clickhousego.ParseDSN(second.URI)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := clickhousego.Open(options)
		if err != nil {
			t.Fatal(err)
		}
		var seen uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM system.named_collections WHERE name = 'crossing_nc'").Scan(&seen); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		if seen != 1 {
			t.Errorf("named collection seen %d times from another scratch database; want 1", seen)
		}
		mustClose(t, ctx, first)
		mustClose(t, ctx, second)
		ran = true
		return 0
	})
	if !ran || code != 1 || !strings.Contains(log.String(), "FAIL: shared server leak: clickhouse new named-collection crossing_nc") {
		t.Errorf("ran %v code %d; want 1 and the named collection reported; log:\n%s", ran, code, log)
	}
}

func TestIsolatedGetsAPrivateServerInSharedMode(t *testing.T) {
	ctx := sharedCtx(t)
	calls := countDockerCalls(t)

	runShared(t, func() {
		shared := mustStart(t, StartPostgres, ctx)
		isolated := mustStart(t, StartPostgresIsolated, ctx)
		defer mustClose(t, ctx, shared)
		defer mustClose(t, ctx, isolated)
		sharedURL, _ := url.Parse(shared.URI)
		isolatedURL, _ := url.Parse(isolated.URI)
		if sharedURL.Host == isolatedURL.Host {
			t.Errorf("isolated instance %s is on the shared server", isolatedURL.Host)
		}
		if isolated.Container == nil {
			t.Error("isolated instance has no container")
		}
		if got := calls(); got != 2 {
			t.Errorf("Docker was asked %d times; want 2 (shared + isolated)", got)
		}
	})
}

func TestSharedPostgresCoversParallelPools(t *testing.T) {
	ctx := sharedCtx(t)
	runShared(t, func() {
		instance := mustStart(t, StartPostgres, ctx)
		defer mustClose(t, ctx, instance)
		var text string
		conn, err := pgx.Connect(ctx, activeSharedServers().pg.adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close(context.Background()) }()
		if err := conn.QueryRow(ctx, "SHOW max_connections").Scan(&text); err != nil {
			t.Fatal(err)
		}
		want := requiredPostgresConnections(testParallel(), pgxDefaultPoolSize())
		if text != strconv.Itoa(want) {
			t.Errorf("max_connections %s, want %d", text, want)
		}
	})
}

// TestSharedPostgresAtCapacityRefusesLoudly proves a full shared server
// refuses a new connection at once (SQLSTATE 53300), never a silent wait.
func TestSharedPostgresAtCapacityRefusesLoudly(t *testing.T) {
	ctx := sharedCtx(t)
	original := sharedPostgresMaxConnections
	sharedPostgresMaxConnections = func() int { return 8 }
	t.Cleanup(func() { sharedPostgresMaxConnections = original })

	runShared(t, func() {
		instance := mustStart(t, StartPostgres, ctx)
		defer mustClose(t, ctx, instance)
		var held []*pgx.Conn
		defer func() {
			for _, conn := range held {
				_ = conn.Close(context.Background())
			}
		}()
		for attempt := 0; attempt < 20; attempt++ {
			began := time.Now()
			conn, err := pgx.Connect(ctx, instance.URI)
			if err == nil {
				held = append(held, conn)
				continue
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "53300" {
				t.Fatalf("connection %d failed with %v; want SQLSTATE 53300 too_many_connections", attempt+1, err)
			}
			if waited := time.Since(began); waited > 10*time.Second {
				t.Errorf("refusal took %s; want an immediate error", waited)
			}
			return
		}
		t.Error("20 connections to an 8-connection server all succeeded")
	})
}
