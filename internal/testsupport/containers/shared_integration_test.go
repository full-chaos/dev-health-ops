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
		{"setting with sighup context", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "ALTER SYSTEM SET autovacuum_naptime = '61s'", "SELECT pg_reload_conf()")
			// The reload is asynchronous; wait until the server reports the new value.
			deadline := time.Now().Add(20 * time.Second)
			for {
				conn, err := pgx.Connect(ctx, s.pg.adminDSN)
				if err != nil {
					t.Fatal(err)
				}
				var value string
				err = conn.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name = 'autovacuum_naptime'").Scan(&value)
				_ = conn.Close(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if value == "61" || time.Now().After(deadline) {
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
		}, "postgres new setting autovacuum_naptime=61"},
		{"parameter privilege", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "GRANT SET ON PARAMETER work_mem TO PUBLIC")
		}, "postgres new parameter-acl work_mem"},
		{"replication slot", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "SELECT pg_create_physical_replication_slot('planted_slot')")
		}, "postgres new replication-slot planted_slot"},
		{"role setting", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "ALTER ROLE worker_test SET work_mem = '5MB'")
		}, "postgres new db-role-setting */worker_test work_mem=5MB"},
		{"relation in the admin database", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "CREATE TABLE planted_admin (id int)")
		}, "postgres new admin-db-relation public.planted_admin"},
		{"clickhouse role", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN, "CREATE ROLE planted_lone_role")
		}, "clickhouse new role planted_lone_role"},
		{"clickhouse quota", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN, "CREATE QUOTA planted_quota FOR INTERVAL 1 hour MAX queries = 1000")
		}, "clickhouse new quota planted_quota"},
		{"clickhouse row policy", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN, "CREATE ROW POLICY planted_policy ON system.one USING 1")
		}, "clickhouse new row-policy planted_policy"},
		{"clickhouse settings profile", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN, "CREATE SETTINGS PROFILE planted_profile SETTINGS max_threads = 2")
		}, "clickhouse new settings-profile planted_profile"},
		{"clickhouse table in a shared database", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN, "CREATE TABLE default.planted_table (id UInt8) ENGINE = Memory")
		}, "clickhouse new shared-db-table default.planted_table"},
		{"clickhouse dictionary", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN, "CREATE DICTIONARY default.planted_dict (id UInt64, v String) PRIMARY KEY id "+
				"SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)")
		}, "clickhouse new dictionary default.planted_dict"},
		{"subscription", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "CREATE SUBSCRIPTION planted_sub CONNECTION 'host=127.0.0.1 dbname=none' "+
				"PUBLICATION planted_pub WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE)")
		}, "postgres new subscription planted_sub"},
		{"replication origin", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "SELECT pg_replication_origin_create('planted_origin')")
		}, "postgres new replication-origin planted_origin"},
		{"comment on a shared object", func(t *testing.T, ctx context.Context, s *sharedServers) {
			pgExec(t, ctx, s.pg.adminDSN, "COMMENT ON ROLE worker_test IS 'planted'")
		}, "postgres new shared-comment pg_authid "},
		{"clickhouse workload", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN, "CREATE WORKLOAD planted_workload")
		}, "clickhouse new workload planted_workload"},
		{"clickhouse resource", func(t *testing.T, ctx context.Context, s *sharedServers) {
			chExec(t, ctx, s.ch.adminDSN, "CREATE RESOURCE planted_resource (WRITE DISK default, READ DISK default)")
		}, "clickhouse new resource planted_resource"},
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

// TestSharedServerFunctionCrossesDatabases shows why SQL user-defined
// functions are in the leak check: one made through one caller's scratch
// database is callable from another caller's, and the run exits 1 naming it.
func TestSharedServerFunctionCrossesDatabases(t *testing.T) {
	ctx := sharedCtx(t)
	log := captureSharedLog(t)
	ran := false
	code := RunWithSharedServers(func() int {
		first := mustStart(t, StartClickHouse, ctx)
		second := mustStart(t, StartClickHouse, ctx)
		chExec(t, ctx, first.URI, "CREATE FUNCTION crossing_udf AS (x) -> x + 41")
		options, err := clickhousego.ParseDSN(second.URI)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := clickhousego.Open(options)
		if err != nil {
			t.Fatal(err)
		}
		var answer uint64
		if err := conn.QueryRow(ctx, "SELECT toUInt64(crossing_udf(1))").Scan(&answer); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		if answer != 42 {
			t.Errorf("crossing_udf(1) from another scratch database = %d, want 42", answer)
		}
		mustClose(t, ctx, first)
		mustClose(t, ctx, second)
		ran = true
		return 0
	})
	if !ran || code != 1 || !strings.Contains(log.String(), "FAIL: shared server leak: clickhouse new function crossing_udf") {
		t.Errorf("ran %v code %d; want 1 and the function reported; log:\n%s", ran, code, log)
	}
}

// TestSharedServerUserCannotCreateNamedCollections: the harness's ClickHouse
// user holds no CREATE NAMED COLLECTION grant, so no test can make one on the
// shared server; the leak snapshot reads system.named_collections all the same.
func TestSharedServerUserCannotCreateNamedCollections(t *testing.T) {
	ctx := sharedCtx(t)
	runShared(t, func() {
		instance := mustStart(t, StartClickHouse, ctx)
		defer mustClose(t, ctx, instance)
		options, err := clickhousego.ParseDSN(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := clickhousego.Open(options)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		err = conn.Exec(ctx, "CREATE NAMED COLLECTION refused_nc AS host = 'planted'")
		if err == nil || !strings.Contains(err.Error(), "Not enough privileges") {
			t.Errorf("CREATE NAMED COLLECTION as the harness user: %v; want a privilege refusal", err)
		}
	})
}

// TestSharedServerRefusesKindsTheImageCannotCreate: kinds the snapshot reads
// although no test can create one on the pinned images.
func TestSharedServerRefusesKindsTheImageCannotCreate(t *testing.T) {
	ctx := sharedCtx(t)
	runShared(t, func() {
		pg := mustStart(t, StartPostgres, ctx)
		ch := mustStart(t, StartClickHouse, ctx)
		defer mustClose(t, ctx, pg)
		defer mustClose(t, ctx, ch)
		servers := activeSharedServers()
		conn, err := pgx.Connect(ctx, servers.pg.adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close(context.Background()) }()
		var pgErr *pgconn.PgError
		if _, err := conn.Exec(ctx, "SECURITY LABEL ON ROLE worker_test IS 'planted'"); !errors.As(err, &pgErr) || pgErr.Code != "22023" {
			t.Errorf("SECURITY LABEL on the PostgreSQL image: %v; want SQLSTATE 22023 (no label provider loaded)", err)
		}
		if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "PREPARE TRANSACTION 'planted'"); !errors.As(err, &pgErr) || pgErr.Code != "55000" {
			t.Errorf("PREPARE TRANSACTION on the PostgreSQL image: %v; want SQLSTATE 55000 (max_prepared_transactions = 0)", err)
		}
		_, _ = conn.Exec(ctx, "ROLLBACK")
		options, err := clickhousego.ParseDSN(servers.ch.adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		chConn, err := clickhousego.Open(options)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = chConn.Close() }()
		err = chConn.Exec(ctx, "CREATE MASKING POLICY planted_mp ON system.one UPDATE dummy = 0 TO ALL")
		if err == nil || !strings.Contains(err.Error(), "code: 344") {
			t.Errorf("CREATE MASKING POLICY on the ClickHouse image: %v; want code 344 (Cloud only)", err)
		}
	})
}

// sharedCatalogCoverage maps every PostgreSQL shared catalog
// (pg_class WHERE relisshared AND relkind = 'r') to the snapshot text that reads
// it, or to the reason it is not read.
var sharedCatalogCoverage = map[string]struct{ read, reason string }{
	"pg_auth_members":       {read: "pg_auth_members"},
	"pg_authid":             {read: "pg_roles"},
	"pg_database":           {read: "pg_database"},
	"pg_db_role_setting":    {read: "pg_db_role_setting"},
	"pg_parameter_acl":      {read: "pg_parameter_acl"},
	"pg_replication_origin": {read: "pg_replication_origin"},
	"pg_shdescription":      {read: "pg_shdescription"},
	"pg_shseclabel":         {read: "pg_shseclabel"},
	"pg_subscription":       {read: "pg_subscription"},
	"pg_tablespace":         {read: "pg_tablespace"},
	"pg_shdepend":           {reason: "dependency rows of objects in the other shared catalogs; a change there is a change in a catalog read above"},
}

// createPrivilegeCoverage maps every ClickHouse CREATE privilege
// (system.privileges) to the snapshot text that reads the kind it creates, or
// to the reason it is not read.
var createPrivilegeCoverage = map[string]struct{ read, reason string }{
	"CREATE DATABASE":                  {read: "system.databases"},
	"CREATE TABLE":                     {read: "system.tables"},
	"CREATE VIEW":                      {read: "system.tables"},
	"CREATE DICTIONARY":                {read: "system.dictionaries"},
	"CREATE FUNCTION":                  {read: "system.functions"},
	"CREATE WORKLOAD":                  {read: "system.workloads"},
	"CREATE RESOURCE":                  {read: "system.resources"},
	"CREATE NAMED COLLECTION":          {read: "system.named_collections"},
	"CREATE USER":                      {read: "system.users"},
	"CREATE ROLE":                      {read: "system.roles"},
	"CREATE ROW POLICY":                {read: "system.row_policies"},
	"CREATE QUOTA":                     {read: "system.quotas"},
	"CREATE SETTINGS PROFILE":          {read: "system.settings_profiles"},
	"CREATE MASKING POLICY":            {read: "system.masking_policies"},
	"CREATE TEMPORARY TABLE":           {reason: "session-scoped: gone when the creating connection closes"},
	"CREATE ARBITRARY TEMPORARY TABLE": {reason: "session-scoped: gone when the creating connection closes"},
	"CREATE TEMPORARY VIEW":            {reason: "session-scoped: gone when the creating connection closes"},
}

// TestSharedServerKindEnumerationIsCovered is the class sweep: it enumerates
// the server-wide kinds from each pinned image and fails on a kind with no
// snapshot read and no reason, or on a read the snapshot does not make.
func TestSharedServerKindEnumerationIsCovered(t *testing.T) {
	ctx := sharedCtx(t)
	runShared(t, func() {
		pg := mustStart(t, StartPostgres, ctx)
		ch := mustStart(t, StartClickHouse, ctx)
		defer mustClose(t, ctx, pg)
		defer mustClose(t, ctx, ch)
		servers := activeSharedServers()
		conn, err := pgx.Connect(ctx, servers.pg.adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close(context.Background()) }()
		rows, err := conn.Query(ctx, "SELECT relname::text FROM pg_class WHERE relisshared AND relkind = 'r' ORDER BY 1")
		if err != nil {
			t.Fatal(err)
		}
		catalogs, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("PostgreSQL shared catalogs: %v", catalogs)
		checkCoverage(t, "PostgreSQL shared catalog", catalogs, sharedCatalogCoverage, strings.Join(postgresSnapshotQueries, "\n"))

		options, err := clickhousego.ParseDSN(servers.ch.adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		chConn, err := clickhousego.Open(options)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = chConn.Close() }()
		result, err := chConn.Query(ctx, "SELECT toString(privilege) FROM system.privileges WHERE toString(privilege) LIKE 'CREATE %' ORDER BY 1")
		if err != nil {
			t.Fatal(err)
		}
		var privileges []string
		for result.Next() {
			var privilege string
			if err := result.Scan(&privilege); err != nil {
				t.Fatal(err)
			}
			privileges = append(privileges, privilege)
		}
		if err := result.Err(); err != nil {
			t.Fatal(err)
		}
		t.Logf("ClickHouse CREATE privileges: %v", privileges)
		checkCoverage(t, "ClickHouse CREATE privilege", privileges, createPrivilegeCoverage, strings.Join(clickHouseSnapshotQueries, "\n"))
	})
}

func checkCoverage(t *testing.T, kind string, found []string, coverage map[string]struct{ read, reason string }, queries string) {
	t.Helper()
	if len(found) == 0 {
		t.Fatalf("no %s enumerated", kind)
	}
	seen := map[string]bool{}
	for _, name := range found {
		seen[name] = true
		entry, ok := coverage[name]
		switch {
		case !ok:
			t.Errorf("%s %s has no snapshot read and no reason", kind, name)
		case entry.read != "" && !strings.Contains(queries, entry.read):
			t.Errorf("%s %s: the snapshot does not read %s", kind, name, entry.read)
		}
	}
	for name := range coverage {
		if !seen[name] {
			t.Errorf("%s %s is in the coverage table but the image does not have it", kind, name)
		}
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
