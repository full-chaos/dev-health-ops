package containers

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5"
)

// SharedServersEnv set to "off" turns shared mode off: every StartPostgres and
// StartClickHouse call starts its own container again, as without
// RunWithSharedServers. Unset or "on" keeps shared mode.
const SharedServersEnv = "DEV_HEALTH_TEST_SHARED_SERVERS"

var sharedState atomic.Pointer[sharedServers]

// sharedLog receives the shared-mode FAIL lines. A variable so tests can read it.
var sharedLog io.Writer = os.Stderr

func activeSharedServers() *sharedServers { return sharedState.Load() }

// RunWithSharedServers runs a test binary in shared mode. Call it from
// TestMain:
//
//	os.Exit(containers.RunWithSharedServers(m.Run))
//
// The first StartPostgres call starts ONE PostgreSQL container and the first
// StartClickHouse call ONE ClickHouse container for this process. Every call
// then gets a private scratch database on that server through the remote path
// (remote.go), created on entry and dropped on Close. After run returns, the
// servers are compared with their state at start; any database, role, role
// membership, setting, user, grant or other server-wide object that is new or
// gone is a FAIL line, and the exit code is 1 even when every test passed.
func RunWithSharedServers(run func() int) (code int) {
	switch value := strings.TrimSpace(os.Getenv(SharedServersEnv)); value {
	case "off":
		return run()
	case "", "on":
	default:
		fmt.Fprintf(sharedLog, "FAIL: %s=%q: want unset, \"on\" or \"off\"\n", SharedServersEnv, value)
		return 1
	}
	servers := &sharedServers{}
	if !sharedState.CompareAndSwap(nil, servers) {
		fmt.Fprintln(sharedLog, "FAIL: RunWithSharedServers is already active in this process")
		return 1
	}
	// Deferred so a panic or runtime.Goexit in run still ends shared mode and
	// terminates the servers.
	defer func() {
		sharedState.Store(nil)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		findings, err := servers.finish(ctx)
		for _, finding := range findings {
			fmt.Fprintf(sharedLog, "FAIL: shared server leak: %s\n", finding)
		}
		if err != nil {
			fmt.Fprintf(sharedLog, "FAIL: shared server: %v\n", err)
		}
		if code == 0 && (len(findings) > 0 || err != nil) {
			code = 1
		}
	}()
	return run()
}

type sharedServers struct {
	pgOnce sync.Once
	pg     *sharedServer
	pgErr  error

	chOnce sync.Once
	ch     *sharedServer
	chErr  error
}

type sharedServer struct {
	engine    string
	container *Instance
	adminDSN  string
	httpDSN   string
	snapshot  func(ctx context.Context, adminDSN string) ([]string, error)
	baseline  []string
}

// sharedBootTimeout bounds a shared server's start. The server outlives the
// test that triggered it, so the caller's context is not used.
const sharedBootTimeout = 3 * time.Minute

func (s *sharedServers) startPostgres(ctx context.Context) (*Instance, error) {
	s.pgOnce.Do(func() {
		boot, cancel := context.WithTimeout(context.Background(), sharedBootTimeout)
		defer cancel()
		s.pg, s.pgErr = bootSharedPostgres(boot)
	})
	if s.pgErr != nil {
		return nil, fmt.Errorf("shared PostgreSQL server: %w", s.pgErr)
	}
	return startPostgresRemote(ctx, s.pg.adminDSN)
}

func (s *sharedServers) startClickHouse(ctx context.Context) (*Instance, error) {
	s.chOnce.Do(func() {
		boot, cancel := context.WithTimeout(context.Background(), sharedBootTimeout)
		defer cancel()
		s.ch, s.chErr = bootSharedClickHouse(boot)
	})
	if s.chErr != nil {
		return nil, fmt.Errorf("shared ClickHouse server: %w", s.chErr)
	}
	return startClickHouseRemote(ctx, s.ch.adminDSN, s.ch.httpDSN)
}

// finish compares every started server with its baseline and terminates it.
func (s *sharedServers) finish(ctx context.Context) ([]string, error) {
	var findings []string
	var errs []error
	for _, server := range []*sharedServer{s.pg, s.ch} {
		if server == nil {
			continue
		}
		after, err := server.snapshot(ctx, server.adminDSN)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: read the server state after the run: %w", server.engine, err))
		} else {
			for _, finding := range diffSnapshots(server.baseline, after) {
				findings = append(findings, server.engine+" "+finding)
			}
		}
		if err := server.container.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: terminate: %w", server.engine, err))
		}
	}
	return findings, errors.Join(errs...)
}

// diffSnapshots names every row that is new in after or gone from before.
func diffSnapshots(before, after []string) []string {
	seen := make(map[string]int, len(before))
	for _, row := range before {
		seen[row]++
	}
	var findings []string
	for _, row := range after {
		if seen[row] > 0 {
			seen[row]--
			continue
		}
		findings = append(findings, "new "+row)
	}
	for row, count := range seen {
		for ; count > 0; count-- {
			findings = append(findings, "gone "+row)
		}
	}
	sort.Strings(findings)
	return findings
}

// testParallel is the -test.parallel value (GOMAXPROCS when unset). Flags are
// parsed by the time a test calls StartPostgres.
func testParallel() int {
	if found := flag.Lookup("test.parallel"); found != nil {
		if value, err := strconv.Atoi(found.Value.String()); err == nil && value > 0 {
			return value
		}
	}
	return runtime.GOMAXPROCS(0)
}

// pgxDefaultPoolSize is pgxpool's default MaxConns: max(4, NumCPU).
func pgxDefaultPoolSize() int {
	return max(4, runtime.NumCPU())
}

// requiredPostgresConnections sizes the shared server: every running parallel
// test may hold two full default pools at once, plus fixed headroom for the
// admin, schema and cleanup connections.
func requiredPostgresConnections(parallel, poolSize int) int {
	return parallel*poolSize*2 + 20
}

// sharedPostgresMaxConnections is a variable so a test can boot a small
// server and prove a full server refuses loudly.
var sharedPostgresMaxConnections = func() int {
	return requiredPostgresConnections(testParallel(), pgxDefaultPoolSize())
}

func bootSharedPostgres(ctx context.Context) (*sharedServer, error) {
	want := sharedPostgresMaxConnections()
	container, err := startPostgresContainer(ctx, []string{"postgres", "-c", "max_connections=" + strconv.Itoa(want)})
	if err != nil {
		return nil, err
	}
	container.engine, container.mode = "postgres", "shared-server"
	server := &sharedServer{engine: "postgres", container: container, adminDSN: container.URI, snapshot: postgresSnapshot}
	if err := checkPostgresCapacity(ctx, server.adminDSN, want); err != nil {
		_ = container.Close(ctx)
		return nil, err
	}
	if server.baseline, err = postgresSnapshot(ctx, server.adminDSN); err != nil {
		_ = container.Close(ctx)
		return nil, fmt.Errorf("read the baseline: %w", err)
	}
	return server, nil
}

func checkPostgresCapacity(ctx context.Context, dsn string, want int) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var text string
	if err := conn.QueryRow(ctx, "SHOW max_connections").Scan(&text); err != nil {
		return err
	}
	got, err := strconv.Atoi(text)
	if err != nil {
		return fmt.Errorf("max_connections %q: %w", text, err)
	}
	if got < want {
		return fmt.Errorf("max_connections is %d, want at least %d (-test.parallel %d x pool size %d x 2 + 20)",
			got, want, testParallel(), pgxDefaultPoolSize())
	}
	return nil
}

func bootSharedClickHouse(ctx context.Context) (*sharedServer, error) {
	container, err := startClickHouseContainer(ctx)
	if err != nil {
		return nil, err
	}
	container.engine, container.mode = "clickhouse", "shared-server"
	httpDSN, err := ClickHouseHTTPDSN(ctx, container)
	if err != nil {
		_ = container.Close(ctx)
		return nil, err
	}
	server := &sharedServer{
		engine:    "clickhouse",
		container: container,
		adminDSN:  container.URI,
		httpDSN:   httpDSN,
		snapshot:  clickHouseSnapshot,
	}
	if server.baseline, err = clickHouseSnapshot(ctx, server.adminDSN); err != nil {
		_ = container.Close(ctx)
		return nil, fmt.Errorf("read the baseline: %w", err)
	}
	return server, nil
}

// postgresSnapshotQueries read every cluster-wide object a test could leave
// behind on a shared server. Each returns one text column.
var postgresSnapshotQueries = []string{
	`SELECT 'database ' || datname FROM pg_database`,
	`SELECT 'role ' || rolname FROM pg_roles`,
	`SELECT 'membership ' || m.rolname || ' in ' || r.rolname
	   FROM pg_auth_members a JOIN pg_roles m ON m.oid = a.member JOIN pg_roles r ON r.oid = a.roleid`,
	`SELECT 'alter-system ' || name || '=' || coalesce(setting, '') FROM pg_file_settings
	  WHERE sourcefile LIKE '%postgresql.auto.conf'`,
	`SELECT 'db-role-setting ' || coalesce(d.datname, '*') || '/' || coalesce(r.rolname, '*') || ' ' ||
	        array_to_string(s.setconfig, ',')
	   FROM pg_db_role_setting s
	   LEFT JOIN pg_database d ON d.oid = s.setdatabase
	   LEFT JOIN pg_roles r ON r.oid = s.setrole`,
	`SELECT 'tablespace ' || spcname FROM pg_tablespace`,
	// Server settings a reload or restart applies to every database.
	`SELECT 'setting ' || name || '=' || setting FROM pg_settings WHERE context IN ('postmaster', 'sighup')`,
	`SELECT 'parameter-acl ' || parname || ' ' || coalesce(array_to_string(paracl, ','), '') FROM pg_parameter_acl`,
	`SELECT 'replication-slot ' || slot_name FROM pg_replication_slots`,
	`SELECT 'prepared-xact ' || gid FROM pg_prepared_xacts`,
	// The admin database the shared server boots with; callers get scratch databases.
	`SELECT 'admin-db-relation ' || n.nspname || '.' || c.relname FROM pg_class c
	   JOIN pg_namespace n ON n.oid = c.relnamespace
	  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')`,
}

func postgresSnapshot(ctx context.Context, dsn string) ([]string, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var rows []string
	for _, query := range postgresSnapshotQueries {
		result, err := conn.Query(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", query, err)
		}
		values, err := pgx.CollectRows(result, pgx.RowTo[string])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", query, err)
		}
		rows = append(rows, values...)
	}
	sort.Strings(rows)
	return rows, nil
}

// clickHouseSnapshotQueries are the ClickHouse counterpart: databases, every
// access entity, grant and role grant, named collections and SQL user-defined
// functions (the last two live outside every database).
var clickHouseSnapshotQueries = []string{
	`SELECT concat('database ', name) FROM system.databases`,
	`SELECT concat('user ', name) FROM system.users`,
	`SELECT concat('role ', name) FROM system.roles`,
	`SELECT concat('settings-profile ', name) FROM system.settings_profiles`,
	`SELECT concat('quota ', name) FROM system.quotas`,
	`SELECT concat('row-policy ', name) FROM system.row_policies`,
	`SELECT concat('grant ', ifNull(user_name, ''), '|', ifNull(role_name, ''), '|', toString(access_type), '|',
	        ifNull(database, ''), '|', ifNull(table, ''), '|', ifNull(column, ''), '|',
	        toString(is_partial_revoke), '|', toString(grant_option)) FROM system.grants`,
	`SELECT concat('role-grant ', ifNull(user_name, ''), '|', ifNull(role_name, ''), '|', granted_role_name, '|',
	        toString(with_admin_option)) FROM system.role_grants`,
	`SELECT concat('named-collection ', name) FROM system.named_collections`,
	`SELECT concat('function ', name) FROM system.functions WHERE origin = 'SQLUserDefined'`,
	// Objects in databases no caller owns: the boot database and default.
	`SELECT concat('shared-db-table ', database, '.', name) FROM system.tables
	  WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')`,
	`SELECT concat('dictionary ', database, '.', name) FROM system.dictionaries`,
}

func clickHouseSnapshot(ctx context.Context, dsn string) ([]string, error) {
	options, err := clickhousego.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	conn, err := clickhousego.Open(options)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	var rows []string
	for _, query := range clickHouseSnapshotQueries {
		result, err := conn.Query(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", query, err)
		}
		for result.Next() {
			var value string
			if err := result.Scan(&value); err != nil {
				_ = result.Close()
				return nil, fmt.Errorf("%s: %w", query, err)
			}
			rows = append(rows, value)
		}
		if err := result.Err(); err != nil {
			_ = result.Close()
			return nil, fmt.Errorf("%s: %w", query, err)
		}
		_ = result.Close()
	}
	sort.Strings(rows)
	return rows, nil
}
