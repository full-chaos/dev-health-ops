package containers

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
)

// errContainerStarted is returned by the recording stub so the caller unwinds
// immediately: these tests care about WHETHER Docker was asked, never about
// producing a working container.
var errContainerStarted = errors.New("container start attempted")

// recordDockerCalls replaces the package's only Docker entry point with a stub
// that counts calls, and restores it afterwards. The returned func reports how
// many times Docker was asked for a container.
func recordDockerCalls(t *testing.T) func() int {
	t.Helper()
	calls := 0
	original := newGenericContainer
	newGenericContainer = func(
		context.Context,
		testcontainers.GenericContainerRequest,
	) (testcontainers.Container, error) {
		calls++
		return nil, errContainerStarted
	}
	t.Cleanup(func() { newGenericContainer = original })
	return func() int { return calls }
}

// unreachableDSN points at a port nothing listens on, so the remote path fails
// fast at connect. That failure is expected and irrelevant: the assertion is
// about which branch was taken, not about reaching a database.
const (
	unreachablePostgresDSN   = "postgres://u:p@127.0.0.1:1/postgres?sslmode=disable"
	unreachableClickHouseDSN = "clickhouse://u:p@127.0.0.1:1/default"
)

func shortCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestStartPostgresWithRemoteDSNNeverReachesDocker is the assertion the whole
// CHAOS-4428 move rests on. A passing suite cannot demonstrate it -- only
// observing that Docker was never asked can.
func TestStartPostgresWithRemoteDSNNeverReachesDocker(t *testing.T) {
	calls := recordDockerCalls(t)
	t.Setenv(PostgresDSNEnv, unreachablePostgresDSN)

	_, err := StartPostgres(shortCtx(t))

	if got := calls(); got != 0 {
		t.Errorf("Docker was asked for a container %d times with %s set; want 0", got, PostgresDSNEnv)
	}
	// Proves the remote branch was actually entered rather than the function
	// failing early for some unrelated reason, which would make the
	// zero-Docker assertion above vacuous.
	if err == nil || !strings.Contains(err.Error(), "connect to remote PostgreSQL") {
		t.Errorf("want a remote-connect failure proving the remote path ran, got: %v", err)
	}
}

// TestStartPostgresWithoutRemoteDSNUsesDocker is the control for the test
// above. Without it, that test would still pass if StartPostgres stopped
// starting containers altogether.
func TestStartPostgresWithoutRemoteDSNUsesDocker(t *testing.T) {
	calls := recordDockerCalls(t)
	t.Setenv(PostgresDSNEnv, "")

	_, err := StartPostgres(shortCtx(t))

	if got := calls(); got != 1 {
		t.Errorf("Docker was asked for a container %d times with %s unset; want 1", got, PostgresDSNEnv)
	}
	if !errors.Is(err, errContainerStarted) {
		t.Errorf("want the recorded container-start error, got: %v", err)
	}
}

func TestStartClickHouseWithRemoteDSNNeverReachesDocker(t *testing.T) {
	calls := recordDockerCalls(t)
	t.Setenv(ClickHouseDSNEnv, unreachableClickHouseDSN)
	// Do NOT inherit a scratch prefix from the ambient environment. Prefix
	// validation runs before the remote datastore is touched, so an invalid
	// inherited prefix would make this test pass without ever reaching the
	// path it exists to prove -- the assertion below is what catches that,
	// and this line is what stops it happening in the first place.
	t.Setenv(ScratchPrefixEnv, "")

	_, err := StartClickHouse(shortCtx(t))

	if got := calls(); got != 0 {
		t.Errorf("Docker was asked for a container %d times with %s set; want 0", got, ClickHouseDSNEnv)
	}
	if err == nil {
		t.Fatal("want a failure from the unreachable remote ClickHouse, got nil")
	}
	// Prove the remote ClickHouse path was actually entered rather than the
	// call failing earlier for an unrelated reason.
	if strings.Contains(err.Error(), ScratchPrefixEnv) {
		t.Fatalf("failed during prefix validation, never reached ClickHouse: %v", err)
	}
	if !strings.Contains(err.Error(), "ClickHouse") {
		t.Errorf("want an error from the remote ClickHouse path, got: %v", err)
	}
}

// TestWithDatabasePathOverridesTheDatabaseQueryParameter is the regression test
// for the sharpest failure this design has.
//
// Both drivers give a `?database=` / `?dbname=` query parameter PRECEDENCE over
// the URL path. Rewriting only the path therefore leaves a DSN pointing at
// whatever the caller's query parameter named -- on the shared server that is
// real data -- while the scratch database sits empty and gets dropped, so the
// writes land somewhere real and nothing looks wrong.
func TestWithDatabasePathOverridesTheDatabaseQueryParameter(t *testing.T) {
	for _, key := range []string{"database", "dbname"} {
		t.Run(key, func(t *testing.T) {
			got, err := withDatabaseURL(
				"clickhouse://u:p@example:9000/default?"+key+"=dh_0830", "scratch_x",
			)
			if err != nil {
				t.Fatalf("withDatabasePath: %v", err)
			}
			if strings.Contains(got, "dh_0830") {
				t.Errorf("rewritten DSN still names the original database: %s", got)
			}
			if !strings.Contains(got, key+"=scratch_x") {
				t.Errorf("want %s pointed at the scratch database, got: %s", key, got)
			}
		})
	}
}

// TestStartClickHouseRejectsAMalformedHTTPDSNBeforeCreating pins the ordering
// that prevents a leak: the HTTP DSN is rewritten BEFORE the database is
// created, because an error after the CREATE returns without an Instance and
// so nothing holds the cleanup that would drop it.
func TestStartClickHouseRejectsAMalformedHTTPDSNBeforeCreating(t *testing.T) {
	recordDockerCalls(t)
	t.Setenv(ClickHouseDSNEnv, unreachableClickHouseDSN)
	t.Setenv(ClickHouseHTTPDSNEnv, "clickhouse://example/%zz")
	t.Setenv(ScratchPrefixEnv, "")

	_, err := StartClickHouse(shortCtx(t))

	if err == nil || !strings.Contains(err.Error(), ClickHouseHTTPDSNEnv) {
		t.Fatalf("want the malformed HTTP DSN rejected by name, got: %v", err)
	}
	// The native DSN is unreachable, so had the rewrite been attempted after
	// the CREATE we would have seen a connect/create error instead.
	if strings.Contains(err.Error(), "create scratch") {
		t.Error("HTTP DSN was validated after the database was created")
	}
}

func TestStartClickHouseWithoutRemoteDSNUsesDocker(t *testing.T) {
	calls := recordDockerCalls(t)
	t.Setenv(ClickHouseDSNEnv, "")

	_, err := StartClickHouse(shortCtx(t))

	if got := calls(); got != 1 {
		t.Errorf("Docker was asked for a container %d times with %s unset; want 1", got, ClickHouseDSNEnv)
	}
	if !errors.Is(err, errContainerStarted) {
		t.Errorf("want the recorded container-start error, got: %v", err)
	}
}

// TestStartValkeyIgnoresRemoteConfiguration pins the deliberate asymmetry
// documented in remote.go: Valkey has no CREATE DATABASE to isolate parallel
// callers with, so it stays on testcontainers even when the other two stores
// are remote. Without this test the omission reads like an oversight.
func TestStartValkeyIgnoresRemoteConfiguration(t *testing.T) {
	calls := recordDockerCalls(t)
	t.Setenv(PostgresDSNEnv, unreachablePostgresDSN)
	t.Setenv(ClickHouseDSNEnv, unreachableClickHouseDSN)

	_, err := StartValkey(shortCtx(t))

	if got := calls(); got != 1 {
		t.Errorf("Valkey asked Docker %d times while other stores were remote; want 1", got)
	}
	if !errors.Is(err, errContainerStarted) {
		t.Errorf("want the recorded container-start error, got: %v", err)
	}
}

// TestScratchLogNamesTheDatabase pins the harness's telemetry contract. The
// line must carry the database name: an orphan left by a killed run is swept
// by name, so a message that omits it would be unactionable. That this line is
// actually emitted on create and drop is shown by the executed run in the PR
// body, which cannot be proven here without a real server.
func TestScratchLogNamesTheDatabase(t *testing.T) {
	var buf bytes.Buffer
	original := scratchLog
	scratchLog = &buf
	t.Cleanup(func() { scratchLog = original })

	logScratch("created", "postgres", "lane_4428_deadbeef")

	got := buf.String()
	for _, want := range []string{"created", "postgres", "lane_4428_deadbeef"} {
		if !strings.Contains(got, want) {
			t.Errorf("scratch log %q does not carry %q", got, want)
		}
	}
}

func TestScratchNameIsUniquePerCall(t *testing.T) {
	seen := make(map[string]bool, 128)
	for range 128 {
		name, err := scratchName()
		if err != nil {
			t.Fatalf("scratchName: %v", err)
		}
		if seen[name] {
			t.Fatalf("scratchName returned a duplicate: %s", name)
		}
		seen[name] = true
	}
}

func TestScratchNameUsesConfiguredLanePrefix(t *testing.T) {
	t.Setenv(ScratchPrefixEnv, "lane_4428")

	name, err := scratchName()
	if err != nil {
		t.Fatalf("scratchName: %v", err)
	}
	if !strings.HasPrefix(name, "lane_4428_") {
		t.Errorf("want the configured lane prefix, got %s", name)
	}
}

// TestScratchPrefixRejectsUnsafeValues matters because the prefix is
// interpolated straight into a CREATE DATABASE statement.
func TestScratchPrefixRejectsUnsafeValues(t *testing.T) {
	for _, prefix := range []string{
		`a"; DROP DATABASE postgres; --`,
		"UPPER",
		"1leading_digit",
		"has-a-dash",
		"has space",
		"way_too_long_to_be_a_sensible_lane_prefix_x",
	} {
		t.Run(prefix, func(t *testing.T) {
			t.Setenv(ScratchPrefixEnv, prefix)
			if _, err := scratchName(); err == nil {
				t.Errorf("scratchName accepted unsafe prefix %q", prefix)
			}
		})
	}
}

// TestAssertSafeIdentifierRejectsAnythingButAPlainIdentifier guards the last
// step before a name reaches a DDL statement. Neither engine can parameterise
// an identifier in DDL, so interpolation is unavoidable and this is where the
// safety is actually enforced -- not inferred from how the name was built.
func TestAssertSafeIdentifierRejectsAnythingButAPlainIdentifier(t *testing.T) {
	for _, name := range []string{
		"",
		`a"; DROP DATABASE postgres; --`,
		"has`backtick",
		"has space",
		"UPPER",
		"1leading_digit",
		"has-a-dash",
	} {
		if err := assertSafeIdentifier(name); err == nil {
			t.Errorf("assertSafeIdentifier accepted unsafe name %q", name)
		}
	}
	if err := assertSafeIdentifier("lane_4428_8a2c4feb5e391ab5"); err != nil {
		t.Errorf("assertSafeIdentifier rejected a well-formed name: %v", err)
	}
}

// TestGeneratedScratchNamesAlwaysPassTheIdentifierGuard ties the two halves
// together: whatever scratchName produces must survive the DDL guard, so the
// guard can never reject a name the harness itself generated.
func TestGeneratedScratchNamesAlwaysPassTheIdentifierGuard(t *testing.T) {
	t.Setenv(ScratchPrefixEnv, "lane_4428")
	for range 64 {
		name, err := scratchName()
		if err != nil {
			t.Fatalf("scratchName: %v", err)
		}
		if err := assertSafeIdentifier(name); err != nil {
			t.Fatalf("generated name %q failed the identifier guard: %v", name, err)
		}
	}
}

func TestWithDatabasePathReplacesDatabase(t *testing.T) {
	got, err := withDatabaseURL("postgres://u:p@example:5432/original?sslmode=disable", "scratch_x")
	if err != nil {
		t.Fatalf("withDatabasePath: %v", err)
	}
	const want = "postgres://u:p@example:5432/scratch_x?sslmode=disable"
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

// TestWithPostgresDatabaseHandlesKeywordValueDSNs covers the DSN form pgx
// accepts that url.Parse cannot read.
//
// This is the review finding that url.Parse alone was the wrong tool. On the
// first input below url.Parse errors on the invalid escape; on the second it
// SUCCEEDS and returns the whole string as an opaque path, so a path rewrite
// would have produced "/scratch_x" -- a DSN that silently lost the host, the
// user and the password.
func TestWithPostgresDatabaseHandlesKeywordValueDSNs(t *testing.T) {
	for name, dsn := range map[string]string{
		// The reviewer's exact repro input.
		"invalid URL escape": "host=127.0.0.1 port=5432 user=u password=p dbname=acr application_name=literal%zz",
		"plain":              "host=127.0.0.1 port=5432 user=u password=p dbname=acr",
	} {
		t.Run(name, func(t *testing.T) {
			got, err := withPostgresDatabase(dsn, "scratch_x")
			if err != nil {
				t.Fatalf("withPostgresDatabase: %v", err)
			}
			// Ask the driver, not a regex: the only claim that matters is
			// which database pgx actually resolves.
			config, err := pgx.ParseConfig(got)
			if err != nil {
				t.Fatalf("pgx could not parse the rewritten DSN %q: %v", got, err)
			}
			if config.Database != "scratch_x" {
				t.Errorf("pgx resolved database %q, want scratch_x (rewritten DSN: %q)", config.Database, got)
			}
			if config.Host != "127.0.0.1" || config.User != "u" {
				t.Errorf("rewrite lost connection details: host=%q user=%q", config.Host, config.User)
			}
		})
	}
}

func TestWithPostgresDatabaseStillRewritesURLForm(t *testing.T) {
	got, err := withPostgresDatabase("postgres://u:p@127.0.0.1:5432/acr?sslmode=disable", "scratch_x")
	if err != nil {
		t.Fatalf("withPostgresDatabase: %v", err)
	}
	config, err := pgx.ParseConfig(got)
	if err != nil {
		t.Fatalf("pgx could not parse %q: %v", got, err)
	}
	if config.Database != "scratch_x" {
		t.Errorf("pgx resolved database %q, want scratch_x", config.Database)
	}
}

// TestWithDatabaseURLRejectsANonURLDSN stops url.Parse's opaque-path behaviour
// from silently discarding a DSN's connection details.
func TestWithDatabaseURLRejectsANonURLDSN(t *testing.T) {
	_, err := withDatabaseURL("host=127.0.0.1 port=5432 dbname=acr", "scratch_x")
	if err == nil || !strings.Contains(err.Error(), "not in URL form") {
		t.Errorf("want a rejection naming the URL-form requirement, got: %v", err)
	}
}

// TestStartPostgresRejectsAnUnrewritableDSNBeforeCreating pins the ordering
// that prevents an orphan: a DSN the rewrite cannot handle must fail BEFORE the
// database is created, or nothing holds the cleanup that would drop it.
func TestStartPostgresRejectsAnUnrewritableDSNBeforeCreating(t *testing.T) {
	recordDockerCalls(t)
	t.Setenv(PostgresDSNEnv, "postgres://u:p@127.0.0.1:1/ac%zz")
	t.Setenv(ScratchPrefixEnv, "")

	_, err := StartPostgres(shortCtx(t))

	if err == nil {
		t.Fatal("want the unrewritable DSN rejected, got nil")
	}
	// Had the rewrite run after the CREATE, we would have reached the
	// connect/create path against an unreachable host instead.
	for _, forbidden := range []string{"connect to remote PostgreSQL", "create scratch"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Errorf("DSN was rewritten after connecting/creating: %v", err)
		}
	}
}

// TestCloseRunsCleanupInsteadOfTerminating pins that a remote instance drops
// its scratch database. A remote Instance has a nil Container, and the old
// Close returned nil for exactly that case -- so without this the scratch
// databases would accumulate silently on a shared server.
func TestCloseRunsCleanupInsteadOfTerminating(t *testing.T) {
	cleaned := false
	instance := &Instance{
		URI:     "postgres://example/scratch",
		cleanup: func(context.Context) error { cleaned = true; return nil },
	}

	if err := instance.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !cleaned {
		t.Error("Close did not run the scratch-database cleanup")
	}
}

func TestClickHouseHTTPDSNUsesRemoteHTTPURI(t *testing.T) {
	instance := &Instance{
		URI:     "clickhouse://u:p@example:9000/scratch_x",
		httpURI: "clickhouse://u:p@example:8123/scratch_x",
	}

	got, err := ClickHouseHTTPDSN(context.Background(), instance)
	if err != nil {
		t.Fatalf("ClickHouseHTTPDSN: %v", err)
	}
	if got != instance.httpURI {
		t.Errorf("got %s, want %s", got, instance.httpURI)
	}
}

// TestClickHouseHTTPDSNWithoutContainerOrHTTPURINamesTheFix keeps the failure
// actionable: the Python migration runner needs the HTTP port, and a remote
// run that set only the native DSN would otherwise fail without saying which
// variable is missing.
func TestClickHouseHTTPDSNWithoutContainerOrHTTPURINamesTheFix(t *testing.T) {
	_, err := ClickHouseHTTPDSN(context.Background(), &Instance{URI: "clickhouse://example/x"})
	if err == nil || !strings.Contains(err.Error(), ClickHouseHTTPDSNEnv) {
		t.Errorf("want an error naming %s, got: %v", ClickHouseHTTPDSNEnv, err)
	}
}
