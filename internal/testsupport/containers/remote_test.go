package containers

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

	_, err := StartClickHouse(shortCtx(t))

	if got := calls(); got != 0 {
		t.Errorf("Docker was asked for a container %d times with %s set; want 0", got, ClickHouseDSNEnv)
	}
	if err == nil {
		t.Error("want a failure from the unreachable remote ClickHouse, got nil")
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

func TestWithDatabasePathReplacesDatabase(t *testing.T) {
	got, err := withDatabasePath("postgres://u:p@example:5432/original?sslmode=disable", "scratch_x")
	if err != nil {
		t.Fatalf("withDatabasePath: %v", err)
	}
	const want = "postgres://u:p@example:5432/scratch_x?sslmode=disable"
	if got != want {
		t.Errorf("got %s, want %s", got, want)
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
