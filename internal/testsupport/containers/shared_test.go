package containers

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
)

func captureSharedLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	original := sharedLog
	sharedLog = &buffer
	t.Cleanup(func() { sharedLog = original })
	return &buffer
}

// TestSharedModeLeavesARemoteDSNInCharge covers the first escape hatch: a
// caller's own remote server wins over the shared server, and Docker is never asked.
func TestSharedModeLeavesARemoteDSNInCharge(t *testing.T) {
	calls := recordDockerCalls(t)
	t.Setenv(PostgresDSNEnv, unreachablePostgresDSN)
	t.Setenv(ClickHouseDSNEnv, unreachableClickHouseDSN)

	code := RunWithSharedServers(func() int {
		if _, err := StartPostgres(shortCtx(t)); err == nil || !strings.Contains(err.Error(), "connect to remote PostgreSQL") {
			t.Errorf("want the remote PostgreSQL path, got: %v", err)
		}
		if _, err := StartClickHouse(shortCtx(t)); err == nil || !strings.Contains(err.Error(), "create scratch ClickHouse database") {
			t.Errorf("want the remote ClickHouse path, got: %v", err)
		}
		return 0
	})

	if code != 0 {
		t.Errorf("exit code %d, want 0", code)
	}
	if got := calls(); got != 0 {
		t.Errorf("Docker was asked %d times with remote DSNs set; want 0", got)
	}
}

// TestSharedModeOffStartsOneContainerPerCall covers the second escape hatch.
func TestSharedModeOffStartsOneContainerPerCall(t *testing.T) {
	calls := recordDockerCalls(t)
	t.Setenv(SharedServersEnv, "off")

	RunWithSharedServers(func() int {
		if activeSharedServers() != nil {
			t.Error("shared mode is active with " + SharedServersEnv + "=off")
		}
		for range 3 {
			_, _ = StartPostgres(shortCtx(t))
		}
		return 0
	})

	if got := calls(); got != 3 {
		t.Errorf("Docker was asked %d times for 3 calls with shared mode off; want 3", got)
	}
}

func TestSharedModeStartsOneContainerPerEngine(t *testing.T) {
	calls := recordDockerCalls(t)

	RunWithSharedServers(func() int {
		for range 3 {
			if _, err := StartPostgres(shortCtx(t)); err == nil || !strings.Contains(err.Error(), "shared PostgreSQL server") {
				t.Errorf("want the shared PostgreSQL start error, got: %v", err)
			}
		}
		for range 2 {
			if _, err := StartClickHouse(shortCtx(t)); err == nil || !strings.Contains(err.Error(), "shared ClickHouse server") {
				t.Errorf("want the shared ClickHouse start error, got: %v", err)
			}
		}
		return 0
	})

	if got := calls(); got != 2 {
		t.Errorf("Docker was asked %d times for 3 PostgreSQL + 2 ClickHouse calls in shared mode; want 2", got)
	}
	if activeSharedServers() != nil {
		t.Error("shared mode is still active after RunWithSharedServers returned")
	}
}

func TestIsolatedNeverUsesSharedServerOrRemoteDSN(t *testing.T) {
	calls := recordDockerCalls(t)
	t.Setenv(PostgresDSNEnv, unreachablePostgresDSN)
	t.Setenv(ClickHouseDSNEnv, unreachableClickHouseDSN)

	RunWithSharedServers(func() int {
		for range 2 {
			_, _ = StartPostgresIsolated(shortCtx(t))
			_, _ = StartClickHouseIsolated(shortCtx(t))
		}
		return 0
	})

	if got := calls(); got != 4 {
		t.Errorf("Docker was asked %d times for 4 isolated calls; want 4", got)
	}
}

func TestRunWithSharedServersRefusesAnUnknownSwitchValue(t *testing.T) {
	log := captureSharedLog(t)
	t.Setenv(SharedServersEnv, "maybe")
	ran := false

	code := RunWithSharedServers(func() int { ran = true; return 0 })

	if code != 1 || ran {
		t.Errorf("code %d ran %v; want 1 and the tests not run", code, ran)
	}
	if !strings.Contains(log.String(), SharedServersEnv) {
		t.Errorf("no FAIL line names %s: %q", SharedServersEnv, log.String())
	}
}

func TestDiffSnapshotsNamesEveryNewAndGoneRow(t *testing.T) {
	before := []string{"database worker_test", "role worker_test", "role pg_monitor"}
	after := []string{
		"database worker_test", "database dh_t_0123", "role worker_test", "role pg_monitor",
		"role planted", "membership worker_test in pg_monitor", "alter-system work_mem=7MB",
	}

	got := diffSnapshots(before, after)

	want := []string{
		"new alter-system work_mem=7MB", "new database dh_t_0123",
		"new membership worker_test in pg_monitor", "new role planted",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("diff = %q, want %q", got, want)
	}
	if got := diffSnapshots(before, before[:2]); !reflect.DeepEqual(got, []string{"gone role pg_monitor"}) {
		t.Errorf("diff of a removed row = %q", got)
	}
	if got := diffSnapshots(before, before); got != nil {
		t.Errorf("diff of equal snapshots = %q, want none", got)
	}
}

func TestFinishReportsEveryEngineLeak(t *testing.T) {
	servers := &sharedServers{
		pg: &sharedServer{engine: "postgres", baseline: []string{"role a"},
			snapshot: func(context.Context, string) ([]string, error) { return []string{"role a", "role b"}, nil }},
		ch: &sharedServer{engine: "clickhouse", baseline: []string{"user u"},
			snapshot: func(context.Context, string) ([]string, error) {
				return []string{"user u", "role-grant u||r|0"}, nil
			}},
	}

	findings, err := servers.finish(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	want := []string{"postgres new role b", "clickhouse new role-grant u||r|0"}
	if !reflect.DeepEqual(findings, want) {
		t.Errorf("findings = %q, want %q", findings, want)
	}
}

// TestCloseDropsWithACancelledContext: a t.Cleanup that passes the test's own,
// already cancelled context must still drop the scratch database.
func TestCloseDropsWithACancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var seen error
	instance := &Instance{cleanup: func(ctx context.Context) error { seen = ctx.Err(); return seen }}

	if err := instance.Close(ctx); err != nil || seen != nil {
		t.Errorf("Close with a cancelled context: err %v, cleanup saw %v; want the drop to run uncancelled", err, seen)
	}
}

func TestRequiredPostgresConnectionsCoversEveryParallelPool(t *testing.T) {
	for _, tc := range []struct{ parallel, pool int }{{1, 4}, {4, 4}, {4, 64}, {8, 16}} {
		got := requiredPostgresConnections(tc.parallel, tc.pool)
		if got < tc.parallel*tc.pool*2 {
			t.Errorf("parallel %d pool %d: %d connections leave no headroom", tc.parallel, tc.pool, got)
		}
	}
}

func TestTimingLinesOnlyWhenSet(t *testing.T) {
	var buffer bytes.Buffer
	original := timingLog
	timingLog = &buffer
	t.Cleanup(func() { timingLog = original })

	t.Setenv(HarnessTimingEnv, "")
	TimePhase("start", "postgres", "container")()
	if buffer.Len() != 0 {
		t.Fatalf("timing written with %s unset: %q", HarnessTimingEnv, buffer.String())
	}

	t.Setenv(HarnessTimingEnv, "1")
	TimePhase("start", "postgres", "shared")()
	TimePhase("schema", "clickhouse", "")()

	lines := strings.Split(strings.TrimSpace(buffer.String()), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], harnessTimingPrefix+"\tstart\tpostgres\tshared\t") ||
		!strings.HasPrefix(lines[1], harnessTimingPrefix+"\tschema\tclickhouse\t\t") {
		t.Errorf("timing lines = %q", lines)
	}
}
