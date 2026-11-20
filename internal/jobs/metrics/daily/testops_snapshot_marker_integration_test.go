//go:build integration

package daily

// The marker that retires a testops_test_metrics_daily (repository, day) carries the snapshot instant of the run that wrote it,
// minus one second, never a version raised above a stored row. Two runs of one repository-day can overlap (distinct generations are distinct
// runs and nothing serialises their execution), so the newest snapshot must win: an older run that finishes late retires
// nothing a newer snapshot wrote, and an equal snapshot is a no-op. Real ClickHouse; merges are stopped so a count of stored
// rows is the rows written.

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

const snapshotOrg = "org-testops-snapshot"

var snapshotDay = time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)

func snapshotAt(second int) time.Time { return time.Date(2026, 1, 3, 0, 0, second, 0, time.UTC) }

func snapshotConn(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	conn := startDefaultBranchClickHouse(ctx, t)
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES testops_test_metrics_daily"); err != nil {
		t.Fatal(err)
	}
	return conn
}

func snapshotSeedLive(t *testing.T, ctx context.Context, conn driver.Conn, repo uuid.UUID, version time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, passed_count, failed_count, pass_rate,
  failure_rate, flake_rate, total_suites, flake_cases, distinct_cases, team_id, org_id, computed_at)
VALUES (?, ?, 4, 2, 2, 0.5, 0.5, 0.5, 1, 1, 2, 'team-1', ?, ?)`, repo, snapshotDay, snapshotOrg, version.UTC()); err != nil {
		t.Fatal(err)
	}
}

type snapshotState struct {
	rows      uint64
	markers   uint64
	retracted uint8
	newest    time.Time
}

func snapshotRead(t *testing.T, ctx context.Context, conn driver.Conn, repo uuid.UUID) snapshotState {
	t.Helper()
	var got snapshotState
	if err := conn.QueryRow(ctx, `SELECT count(), countIf(retracted = 1), argMax(retracted, computed_at), max(computed_at)
FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ?`, snapshotOrg, repo, snapshotDay).Scan(
		&got.rows, &got.markers, &got.retracted, &got.newest); err != nil {
		t.Fatal(err)
	}
	t.Logf("SNAPSHOT STATE repo=%s %+v", repo, got)
	return got
}

func snapshotPartitionRetire(t *testing.T, ctx context.Context, conn driver.Conn, repo uuid.UUID, snapshot time.Time) {
	t.Helper()
	scope := newStaleKeyScope([]string{repo.String()})
	if _, err := supersedeStaleTeamKeys(ctx, conn, teamkeytables.TestopsTestMetricsDaily, snapshotOrg, snapshotDay, scope, nil, snapshot); err != nil {
		t.Fatal(err)
	}
}

func snapshotOrgRetire(t *testing.T, ctx context.Context, conn driver.Conn, snapshot time.Time, readPresent func(context.Context) (staleKeyScope, error)) {
	t.Helper()
	if _, err := retractStaleTeamKeysOutsideRun(ctx, conn, teamkeytables.TestopsTestMetricsDaily, snapshotOrg, snapshotDay,
		newStaleKeyScope(), readPresent, snapshot); err != nil {
		t.Fatal(err)
	}
}

func noRepositories(context.Context) (staleKeyScope, error) { return newStaleKeyScope(), nil }

// R1: a newer snapshot's live row is stored; an older run, finishing late, finds the key live and must not retire it.
func TestAnOlderRunFinishingLateNeverRetiresANewerSnapshotsRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := snapshotConn(t, ctx)
	repo := uuid.MustParse("13928000-0000-4000-8000-0000000000e1")
	snapshotSeedLive(t, ctx, conn, repo, snapshotAt(20))
	snapshotPartitionRetire(t, ctx, conn, repo, snapshotAt(10))
	if got := snapshotRead(t, ctx, conn, repo); got.markers != 0 || got.retracted != 0 || got.rows != 1 {
		t.Fatalf("an older snapshot retired a newer live row: %+v, want 1 row, no marker", got)
	}
}

// R2: an equal snapshot is a no-op; a stored row older than the snapshot is retired one second below the snapshot (the control).
func TestAMarkerCarriesTheSnapshotInstantAndAnEqualSnapshotIsANoOp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := snapshotConn(t, ctx)
	equal := uuid.MustParse("13928000-0000-4000-8000-0000000000e2")
	older := uuid.MustParse("13928000-0000-4000-8000-0000000000e3")
	snapshotSeedLive(t, ctx, conn, equal, snapshotAt(10))
	snapshotSeedLive(t, ctx, conn, older, snapshotAt(5))
	snapshotPartitionRetire(t, ctx, conn, equal, snapshotAt(10))
	snapshotPartitionRetire(t, ctx, conn, older, snapshotAt(10))
	if got := snapshotRead(t, ctx, conn, equal); got.markers != 0 || got.retracted != 0 || got.rows != 1 {
		t.Fatalf("an equal snapshot wrote a marker: %+v, want a no-op", got)
	}
	got := snapshotRead(t, ctx, conn, older)
	if got.markers != 1 || got.retracted != 1 || !got.newest.Equal(snapshotAt(9)) {
		t.Fatalf("an older stored row was not retired below the snapshot: %+v, want a marker at %s", got, snapshotAt(9))
	}
}

// R3: the finalizer of a whole-organization run. A repository that returns writes a newer live row after the finalizer's
// live read (inside the membership read); the finalizer's marker must not outrank it. The control retires a row older than the
// finalizer's snapshot one second below that snapshot, and an equal snapshot is a no-op.
func TestTheOrganizationFinalizerNeverRetiresARowANewerSnapshotWrote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := snapshotConn(t, ctx)
	returned := uuid.MustParse("13928000-0000-4000-8000-0000000000e4")
	gone := uuid.MustParse("13928000-0000-4000-8000-0000000000e5")
	equal := uuid.MustParse("13928000-0000-4000-8000-0000000000e6")
	snapshotSeedLive(t, ctx, conn, returned, snapshotAt(5))
	snapshotSeedLive(t, ctx, conn, gone, snapshotAt(5))
	snapshotSeedLive(t, ctx, conn, equal, snapshotAt(10))
	snapshotOrgRetire(t, ctx, conn, snapshotAt(10), func(inner context.Context) (staleKeyScope, error) {
		snapshotSeedLive(t, ctx, conn, returned, snapshotAt(20))
		return newStaleKeyScope(), nil
	})
	if got := snapshotRead(t, ctx, conn, returned); got.retracted != 0 || !got.newest.Equal(snapshotAt(20)) {
		t.Fatalf("the finalizer retired a row a newer snapshot wrote: %+v, want the live row at %s newest", got, snapshotAt(20))
	}
	if got := snapshotRead(t, ctx, conn, gone); got.markers != 1 || got.retracted != 1 || !got.newest.Equal(snapshotAt(9)) {
		t.Fatalf("a gone repository was not retired at the finalizer's snapshot: %+v", got)
	}
	if got := snapshotRead(t, ctx, conn, equal); got.markers != 0 || got.retracted != 0 {
		t.Fatalf("an equal snapshot wrote a marker: %+v", got)
	}
}

// R4: the named limit (ticket 14016, filed separately). A newer run finds no stored row for the key and writes nothing
// (missing stays missing); an older run then commits its live row late. The snapshot stamp cannot cover this: the stale live
// row stays the newest row of the key. This cell executes the limit; it must stay green until that ticket closes it.
func TestNamedLimitAnOlderLiveRowCommittedAfterANewerEmptyScanStaysLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := snapshotConn(t, ctx)
	repo := uuid.MustParse("13928000-0000-4000-8000-0000000000e7")
	snapshotPartitionRetire(t, ctx, conn, repo, snapshotAt(20))
	if got := snapshotRead(t, ctx, conn, repo); got.rows != 0 {
		t.Fatalf("a newer empty scan wrote a row: %+v", got)
	}
	snapshotSeedLive(t, ctx, conn, repo, snapshotAt(10))
	got := snapshotRead(t, ctx, conn, repo)
	if got.rows != 1 || got.retracted != 0 || got.markers != 0 {
		t.Fatalf("the limit moved: %+v, want the older live row alone (live)", got)
	}
}
