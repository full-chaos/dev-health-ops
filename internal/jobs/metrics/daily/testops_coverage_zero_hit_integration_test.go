//go:build integration

package daily

// A zero-hit coverage snapshot (lines_covered 0, pct 0) reaches the daily coverage row as 0, with a delta against the prior
// snapshot; a snapshot with no denominator (lines_total 0, pct NULL) stores 0/0 and no pct or delta; a snapshot with no line
// data stores NULL counts (CHAOS-14284).

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestCoverageRowKeepsZeroHitNoDenominatorAndNoDataApart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	chschema.Apply(ctx, t, instance)

	const orgID = "00000000-0000-4000-8000-000000014284"
	repoID := uuid.MustParse("00000000-0000-4000-8000-0000000142b1")
	day := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	seedTestopsDifferentialFixture(ctx, t, conn, orgID, repoID, day)
	seedTestopsAuthoritativeOwner(ctx, t, conn, orgID, repoID, day)

	executor, err := NewTestopsCoverageExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	compute := func() {
		t.Helper()
		n++
		id := time.Now().Format("150405.000") + string(rune('a'+n))
		run := Run{ID: id, OrganizationID: orgID, TargetDay: day}
		part := Partition{ID: "part-" + id, RunID: id, RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
		if _, err := executor.ComputeFamily(ctx, run, part); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1100 * time.Millisecond)
	}
	type stored struct {
		total, covered *uint32
		pct, delta     *float64
	}
	read := func() stored {
		t.Helper()
		var s stored
		if err := conn.QueryRow(ctx, `SELECT (argMax(tuple(lines_total), computed_at)).1, (argMax(tuple(lines_covered), computed_at)).1,
(argMax(tuple(line_coverage_pct), computed_at)).1, (argMax(tuple(coverage_delta_pct), computed_at)).1
FROM testops_coverage_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ?`, orgID, repoID, day).Scan(&s.total, &s.covered, &s.pct, &s.delta); err != nil {
			t.Fatal(err)
		}
		return s
	}
	// run-1/snap-b is the newest snapshot of the day; run-prior/snap-p (61.5) is the prior one.
	snapshot := func(version time.Time, total, covered *uint32, pct *float64) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO coverage_snapshots
(repo_id, run_id, snapshot_id, lines_total, lines_covered, line_coverage_pct, team_id, service_id, org_id, last_synced)
VALUES (?, 'run-1', 'snap-b', ?, ?, ?, NULL, NULL, ?, ?)`, repoID, total, covered, pct, orgID, version); err != nil {
			t.Fatal(err)
		}
	}
	u := func(v uint32) *uint32 { return &v }
	f := func(v float64) *float64 { return &v }

	snapshot(time.Now().UTC().Add(time.Hour), u(100), u(0), f(0))
	compute()
	got := read()
	if got.total == nil || *got.total != 100 || got.covered == nil || *got.covered != 0 || got.pct == nil || *got.pct != 0 {
		t.Fatalf("zero-hit snapshot stored %v/%v pct %v, want 0/100 pct 0", got.covered, got.total, got.pct)
	}
	if got.delta == nil || math.Abs(*got.delta-(0-61.5)) > 1e-9 {
		t.Fatalf("zero-hit snapshot stored delta %v, want -61.5 against the prior snapshot", got.delta)
	}

	snapshot(time.Now().UTC().Add(2*time.Hour), u(0), u(0), nil)
	compute()
	got = read()
	if got.total == nil || *got.total != 0 || got.covered == nil || *got.covered != 0 || got.pct != nil || got.delta != nil {
		t.Fatalf("no-denominator snapshot stored %v/%v pct %v delta %v, want 0/0 and no pct or delta", got.covered, got.total, got.pct, got.delta)
	}

	snapshot(time.Now().UTC().Add(3*time.Hour), nil, nil, nil)
	compute()
	got = read()
	if got.total != nil || got.covered != nil || got.pct != nil {
		t.Fatalf("no-line-data snapshot stored %v/%v pct %v, want NULL", got.covered, got.total, got.pct)
	}
}
