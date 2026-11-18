//go:build integration

package daily

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The cicd daily writer retires a repository-day it no longer counts (CHAOS-14016, W1), and an older read never
// overwrites a newer count (the write fence): the version of every row is the instant of its source read, and a key
// whose stored row is not older than that instant is left alone.

type cicdFenceEnv struct {
	t    *testing.T
	ctx  context.Context
	conn driver.Conn
}

func newCICDFenceEnv(t *testing.T) *cicdFenceEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &cicdFenceEnv{t: t, ctx: ctx, conn: conn}
}

func (e *cicdFenceEnv) exec(statement string, args ...any) {
	e.t.Helper()
	if err := e.conn.Exec(e.ctx, statement, args...); err != nil {
		e.t.Fatalf("%v\n%s", err, statement)
	}
}

func (e *cicdFenceEnv) run(org, repo, id, status, started, finished, synced string) {
	e.t.Helper()
	e.exec(`INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, status, started_at, finished_at, last_synced)
VALUES (?, toUUID(?), ?, ?, toDateTime64(?, 3, 'UTC'), toDateTime64(?, 3, 'UTC'), toDateTime64(?, 3, 'UTC'))`,
		org, repo, id, status, started, finished, synced)
}

func (e *cicdFenceEnv) compute(conn driver.Conn, org, repo, day string) {
	e.t.Helper()
	executor, err := NewCICDExecutor(conn)
	if err != nil {
		e.t.Fatal(err)
	}
	target, _ := time.Parse(time.DateOnly, day)
	run := Run{ID: "run-" + org + "-" + day, OrganizationID: org, TargetDay: target}
	part := Partition{ID: "part-" + org + "-" + day, RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo)}}
	if _, err := executor.ComputeFamily(e.ctx, run, part); err != nil {
		e.t.Fatal(err)
	}
}

// newest is the newest stored row of the key under FINAL (an equal version: the row inserted last).
func (e *cicdFenceEnv) newest(org, repo, day string) (rows uint64, pipelines uint32, retracted uint8) {
	e.t.Helper()
	if err := e.conn.QueryRow(e.ctx, `SELECT count() FROM cicd_metrics_daily WHERE org_id = ? AND repo_id = toUUID(?) AND day = ?`,
		org, repo, day).Scan(&rows); err != nil {
		e.t.Fatal(err)
	}
	if rows == 0 {
		return 0, 0, 0
	}
	if err := e.conn.QueryRow(e.ctx, `SELECT pipelines_count, retracted FROM cicd_metrics_daily FINAL
WHERE org_id = ? AND repo_id = toUUID(?) AND day = ?`, org, repo, day).Scan(&pipelines, &retracted); err != nil {
		e.t.Fatal(err)
	}
	return rows, pipelines, retracted
}

const fenceRepo = "14016000-0000-4000-8000-0000000000f5"

// W1: a rerun moved the only run of D to D+1. The recompute of D writes a row that retires D (retracted = 1, no
// measure); the compute of D+1 counts the run there.
func TestCICDRecomputeRetiresADayItNoLongerCounts(t *testing.T) {
	e := newCICDFenceEnv(t)
	org := "org-14016-w1"
	e.run(org, fenceRepo, "r1", "failure", "2026-08-20 23:05:00", "2026-08-20 23:35:00", "2026-08-21 00:30:00")
	e.compute(e.conn, org, fenceRepo, "2026-08-20")
	if _, pipelines, retracted := e.newest(org, fenceRepo, "2026-08-20"); pipelines != 1 || retracted != 0 {
		t.Fatalf("before the rerun: D holds %d pipelines (retracted %d), want 1 live", pipelines, retracted)
	}
	e.run(org, fenceRepo, "r1", "success", "2026-08-21 05:01:00", "2026-08-21 05:10:00", "2026-08-21 05:30:00")
	time.Sleep(1100 * time.Millisecond) // computed_at is whole seconds: the recompute reads in a later second
	e.compute(e.conn, org, fenceRepo, "2026-08-20")
	e.compute(e.conn, org, fenceRepo, "2026-08-21")
	if _, pipelines, retracted := e.newest(org, fenceRepo, "2026-08-20"); retracted != 1 || pipelines != 0 {
		t.Errorf("after the rerun: D's newest row holds %d pipelines, retracted %d; want the retraction row (0, 1)", pipelines, retracted)
	}
	if _, pipelines, retracted := e.newest(org, fenceRepo, "2026-08-21"); pipelines != 1 || retracted != 0 {
		t.Errorf("after the rerun: D+1 holds %d pipelines (retracted %d), want 1 live", pipelines, retracted)
	}
	// A recompute of a day that holds no stored row and no run writes nothing.
	e.compute(e.conn, org, fenceRepo, "2026-08-22")
	if rows, _, _ := e.newest(org, fenceRepo, "2026-08-22"); rows != 0 {
		t.Errorf("a day with no run and no stored row holds %d rows, want none", rows)
	}
}

// overlapConn runs hook once, right after the first read of ci_pipeline_runs has started: that read keeps the
// data it started on, and the hook is a newer compute that finishes first.
type overlapConn struct {
	driver.Conn
	fired atomic.Bool
	hook  func()
}

func (conn *overlapConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	rows, err := conn.Conn.Query(ctx, query, args...)
	if err == nil && strings.Contains(query, "FROM ci_pipeline_runs") && conn.fired.CompareAndSwap(false, true) {
		conn.hook()
	}
	return rows, err
}

// The write fence: compute A reads D's two runs; while A works, the rerun moves one run to D+1 and compute B reads
// and writes D (1 run). A writes last. B's newer count stays the newest row of D.
func TestCICDOlderReadWrittenLastDoesNotWin(t *testing.T) {
	e := newCICDFenceEnv(t)
	org := "org-14016-fence"
	e.run(org, fenceRepo, "r1", "success", "2026-08-20 10:00:00", "2026-08-20 10:10:00", "2026-08-20 10:20:00")
	e.run(org, fenceRepo, "r2", "failure", "2026-08-20 22:00:00", "2026-08-20 23:50:00", "2026-08-20 23:55:00")
	slow := &overlapConn{Conn: e.conn}
	slow.hook = func() {
		e.run(org, fenceRepo, "r2", "success", "2026-08-21 00:05:00", "2026-08-21 00:20:00", "2026-08-21 00:30:00")
		e.compute(e.conn, org, fenceRepo, "2026-08-20")
	}
	e.compute(slow, org, fenceRepo, "2026-08-20")
	if !slow.fired.Load() {
		t.Fatal("the overlap hook did not run: compute A never read ci_pipeline_runs")
	}
	if _, pipelines, retracted := e.newest(org, fenceRepo, "2026-08-20"); pipelines != 1 || retracted != 0 {
		t.Errorf("D's newest row holds %d pipelines (retracted %d), want B's newer count 1: the older read written last won", pipelines, retracted)
	}
}

// A stored row stamped after this compute's read (a newer compute, or a host clock ahead) is left alone.
func TestCICDComputeLeavesAStoredRowNewerThanItsRead(t *testing.T) {
	e := newCICDFenceEnv(t)
	org := "org-14016-newer"
	e.run(org, fenceRepo, "r1", "success", "2026-08-20 10:00:00", "2026-08-20 10:10:00", "2026-08-20 10:20:00")
	e.exec(`INSERT INTO cicd_metrics_daily (org_id, repo_id, day, pipelines_count, success_rate, computed_at)
VALUES (?, toUUID(?), '2026-08-20', 7, 1, '2099-01-01 00:00:00')`, org, fenceRepo)
	e.compute(e.conn, org, fenceRepo, "2026-08-20")
	if rows, pipelines, _ := e.newest(org, fenceRepo, "2026-08-20"); rows != 1 || pipelines != 7 {
		t.Errorf("D holds %d rows, newest %d pipelines; want the newer stored row alone (1 row, 7)", rows, pipelines)
	}
}
