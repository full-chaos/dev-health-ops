//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

var testopsRiskOutputTables = []string{"testops_release_confidence", "testops_quality_drag", "testops_pipeline_stability"}

const (
	riskRetiredOrgID = "00000000-0000-4000-8000-000000014009"
	riskRetiredRepo  = "00000000-0000-4000-8000-0000000140a1"
)

var riskRetiredDay = time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)

type riskRetiredStack struct {
	conn      driver.Conn
	executor  *TestopsRiskExecutor
	run       Run
	partition Partition
}

func startRiskRetiredStack(ctx context.Context, t *testing.T) riskRetiredStack {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	for _, statement := range []string{
		`CREATE TABLE ci_pipeline_runs (
    repo_id UUID, run_id String, status Nullable(String),
    queued_at Nullable(DateTime64(3, 'UTC')), started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')), last_synced DateTime64(3, 'UTC'),
    pipeline_name Nullable(String), provider LowCardinality(String) DEFAULT '',
    duration_seconds Nullable(Float64), queue_seconds Nullable(Float64),
    retry_count UInt32 DEFAULT 0, cancel_reason Nullable(String), trigger_source Nullable(String),
    commit_hash Nullable(String), branch Nullable(String), pr_number Nullable(UInt32),
    team_id Nullable(String), service_id Nullable(String), org_id LowCardinality(String) DEFAULT ''
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, run_id)`,
		`CREATE TABLE test_case_results (
    repo_id UUID, run_id String, suite_id String, case_id String, case_name String,
    class_name Nullable(String), status LowCardinality(String), duration_seconds Nullable(Float64),
    retry_attempt UInt32 DEFAULT 0, failure_message Nullable(String), failure_type Nullable(String),
    stack_trace Nullable(String), is_quarantined UInt8 DEFAULT 0,
    org_id LowCardinality(String) DEFAULT '', last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, run_id, suite_id, case_id)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	chschema.CreateTables(ctx, t, conn, append([]string{"test_suite_results", "coverage_snapshots"}, testopsRiskOutputTables...)...)
	applyTestopsRepositoryOwnershipSchema(ctx, t, conn)
	seedTestopsAuthoritativeOwner(ctx, t, conn, riskRetiredOrgID, uuid.MustParse(riskRetiredRepo), riskRetiredDay)

	executor, err := NewTestopsRiskExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	return riskRetiredStack{
		conn:     conn,
		executor: executor,
		run:      Run{OrganizationID: riskRetiredOrgID, TargetDay: riskRetiredDay},
		partition: Partition{
			ID:      "00000000-0000-4000-8000-000000014021",
			RunID:   "00000000-0000-4000-8000-000000014020",
			RepoIDs: []RepositoryID{RepositoryID(riskRetiredRepo)},
		},
	}
}

func (stack riskRetiredStack) exec(ctx context.Context, t *testing.T, statement string) {
	t.Helper()
	if err := stack.conn.Exec(ctx, statement); err != nil {
		t.Fatal(err)
	}
}

// seedRun writes one version of run-1 with its suite and one failed case. day is the day the run started; status
// and finished decide whether it is a completed run.
func (stack riskRetiredStack) seedRun(ctx context.Context, t *testing.T, day, status string, finished bool, version string) {
	t.Helper()
	finishedAt := "NULL"
	if finished {
		finishedAt = "toDateTime64('" + day + " 09:11:00', 3, 'UTC')"
	}
	stack.exec(ctx, t, `INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, retry_count, org_id) VALUES
(toUUID('`+riskRetiredRepo+`'), 'run-1', '`+status+`', toDateTime64('`+day+` 09:00:00', 3, 'UTC'), toDateTime64('`+day+` 09:01:00', 3, 'UTC'), `+finishedAt+`, toDateTime64('`+version+`', 3, 'UTC'), 0, '`+riskRetiredOrgID+`')`)
	stack.exec(ctx, t, `INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, started_at, finished_at, org_id, last_synced) VALUES
(toUUID('`+riskRetiredRepo+`'), 'run-1', 'suite-1', 'unit', 10, 9, 1, 0, toDateTime64('`+day+` 09:01:00', 3, 'UTC'), toDateTime64('`+day+` 09:05:00', 3, 'UTC'), '`+riskRetiredOrgID+`', toDateTime64('`+version+`', 3, 'UTC'))`)
	stack.exec(ctx, t, `INSERT INTO test_case_results (repo_id, run_id, suite_id, case_id, case_name, status, org_id, last_synced) VALUES
(toUUID('`+riskRetiredRepo+`'), 'run-1', 'suite-1', 'c1', 'test_one', 'failed', '`+riskRetiredOrgID+`', toDateTime64('`+version+`', 3, 'UTC'))`)
}

func (stack riskRetiredStack) compute(ctx context.Context, t *testing.T, label string, at time.Time) int {
	t.Helper()
	stack.executor.nowUTC = func() time.Time { return at }
	written, err := stack.executor.ComputeFamily(ctx, stack.run, stack.partition)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	t.Logf("%s: written=%d", label, written)
	return written
}

// liveRows counts the rows of (org, repo, D) a reader takes as a measurement: the newest row of the key (FINAL), and
// not a retraction when the table has the marker column.
func (stack riskRetiredStack) liveRows(ctx context.Context, t *testing.T, table string) uint64 {
	t.Helper()
	var markers uint64
	if err := stack.conn.QueryRow(ctx, `SELECT count() FROM system.columns
WHERE database = currentDatabase() AND table = ? AND name = 'retracted'`, table).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	live := ""
	if markers > 0 {
		live = " AND retracted = 0"
	}
	var count uint64
	if err := stack.conn.QueryRow(ctx, `SELECT count() FROM `+table+` FINAL
WHERE org_id = ? AND repo_id = toUUID(?) AND day = ?`+live, riskRetiredOrgID, riskRetiredRepo, riskRetiredDay).Scan(&count); err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	t.Logf("%s: live rows for (org, repo, D) = %d (marker column: %t)", table, count, markers > 0)
	return count
}

// A recompute of day D whose only run moved to D+1 gives the family no record for D in any of its tables: each
// table must then hold no live row for D. When the run comes back to D, the next compute makes D live again.
func TestTestopsRiskRetiresADayTheRecomputeNoLongerCounts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	stack := startRiskRetiredStack(ctx, t)

	stack.seedRun(ctx, t, "2026-08-15", "failure", true, "2026-08-15 10:00:00")
	if written := stack.compute(ctx, t, "compute 1 (run on D)", time.Date(2026, 8, 16, 1, 0, 0, 0, time.UTC)); written != 3 {
		t.Fatalf("fixture: compute 1 wrote %d rows, want 3", written)
	}
	stack.seedRun(ctx, t, "2026-08-16", "failure", true, "2026-08-16 12:00:00")
	stack.compute(ctx, t, "compute 2 (run moved to D+1)", time.Date(2026, 8, 17, 1, 0, 0, 0, time.UTC))

	for _, table := range testopsRiskOutputTables {
		t.Run("retired/"+table, func(t *testing.T) {
			if live := stack.liveRows(ctx, t, table); live != 0 {
				t.Errorf("%s keeps %d live row(s) for a day the recompute gave no record", table, live)
			}
		})
	}

	stack.seedRun(ctx, t, "2026-08-15", "failure", true, "2026-08-17 12:00:00")
	if written := stack.compute(ctx, t, "compute 3 (run back on D)", time.Date(2026, 8, 18, 1, 0, 0, 0, time.UTC)); written < 3 {
		t.Fatalf("fixture: compute 3 wrote %d rows, want at least 3", written)
	}
	for _, table := range testopsRiskOutputTables {
		t.Run("reappears/"+table, func(t *testing.T) {
			if live := stack.liveRows(ctx, t, table); live != 1 {
				t.Errorf("%s holds %d live row(s) for a day the compute counts again, want 1", table, live)
			}
		})
	}
}

// A recompute in which the run of D is no longer completed keeps the pipeline and test records (release confidence
// and quality drag) but has no stability: only the stability row of D is retired.
func TestTestopsRiskRetiresOnlyTheTableThatLostItsRecord(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	stack := startRiskRetiredStack(ctx, t)

	stack.seedRun(ctx, t, "2026-08-15", "failure", true, "2026-08-15 10:00:00")
	if written := stack.compute(ctx, t, "compute 1 (completed run on D)", time.Date(2026, 8, 16, 1, 0, 0, 0, time.UTC)); written != 3 {
		t.Fatalf("fixture: compute 1 wrote %d rows, want 3", written)
	}
	stack.seedRun(ctx, t, "2026-08-15", "running", false, "2026-08-16 12:00:00")
	stack.compute(ctx, t, "compute 2 (run on D not completed)", time.Date(2026, 8, 17, 1, 0, 0, 0, time.UTC))

	want := map[string]uint64{"testops_release_confidence": 1, "testops_quality_drag": 1, "testops_pipeline_stability": 0}
	for _, table := range testopsRiskOutputTables {
		t.Run(table, func(t *testing.T) {
			if live := stack.liveRows(ctx, t, table); live != want[table] {
				t.Errorf("%s: %d live row(s) for D, want %d", table, live, want[table])
			}
		})
	}
}
