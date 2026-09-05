//go:build integration

package daily

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// ----------------------------------------------------------------------
// CHAOS-4284: the PUSHDOWN-vs-RAW differential.
//
// The three native testops families read ClickHouse differently from the
// row-at-a-time loaders CHAOS-4294 wrote next door: case rows are reduced to
// one row per case_name in-database, coverage is reduced to one snapshot, and
// dedup is FINAL, read-for-read with Python. Each of those is a place the
// new readers could silently disagree with the oracle-proved path.
//
// So this test runs BOTH paths over the SAME seeded ClickHouse and asserts the
// resulting records are equal FIELD BY FIELD, floats compared by bit pattern:
//
//	(a) pushdown readers  -> testops accumulators  -> records
//	(b) existing FINAL row loaders -> testops slice API -> records
//
// (b) is the side already covered by the live-Python oracles in
// internal/jobs/metrics/testops, so equality here transitively ties the
// pushdown path to Python without re-running Python inside a container.
//
// The fixture is deliberately hostile. Every row below exists to break a
// specific assumption:
//
//   - Five duplicate copies of each case row with ASCENDING last_synced and
//     CONTRADICTORY statuses. This is CHAOS-5045's shape (GitHub's TestOps
//     ARTIFACTS phase re-projecting the same rows every hourly unit). If the
//     argMax dedup is dropped or keyed wrongly, flake_rate and
//     retry_dependency_rate move, because a superseded "failed" would join
//     the newest "passed" in the same status set.
//   - Statuses with mixed case and surrounding whitespace, so a reader that
//     normalised in SQL instead of Go would diverge.
//   - A suite from the NEXT day sharing a run_id with today's, which the
//     day-boundary guard must exclude.
//   - team_id/service_id as nil vs "" -- distinct Python dict keys, and
//     therefore distinct output rows for testops_pipeline.
//   - Two coverage snapshots under one run_id, so the (run_id, snapshot_id)
//     lexical tie-break is actually exercised.
//
// SYSTEM STOP MERGES is held for the whole test (CHAOS-4902/CHAOS-4953,
// per-table not server-wide): without it ReplacingMergeTree may collapse the
// duplicates on its own, and both paths would then be passing on the ENGINE's
// dedup rather than on their own queries -- a vacuous green.
// ----------------------------------------------------------------------

func TestNativeTestopsPushdownMatchesRowLoadersAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Sorting keys carry org_id FIRST, matching production after migration
	// 042 (042_rmt_org_id_dedup_keys.py) -- NOT 029's original
	// (repo_id, ...) keys. The native readers GROUP BY exactly these tuples,
	// so a fixture on the pre-042 keys would be testing a shape that no
	// longer exists. Shared with the executor test below via
	// testopsDifferentialSchema so the two can never drift apart.
	for _, statement := range testopsDifferentialSchema() {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	for _, table := range []string{"ci_pipeline_runs", "test_suite_results", "test_case_results", "coverage_snapshots"} {
		if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatalf("stop merges %s: %v", table, err)
		}
		defer func(table string) {
			resumeCtx, cancelResume := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelResume()
			if err := conn.Exec(resumeCtx, "SYSTEM START MERGES "+table); err != nil {
				t.Errorf("resume merges %s: %v", table, err)
			}
		}(table)
	}

	const orgID = "00000000-0000-4000-8000-000000000009"
	repoID := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	day := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	start := day
	end := day.Add(24 * time.Hour)
	historyStart := day.AddDate(0, 0, -29)
	priorStart := day.AddDate(0, 0, -30)

	seedTestopsDifferentialFixture(ctx, t, conn, orgID, repoID, day)

	// One positive control PER TABLE. Previously only ci_pipeline_runs was
	// checked, and an executed mutation proved the consequence: FINAL could be
	// deleted from the four test_suite_results reads and the coverage_snapshots
	// read -- 5 of 10 sites -- with the whole suite still GREEN.
	assertTieFixtureActuallyTies(ctx, t, conn, orgID, repoID, "ci_pipeline_runs", "run_id")
	assertTieFixtureActuallyTies(ctx, t, conn, orgID, repoID, "test_suite_results", "run_id, suite_id")
	assertTieFixtureActuallyTies(ctx, t, conn, orgID, repoID, "test_case_results", "run_id, suite_id, case_id")
	assertTieFixtureActuallyTies(ctx, t, conn, orgID, repoID, "coverage_snapshots", "run_id, snapshot_id")

	// ---------------- testops_pipeline ----------------
	pushAccumulator := testops.NewPipelineAccumulator(repoID, repoID.String(), nil)
	if err := loadNativeTestopsPipelineRuns(ctx, conn, pushAccumulator, orgID, repoID, start, end); err != nil {
		t.Fatalf("pushdown pipeline read: %v", err)
	}
	pushPipeline := pushAccumulator.Finish()

	rawPipelineRows, err := loadTestopsPipelineRuns(ctx, conn, orgID, repoID, start, end)
	if err != nil {
		t.Fatalf("row-loader pipeline read: %v", err)
	}
	rawPipeline := testops.ComputePipelineMetrics(repoID, rawPipelineRows, repoID.String(), nil)

	if len(pushPipeline) != len(rawPipeline) {
		t.Fatalf("pipeline row count: pushdown=%d rowloader=%d", len(pushPipeline), len(rawPipeline))
	}
	if len(pushPipeline) < 2 {
		t.Fatalf("fixture is vacuous: expected >=2 pipeline groups (nil vs \"\" service_id), got %d", len(pushPipeline))
	}
	// r1 P3 non-vacuity guard: at least one group must contain a run that
	// landed in NO status bucket, which happens only for the NULL-status
	// winner. If the NULL seed is ever dropped or its status stops being NULL,
	// every run falls into success/failure/cancelled and the bare-vs-tuple
	// argMax distinction stops being exercised -- the test would keep passing
	// while proving nothing about the invariant it exists to protect.
	sawUnbucketed := false
	for _, metric := range pushPipeline {
		if metric.PipelinesCount > metric.SuccessCount+metric.FailureCount+metric.CancelledCount {
			sawUnbucketed = true
			break
		}
	}
	if !sawUnbucketed {
		t.Fatal("fixture is vacuous for the NULL-status case: every run landed in a status bucket, " +
			"so a bare-argMax regression (which would resolve the NULL winner to a stale non-NULL status) " +
			"could not be detected")
	}
	// TOTAL-ORDER both sides before comparing index-by-index.
	//
	// PipelineAccumulator.Finish sorts by (teamID, serviceValue), but a NIL
	// service and a non-nil EMPTY-STRING service both map to serviceValue "" --
	// and this fixture deliberately contains both (see the >=2 groups guard
	// above). So that key is NOT TOTAL for this data, sort.SliceStable falls
	// back to INSERTION order for the tie, and insertion order is the order rows
	// arrived from the loader. The two loaders read different queries, so their
	// arrival orders need not agree.
	//
	// This made the differential silently ORDER-DEPENDENT: it passed only while
	// both loaders happened to surface the tied groups in the same sequence.
	// Adding duplicate rows to the fixtures changed the physical layout, changed
	// the raw loader's arrival order, and flipped the tie -- surfacing a latent
	// defect rather than introducing one. Sorting both sides by a key that
	// SEPARATES nil from "" removes the dependency entirely.
	sortPipelineMetricsForComparison(rawPipeline)
	sortPipelineMetricsForComparison(pushPipeline)
	for index := range rawPipeline {
		if !samePipelineMetric(rawPipeline[index], pushPipeline[index]) {
			t.Fatalf("pipeline row %d diverges:\nrowloader=%+v\npushdown =%+v", index, rawPipeline[index], pushPipeline[index])
		}
	}

	// ---------------- testops_test ----------------
	pushTestAccumulator := testops.NewTestAccumulator(repoID, repoID.String(), nil)
	if err := loadNativeTestopsSuites(ctx, conn, pushTestAccumulator, orgID, repoID, start, end); err != nil {
		t.Fatalf("pushdown suite read: %v", err)
	}
	if err := loadNativeTestopsCaseGroups(ctx, conn, pushTestAccumulator, orgID, repoID, start, end); err != nil {
		t.Fatalf("pushdown case-group read: %v", err)
	}
	pushHistorical, err := loadNativeHistoricalFailedCaseNames(ctx, conn, orgID, repoID, historyStart, start, end)
	if err != nil {
		t.Fatalf("pushdown historical read: %v", err)
	}
	pushTest := pushTestAccumulator.Finish(pushHistorical)

	rawSuites, rawCases, err := loadTestopsSuiteAndCaseRows(ctx, conn, orgID, repoID, start, end)
	if err != nil {
		t.Fatalf("row-loader suite/case read: %v", err)
	}
	rawHistorical, err := loadHistoricalFailedCaseNames(ctx, conn, orgID, repoID, historyStart, start, end)
	if err != nil {
		t.Fatalf("row-loader historical read: %v", err)
	}
	rawTest := testops.ComputeTestMetrics(repoID, rawSuites, rawCases, rawHistorical, repoID.String(), nil)

	// The duplicate rows must actually reach the row loader, or the argMax
	// dedup was never under test: the FINAL-based loader collapses them, so
	// its case count is the DISTINCT count while the table holds 5x that.
	assertRowCountAtLeast(ctx, t, conn, "test_case_results", 15)

	if len(pushTest) != len(rawTest) {
		t.Fatalf("test row count: pushdown=%d rowloader=%d", len(pushTest), len(rawTest))
	}
	if len(rawTest) != 1 {
		t.Fatalf("expected exactly 1 testops_test row, got %d", len(rawTest))
	}
	if !sameTestMetric(rawTest[0], pushTest[0]) {
		t.Fatalf("test row diverges:\nrowloader=%+v\npushdown =%+v", rawTest[0], pushTest[0])
	}
	if rawTest[0].FlakeRate == 0 || rawTest[0].RetryDependencyRate == 0 || rawTest[0].FailureRecurrence == 0 {
		t.Fatalf("fixture is vacuous: flake/retry/recurrence must all be nonzero, got %+v", rawTest[0])
	}

	// ---------------- testops_coverage ----------------
	pushCurrent, err := loadNativeTestopsLatestCoverage(ctx, conn, orgID, repoID, start, end)
	if err != nil {
		t.Fatalf("pushdown coverage read: %v", err)
	}
	pushPrior, err := loadNativeTestopsLatestCoverage(ctx, conn, orgID, repoID, priorStart, start)
	if err != nil {
		t.Fatalf("pushdown prior-coverage read: %v", err)
	}
	pushCoverage := testops.ComputeCoverageMetric(repoID, pushCurrent, pushPrior, repoID.String(), nil)

	rawCurrent, err := loadTestopsCoverageSnapshots(ctx, conn, orgID, repoID, start, end)
	if err != nil {
		t.Fatalf("row-loader coverage read: %v", err)
	}
	rawPrior, err := loadTestopsCoverageSnapshots(ctx, conn, orgID, repoID, priorStart, start)
	if err != nil {
		t.Fatalf("row-loader prior-coverage read: %v", err)
	}
	rawCoverage := testops.ComputeCoverageMetric(repoID, rawCurrent, rawPrior, repoID.String(), nil)

	if len(rawCurrent) < 2 {
		t.Fatalf("fixture is vacuous: expected >=2 current coverage snapshots so the (run_id, snapshot_id) tie-break runs, got %d", len(rawCurrent))
	}
	if rawCoverage == nil || pushCoverage == nil {
		t.Fatalf("coverage: rowloader=%v pushdown=%v, both must be non-nil", rawCoverage, pushCoverage)
	}
	if !sameCoverageMetric(*rawCoverage, *pushCoverage) {
		t.Fatalf("coverage row diverges:\nrowloader=%+v\npushdown =%+v", *rawCoverage, *pushCoverage)
	}
	if rawCoverage.CoverageDeltaPct == nil {
		t.Fatal("fixture is vacuous: coverage_delta_pct must be exercised (prior snapshot missing?)")
	}
}

// TestNativeTestopsExecutorsWriteTheirTablesAgainstRealClickHouse drives the
// three executors through the SAME entry point PartitionHandler uses
// (ComputeFamily), so it also covers the scope/validation path, the
// LoadWellbeingTeams call every family makes, and the ClickHouse batch
// writers -- none of which the differential above touches.
func TestNativeTestopsExecutorsWriteTheirTablesAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range testopsDifferentialSchema() {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const orgID = "00000000-0000-4000-8000-000000000009"
	repoID := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	day := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	seedTestopsDifferentialFixture(ctx, t, conn, orgID, repoID, day)

	run := Run{ID: "run-1", OrganizationID: orgID, TargetDay: day}
	partition := Partition{ID: "partition-1", RunID: "run-1", RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}

	pipelineExecutor, err := NewTestopsPipelineExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	testExecutor, err := NewTestopsTestExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	coverageExecutor, err := NewTestopsCoverageExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}

	for _, spec := range []struct {
		name     string
		executor NativeFamilyExecutor
		table    string
	}{
		{"testops_pipeline", pipelineExecutor, "testops_pipeline_metrics_daily"},
		{"testops_test", testExecutor, "testops_test_metrics_daily"},
		{"testops_coverage", coverageExecutor, "testops_coverage_metrics_daily"},
	} {
		written, err := spec.executor.ComputeFamily(ctx, run, partition)
		if err != nil {
			t.Fatalf("%s ComputeFamily: %v", spec.name, err)
		}
		if written == 0 {
			t.Fatalf("%s reported 0 rows written; the fixture has data for it", spec.name)
		}
		// The reported count is what feeds ObserveDailyMetricsNativeFamily's
		// rows argument (daily.go:598), so a count that does not match the
		// table would make the telemetry lie even while the write succeeded.
		var stored uint64
		if err := conn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM %s WHERE org_id = ? AND repo_id = ?", spec.table),
			orgID, repoID,
		).Scan(&stored); err != nil {
			t.Fatalf("%s readback: %v", spec.name, err)
		}
		if stored != uint64(written) {
			t.Fatalf("%s wrote %d rows but reported %d", spec.name, stored, written)
		}
	}
}

// testopsDifferentialSchema is the DDL both tests above use. Kept as one
// function so the two can never drift into testing different table shapes.
func testopsDifferentialSchema() []string {
	return []string{
		`CREATE TABLE teams (
    id String, name String, members Array(String), repo_patterns Array(String), org_id String
) ENGINE = ReplacingMergeTree ORDER BY (id)`,
		`CREATE TABLE ci_pipeline_runs (
    repo_id UUID, run_id String, status Nullable(String),
    queued_at Nullable(DateTime64(3, 'UTC')), started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')), last_synced DateTime64(3, 'UTC'),
    pipeline_name Nullable(String), provider LowCardinality(String) DEFAULT '',
    duration_seconds Nullable(Float64), queue_seconds Nullable(Float64),
    retry_count UInt32 DEFAULT 0, cancel_reason Nullable(String), trigger_source Nullable(String),
    commit_hash Nullable(String), branch Nullable(String), pr_number Nullable(UInt32),
    team_id Nullable(String), service_id Nullable(String), org_id LowCardinality(String) DEFAULT ''
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, run_id)`,
		`CREATE TABLE test_suite_results (
    repo_id UUID, run_id String, suite_id String, suite_name String,
    framework Nullable(String), environment Nullable(String),
    total_count UInt32, passed_count UInt32, failed_count UInt32, skipped_count UInt32,
    error_count UInt32 DEFAULT 0, quarantined_count UInt32 DEFAULT 0, retried_count UInt32 DEFAULT 0,
    duration_seconds Nullable(Float64), started_at Nullable(DateTime64(3, 'UTC')),
    finished_at Nullable(DateTime64(3, 'UTC')), team_id Nullable(String), service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '', last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, run_id, suite_id)`,
		`CREATE TABLE test_case_results (
    repo_id UUID, run_id String, suite_id String, case_id String, case_name String,
    class_name Nullable(String), status LowCardinality(String), duration_seconds Nullable(Float64),
    retry_attempt UInt32 DEFAULT 0, failure_message Nullable(String), failure_type Nullable(String),
    stack_trace Nullable(String), is_quarantined UInt8 DEFAULT 0,
    org_id LowCardinality(String) DEFAULT '', last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, run_id, suite_id, case_id)`,
		`CREATE TABLE coverage_snapshots (
    repo_id UUID, run_id String, snapshot_id String, report_format Nullable(String),
    lines_total Nullable(UInt32), lines_covered Nullable(UInt32), line_coverage_pct Nullable(Float64),
    branches_total Nullable(UInt32), branches_covered Nullable(UInt32), branch_coverage_pct Nullable(Float64),
    functions_total Nullable(UInt32), functions_covered Nullable(UInt32),
    commit_hash Nullable(String), branch Nullable(String), pr_number Nullable(UInt32),
    team_id Nullable(String), service_id Nullable(String), org_id LowCardinality(String) DEFAULT '',
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, run_id, snapshot_id)`,
		`CREATE TABLE testops_pipeline_metrics_daily (
    repo_id UUID, day Date, pipelines_count UInt32, success_count UInt32, failure_count UInt32,
    cancelled_count UInt32, success_rate Float64, failure_rate Float64, cancel_rate Float64,
    rerun_rate Float64, median_duration_seconds Nullable(Float64), p95_duration_seconds Nullable(Float64),
    avg_queue_seconds Nullable(Float64), p95_queue_seconds Nullable(Float64),
    team_id Nullable(String), service_id Nullable(String), org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
		`CREATE TABLE testops_test_metrics_daily (
    repo_id UUID, day Date, total_cases UInt32, passed_count UInt32, failed_count UInt32,
    skipped_count UInt32, quarantined_count UInt32, pass_rate Float64, failure_rate Float64,
    flake_rate Float64, retry_dependency_rate Float64, total_suites UInt32,
    suite_duration_p50_seconds Nullable(Float64), suite_duration_p95_seconds Nullable(Float64),
    failure_recurrence_score Float64, team_id Nullable(String), service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
		`CREATE TABLE testops_coverage_metrics_daily (
    repo_id UUID, day Date, line_coverage_pct Nullable(Float64), branch_coverage_pct Nullable(Float64),
    lines_total Nullable(UInt32), lines_covered Nullable(UInt32), coverage_delta_pct Nullable(Float64),
    uncovered_files_count UInt32, coverage_regression_count UInt32,
    team_id Nullable(String), service_id Nullable(String), org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
	}
}

// seedTestopsDifferentialFixture writes the hostile corpus described in this
// file's header comment.
func seedTestopsDifferentialFixture(
	ctx context.Context, t *testing.T, conn driver.Conn,
	orgID string, repoID uuid.UUID, day time.Time,
) {
	t.Helper()
	inDay := day.Add(9 * time.Hour)
	nextDay := day.Add(30 * time.Hour)
	priorDay := day.AddDate(0, 0, -3).Add(9 * time.Hour)
	historyDay := day.AddDate(0, 0, -10).Add(9 * time.Hour)
	synced := day.Add(23 * time.Hour)

	// --- ci_pipeline_runs: three groups (nil service, "" service, "svc-1"),
	// plus a superseded duplicate whose stale status would flip
	// success_count if the dedup were dropped.
	type pipelineSeed struct {
		runID string
		// status is a POINTER so a seed can carry a genuine SQL NULL. r1 (P3)
		// found the fixture could not detect a bare-argMax regression: every
		// seeded status was non-NULL, and ClickHouse's argMax SKIPS rows whose
		// arg is NULL -- so `argMax(status, last_synced)` and
		// `argMax((status, ...), last_synced)` agree on all-non-NULL data and
		// disagree only when the WINNER's status is NULL. Without a NULL winner
		// the test passed either implementation, i.e. it was vacuous for exactly
		// the invariant its own comment claims to protect.
		status    *string
		service   *string
		queue     float64
		retry     uint32
		started   time.Time
		lastSync  time.Time
		duration  float64
		wantStale bool
	}
	emptyService := ""
	namedService := "svc-1"
	statusSuccess := "success"
	statusSuccessPadded := "  SUCCESS  "
	statusFailed := "failed"
	statusCancelled := "cancelled"
	for _, seed := range []pipelineSeed{
		{runID: "run-1", status: &statusSuccess, service: nil, queue: 0.1, started: inDay, lastSync: synced, duration: 100},
		{runID: "run-2", status: &statusSuccessPadded, service: nil, queue: 0.1, started: inDay, lastSync: synced, duration: 200},
		{runID: "run-3", status: &statusFailed, service: &emptyService, queue: 0.1, started: inDay, lastSync: synced, duration: 300},
		{runID: "run-4", status: &statusCancelled, service: &namedService, queue: 0.1, retry: 3, started: inDay, lastSync: synced, duration: 400},
		// Superseded copy of run-1: OLDER last_synced, contradictory status.
		// argMax and FINAL must both discard it.
		{runID: "run-1", status: &statusFailed, service: nil, queue: 99, started: inDay, lastSync: synced.Add(-2 * time.Hour), duration: 1, wantStale: true},
		// TIE FIXTURE (2026-09-04 fleet ruling): two rows, SAME sorting key
		// (org_id, repo_id, run_id), IDENTICAL last_synced, DIFFERENT payload.
		// This is the case the readers' dedup must resolve deterministically,
		// and the case the previous argMax form could NOT: argMax may return
		// either row on a version tie, FINAL returns the last-inserted one.
		//
		// The old fixture gave every row a distinct last_synced, so it could
		// never exhibit this at all -- a green result said nothing about the
		// class, which is exactly the "negative repro that could not have shown
		// the effect" trap. assertTieFixtureActuallyTies below is the positive
		// control proving the tie is really present in the table.
		{runID: "run-tie", status: &statusSuccess, service: nil, queue: 0.1, started: inDay, lastSync: synced, duration: 700},
		{runID: "run-tie", status: &statusFailed, service: nil, queue: 0.1, started: inDay, lastSync: synced, duration: 700, wantStale: true},
		// r1 P3: NULL-status WINNER over a superseded non-NULL version. This is
		// the ONLY shape that separates tuple-argMax from bare argMax: the tuple
		// preserves the winner's NULL (FINAL semantics, since RMT keeps the whole
		// row), whereas `argMax(status, last_synced)` SKIPS the NULL arg and
		// returns the older "failed". Both readers under differential must agree
		// that this run's status is NULL, i.e. it counts toward pipelines_count
		// but toward NO status bucket.
		{runID: "run-null", status: nil, service: nil, queue: 0.1, started: inDay, lastSync: synced, duration: 600},
		{runID: "run-null", status: &statusFailed, service: nil, queue: 0.1, started: inDay, lastSync: synced.Add(-3 * time.Hour), duration: 600, wantStale: true},
		// Prior-window run, so coverage's prior snapshot has a run to join.
		{runID: "run-prior", status: &statusSuccess, service: nil, queue: 0.1, started: priorDay, lastSync: synced, duration: 500},
	} {
		if err := conn.Exec(ctx, `INSERT INTO ci_pipeline_runs
(repo_id, run_id, status, queued_at, started_at, finished_at, last_synced,
 duration_seconds, queue_seconds, retry_count, team_id, service_id, org_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			repoID, seed.runID, seed.status,
			seed.started, seed.started, seed.started.Add(time.Duration(seed.duration)*time.Second),
			seed.lastSync, seed.duration, seed.queue, seed.retry,
			nil, seed.service, orgID,
		); err != nil {
			t.Fatalf("seed ci_pipeline_runs %s: %v", seed.runID, err)
		}
	}

	// --- test_suite_results: today's suite, a NEXT-DAY suite sharing run-1's
	// id (the day-boundary guard), and a historical suite for recurrence.
	// Every suite row is seeded THREE times: a stale version, a TIE-loser
	// sharing the winner's last_synced, and the winner LAST. Insertion order is
	// load-bearing -- ReplacingMergeTree resolves a version tie to the
	// last-inserted row, which is the determinism the FINAL revert relies on.
	//
	// The losers carry a deliberately wrong passed_count. After FINAL the table
	// is byte-identical to the single-row fixture this replaced, so every
	// pre-existing assertion still holds; WITHOUT FINAL the wrong counts leak.
	type suiteSeed struct {
		runID, suiteID string
		started        time.Time
	}
	for _, seed := range []suiteSeed{
		{runID: "run-1", suiteID: "suite-1", started: inDay},
		{runID: "run-1", suiteID: "suite-next-day", started: nextDay},
		{runID: "run-hist", suiteID: "suite-hist", started: historyDay},
	} {
		for _, v := range []struct {
			lastSync time.Time
			passed   uint32
		}{
			{synced.Add(-2 * time.Hour), 99}, // stale
			{synced, 98},                     // TIE-loser: identical last_synced
			{synced, 3},                      // winner, inserted LAST
		} {
			if err := conn.Exec(ctx, `INSERT INTO test_suite_results
(repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count,
 error_count, quarantined_count, duration_seconds, started_at, finished_at,
 team_id, service_id, org_id, last_synced)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				repoID, seed.runID, seed.suiteID, "suite", uint32(6), v.passed, uint32(2), uint32(1),
				uint32(1), uint32(1), 12.5, seed.started, seed.started.Add(time.Minute),
				nil, nil, orgID, v.lastSync,
			); err != nil {
				t.Fatalf("seed test_suite_results %s/%s: %v", seed.runID, seed.suiteID, err)
			}
		}
	}

	// --- test_case_results: FIVE copies of every case row, ascending
	// last_synced, with the OLDER copies carrying contradictory statuses.
	// This is the CHAOS-5045 duplicate shape.
	type caseSeed struct {
		runID, suiteID, caseID, caseName string
		winnerStatus                     string
		staleStatus                      string
		winnerRetry                      uint32
	}
	caseSeeds := []caseSeed{
		// "flaky" fails on attempt 0 and passes on a retry -> flake + retry-dependent.
		{runID: "run-1", suiteID: "suite-1", caseID: "c1", caseName: "flaky", winnerStatus: "FAILED", staleStatus: "passed"},
		{runID: "run-1", suiteID: "suite-1", caseID: "c2", caseName: "flaky", winnerStatus: " Passed ", staleStatus: "failed", winnerRetry: 2},
		// "recurring" fails today AND in the history window -> recurrence.
		{runID: "run-1", suiteID: "suite-1", caseID: "c3", caseName: "recurring", winnerStatus: "timed_out", staleStatus: "passed"},
		{runID: "run-1", suiteID: "suite-1", caseID: "c4", caseName: "clean", winnerStatus: "succeeded", staleStatus: "failed"},
		// Next-day suite: excluded by the day-boundary guard. Its status
		// would add a "passed" to "recurring" and destroy the recurrence
		// score if the guard were dropped.
		{runID: "run-1", suiteID: "suite-next-day", caseID: "c5", caseName: "recurring", winnerStatus: "passed", staleStatus: "passed"},
		// Historical failure for "recurring".
		{runID: "run-hist", suiteID: "suite-hist", caseID: "c6", caseName: "recurring", winnerStatus: "error", staleStatus: "passed"},
	}
	// SIX copies now, not five. Copies 0-3 are the ascending-last_synced stale
	// shape (CHAOS-5045). Copy 4 is a TIE-LOSER carrying the winner's exact
	// last_synced with a contradictory status, and copy 5 is the winner,
	// inserted LAST so the tie resolves to it. Without copy 4 the table had
	// duplicates but no TIE, so the version-tie path was never exercised here.
	for _, seed := range caseSeeds {
		for copyIndex := 0; copyIndex < 6; copyIndex++ {
			status := seed.staleStatus
			retry := uint32(0)
			lastSync := synced.Add(-time.Duration(6-copyIndex) * time.Hour)
			if copyIndex == 4 { // TIE-loser: winner's version, wrong status
				lastSync = synced
			}
			if copyIndex == 5 { // newest copy wins, inserted LAST
				status = seed.winnerStatus
				retry = seed.winnerRetry
				lastSync = synced
			}
			if err := conn.Exec(ctx, `INSERT INTO test_case_results
(repo_id, run_id, suite_id, case_id, case_name, status, retry_attempt, org_id, last_synced)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				repoID, seed.runID, seed.suiteID, seed.caseID, seed.caseName,
				status, retry, orgID, lastSync,
			); err != nil {
				t.Fatalf("seed test_case_results %s copy %d: %v", seed.caseID, copyIndex, err)
			}
		}
	}

	// --- coverage_snapshots: two snapshots under run-1 so the
	// (run_id, snapshot_id) lexical tie-break decides, plus one in the prior
	// window so coverage_delta_pct is non-nil.
	type coverageSeed struct {
		runID, snapshotID string
		linePct           float64
		linesTotal        uint32
		linesCovered      uint32
	}
	// Same three-version shape as test_suite_results: stale, TIE-loser sharing
	// the winner's last_synced, winner LAST. Losers carry a wrong coverage pct.
	for _, seed := range []coverageSeed{
		{runID: "run-1", snapshotID: "snap-a", linePct: 70.0, linesTotal: 1000, linesCovered: 700},
		{runID: "run-1", snapshotID: "snap-b", linePct: 81.5, linesTotal: 1000, linesCovered: 815},
		{runID: "run-prior", snapshotID: "snap-p", linePct: 61.5, linesTotal: 900, linesCovered: 553},
	} {
		for _, v := range []struct {
			lastSync time.Time
			pct      float64
		}{
			{synced.Add(-2 * time.Hour), 1.5}, // stale
			{synced, 2.5},                     // TIE-loser
			{synced, seed.linePct},            // winner, inserted LAST
		} {
			if err := conn.Exec(ctx, `INSERT INTO coverage_snapshots
(repo_id, run_id, snapshot_id, lines_total, lines_covered, line_coverage_pct, branch_coverage_pct,
 team_id, service_id, org_id, last_synced)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				repoID, seed.runID, seed.snapshotID, seed.linesTotal, seed.linesCovered,
				v.pct, v.pct-5.0, nil, nil, orgID, v.lastSync,
			); err != nil {
				t.Fatalf("seed coverage_snapshots %s/%s: %v", seed.runID, seed.snapshotID, err)
			}
		}
	}
}

func assertRowCountAtLeast(ctx context.Context, t *testing.T, conn driver.Conn, table string, want uint64) {
	t.Helper()
	var got uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got < want {
		t.Fatalf("%s holds %d rows, expected >=%d -- the duplicate fixture did not land, so the dedup was never under test", table, got, want)
	}
}

// ----------------------------------------------------------------------
// Field-wise comparators. A struct `!=` would compare the *float64 fields by
// ADDRESS; floats are compared by BIT PATTERN so a divergence in the last ULP
// (the FMA / Neumaier class this port must not introduce) cannot slip through
// as "close enough".
// ----------------------------------------------------------------------

func samePipelineMetric(a, b testops.PipelineMetric) bool {
	return a.RepoID == b.RepoID && a.OrgID == b.OrgID &&
		a.PipelinesCount == b.PipelinesCount && a.SuccessCount == b.SuccessCount &&
		a.FailureCount == b.FailureCount && a.CancelledCount == b.CancelledCount &&
		sameFloat(a.SuccessRate, b.SuccessRate) && sameFloat(a.FailureRate, b.FailureRate) &&
		sameFloat(a.CancelRate, b.CancelRate) && sameFloat(a.RerunRate, b.RerunRate) &&
		sameFloatPtr(a.MedianDurationSeconds, b.MedianDurationSeconds) &&
		sameFloatPtr(a.P95DurationSeconds, b.P95DurationSeconds) &&
		sameFloatPtr(a.AvgQueueSeconds, b.AvgQueueSeconds) &&
		sameFloatPtr(a.P95QueueSeconds, b.P95QueueSeconds) &&
		sameStrPtr(a.TeamID, b.TeamID) && sameStrPtr(a.ServiceID, b.ServiceID)
}

func sameTestMetric(a, b testops.TestMetric) bool {
	return a.RepoID == b.RepoID && a.OrgID == b.OrgID &&
		a.TotalCases == b.TotalCases && a.PassedCount == b.PassedCount &&
		a.FailedCount == b.FailedCount && a.SkippedCount == b.SkippedCount &&
		a.QuarantinedCount == b.QuarantinedCount && a.TotalSuites == b.TotalSuites &&
		sameFloat(a.PassRate, b.PassRate) && sameFloat(a.FailureRate, b.FailureRate) &&
		sameFloat(a.FlakeRate, b.FlakeRate) && sameFloat(a.RetryDependencyRate, b.RetryDependencyRate) &&
		sameFloatPtr(a.SuiteDurationP50Seconds, b.SuiteDurationP50Seconds) &&
		sameFloatPtr(a.SuiteDurationP95Seconds, b.SuiteDurationP95Seconds) &&
		sameFloat(a.FailureRecurrence, b.FailureRecurrence) &&
		sameStrPtr(a.TeamID, b.TeamID) && sameStrPtr(a.ServiceID, b.ServiceID)
}

func sameCoverageMetric(a, b testops.CoverageMetric) bool {
	return a.RepoID == b.RepoID && a.OrgID == b.OrgID &&
		sameFloatPtr(a.LineCoveragePct, b.LineCoveragePct) &&
		sameFloatPtr(a.BranchCoveragePct, b.BranchCoveragePct) &&
		sameUintPtr(a.LinesTotal, b.LinesTotal) && sameUintPtr(a.LinesCovered, b.LinesCovered) &&
		sameFloatPtr(a.CoverageDeltaPct, b.CoverageDeltaPct) &&
		a.UncoveredFilesCount == b.UncoveredFilesCount &&
		a.CoverageRegressionCount == b.CoverageRegressionCount &&
		sameStrPtr(a.TeamID, b.TeamID) && sameStrPtr(a.ServiceID, b.ServiceID)
}

func sameFloat(a, b float64) bool { return math.Float64bits(a) == math.Float64bits(b) }

func sameFloatPtr(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return sameFloat(*a, *b)
}

func sameUintPtr(a, b *uint32) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameStrPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// sortPipelineMetricsForComparison imposes a TOTAL order on pipeline metrics.
//
// nilServiceSentinel/setServicePrefix keep a NIL pointer and a pointer to the
// EMPTY STRING in distinct buckets. Collapsing them with a plain
// `if p != nil { v = *p }` would reproduce the exact ambiguity this exists to
// remove, since *p may itself be "".
func sortPipelineMetricsForComparison(metrics []testops.PipelineMetric) {
	key := func(m testops.PipelineMetric) string {
		return pipelineSortComponent(m.TeamID) + "\x1f" + pipelineSortComponent(m.ServiceID)
	}
	sort.SliceStable(metrics, func(i, j int) bool { return key(metrics[i]) < key(metrics[j]) })
}

// pipelineSortComponent renders a *string so that nil and "" never collide.
func pipelineSortComponent(p *string) string {
	if p == nil {
		return "\x00" // nil sorts before every set value, including ""
	}
	return "\x01" + *p
}

// assertTieFixtureActuallyTies is the POSITIVE CONTROL for the tie fixture,
// PARAMETRISED BY TABLE.
//
// A test that asserts "the readers handle a version tie" proves nothing if the
// table never actually contains one. It must be applied to EVERY table whose
// dedup the test claims to cover, not just one:
//
// This control was originally ci_pipeline_runs-only, and an executed mutation
// proved what that cost. test_suite_results and coverage_snapshots had no
// duplicate rows at all, so FINAL was a NO-OP on them -- deleting FINAL from
// their five reads (of the ten in this PR) left the entire suite GREEN. The
// differential could not detect the removal of half of what it exists to test.
// A per-table control is the fix: a table with no tie now fails loudly here,
// rather than silently making its assertions vacuous.
//
// It queries WITHOUT FINAL on purpose. FINAL would collapse the duplicate and
// report one row, which is precisely the state this control exists to rule out.
func assertTieFixtureActuallyTies(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID string, repoID uuid.UUID,
	table, keyCols string,
) {
	t.Helper()
	var tied uint64
	query := fmt.Sprintf(`
SELECT count() FROM (
  SELECT %s, last_synced, count() AS n
  FROM %s
  WHERE org_id = ? AND repo_id = ?
  GROUP BY %s, last_synced
  HAVING n > 1
)`, keyCols, table, keyCols)
	if err := conn.QueryRow(ctx, query, orgID, repoID).Scan(&tied); err != nil {
		t.Fatalf("tie-fixture positive control for %s: %v", table, err)
	}
	if tied == 0 {
		t.Fatalf("tie fixture is absent for %s: no (%s, last_synced) has more than one physical "+
			"row, so nothing in this test exercises version-tie resolution for that table and a "+
			"green result would prove nothing about it", table, keyCols)
	}
}
