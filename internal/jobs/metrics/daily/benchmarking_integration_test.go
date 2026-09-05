//go:build integration

package daily

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// benchmarkingSchema is the production shape of the tables this family touches:
// `repos` (for the org anchor), one source metric table, and ALL SIX output
// tables, copied from migration 031_testops_benchmarking.sql.
//
// All six, not just the two the assertions read. The first version created only
// those two, reasoning that "the other four share the same writer path and are
// covered by the compute-side oracle". Both halves of that are true and the
// conclusion still does not follow: the oracle covers the VALUES those tables
// receive, but the writer prepares one batch PER TABLE and aborts on the first
// that does not exist --
//
//	prepare testops_period_comparisons batch: code: 60,
//	message: Table worker_test.testops_period_comparisons does not exist
//
// so the run died before reaching any assertion. The question a write-path
// fixture must answer is which tables the WRITER WRITES, never which tables the
// assertions read.
//
// Note these six carry org_id as a plain column with DEFAULT ” and do NOT have
// it in their sorting keys: migration 031 created them AFTER 027's org_id-first
// sweep, so they were never part of it. That is production's real shape and is
// reproduced faithfully rather than corrected here. It is safe today only
// because all six are plain MergeTree (no dedup collapse) and the loader filters
// org_id explicitly.
var benchmarkingSchema = []string{
	`CREATE TABLE repos (
    id UUID, org_id String, repo String, settings String, provider String,
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, id)`,
	`CREATE TABLE testops_pipeline_metrics_daily (
    repo_id UUID, day Date, team_id Nullable(String), service_id Nullable(String),
    success_rate Float64, failure_rate Float64, rerun_rate Float64,
    median_duration_seconds Float64, p95_duration_seconds Float64,
    avg_queue_seconds Float64, p95_queue_seconds Float64,
    computed_at DateTime('UTC'), org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)`,
	`CREATE TABLE testops_metric_baselines (
    metric_name LowCardinality(String), scope_type LowCardinality(String), scope_key String,
    period_start Date, period_end Date, rolling_window_days UInt16,
    current_value Float64, baseline_value Float64, percentile_rank Float64,
    p25_value Float64, p50_value Float64, p75_value Float64, p90_value Float64,
    sample_size UInt32, org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(period_end)
  ORDER BY (metric_name, scope_type, scope_key, period_end, rolling_window_days)`,
	`CREATE TABLE testops_maturity_bands (
    metric_name LowCardinality(String), scope_type LowCardinality(String), scope_key String,
    period_start Date, period_end Date, value Float64, percentile_rank Float64,
    maturity_band LowCardinality(String), confidence Float64,
    org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(period_end)
  ORDER BY (metric_name, scope_type, scope_key, period_end)`,
	`CREATE TABLE testops_period_comparisons (
    metric_name LowCardinality(String), scope_type LowCardinality(String), scope_key String,
    current_period_start Date, current_period_end Date,
    comparison_period_start Date, comparison_period_end Date,
    current_value Float64, comparison_value Float64, absolute_delta Float64,
    percentage_change Nullable(Float64), trend_direction LowCardinality(String),
    org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(current_period_end)
  ORDER BY (metric_name, scope_type, scope_key, current_period_end, comparison_period_end)`,
	`CREATE TABLE testops_metric_anomalies (
    metric_name LowCardinality(String), scope_type LowCardinality(String), scope_key String,
    day Date, value Float64, baseline_value Float64, z_score Float64,
    anomaly_type LowCardinality(String), direction LowCardinality(String),
    severity LowCardinality(String), volatility_score Float64,
    org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(day)
  ORDER BY (metric_name, scope_type, scope_key, day, anomaly_type)`,
	`CREATE TABLE testops_metric_correlations (
    metric_name LowCardinality(String), paired_metric_name LowCardinality(String),
    scope_type LowCardinality(String), scope_key String,
    period_start Date, period_end Date, coefficient Float64, p_value Float64,
    sample_size UInt32, is_significant UInt8, interpretation String,
    org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(period_end)
  ORDER BY (metric_name, paired_metric_name, scope_type, scope_key, period_end)`,
	`CREATE TABLE testops_benchmark_insights (
    insight_id String, insight_type LowCardinality(String),
    scope_type LowCardinality(String), scope_key String,
    metric_name LowCardinality(String), paired_metric_name Nullable(String),
    period_start Nullable(Date), period_end Nullable(Date),
    severity LowCardinality(String), summary String,
    evidence_json String DEFAULT '{}',
    org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(coalesce(period_end, toDate(computed_at)))
  ORDER BY (insight_id, computed_at)`,
}

// TestBenchmarkingFinalizeRunsOnlyAfterEveryPartitionSucceeded is CHAOS-5194's
// real-ClickHouse race test (team-lead's design ruling, point 5): the
// once-per-org duplication proof this test replaces
// (TestBenchmarkingComputesOncePerOrgNotOncePerPartition, removed --
// finalize scope runs exactly once per RUN by construction, so there is no
// anchor mechanism left to prove) is superseded by a STRONGER proof this
// family's move to finalize scope was actually FOR: that it cannot compute
// AT ALL while any partition is still open, against REAL ClickHouse writes,
// not a mocked short-circuit.
//
// RED: with the org's partition barrier reporting 2 of 3 succeeded, the
// executor must refuse and leave the output tables EMPTY.
// GREEN: the SAME executor, SAME run, SAME ClickHouse data, with the barrier
// now reporting 3 of 3 succeeded, must compute and write real rows.
//
// The fakeStore stands in for Postgres here deliberately: this test's job is
// to prove the EXECUTOR honours whatever the barrier reports against REAL
// ClickHouse output, not to re-prove ClaimFinalize's own Postgres query
// (that has its own coverage, postgres.go). A pure-Go unit test already
// proves the barrier check fires before any ClickHouse read at all
// (benchmarking_finalize_native_executor_test.go); this test is the
// complementary proof that once past the barrier, real computation and real
// writes actually happen.
func TestBenchmarkingFinalizeRunsOnlyAfterEveryPartitionSucceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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

	for _, statement := range benchmarkingSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	repos := seedBenchmarkingOrg(t, ctx, conn)
	run := benchmarkingRun("run-race", repos)
	org := run.OrganizationID

	// ---- RED: 2 of 3 partitions succeeded ----------------------------------
	redStore := &fakeStore{partitionTotal: 3, partitionSucceeded: 2}
	redExecutor, err := NewBenchmarkingFinalizeExecutor(redStore, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	redRows, err := redExecutor.ComputeFinalizeFamily(ctx, run)
	if err == nil {
		t.Fatal("RED: ComputeFinalizeFamily succeeded with a partition still open (2/3) -- " +
			"the barrier did not hold, which is exactly the race this family's finalize-scope " +
			"move exists to prevent")
	}
	if !errors.Is(err, ErrBenchmarkingPartitionsIncomplete) {
		t.Errorf("RED: error does not wrap ErrBenchmarkingPartitionsIncomplete: %v", err)
	}
	if redRows != 0 {
		t.Errorf("RED: reported %d rows, want 0", redRows)
	}
	var redBaselineRows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM testops_metric_baselines WHERE org_id = ?`, org,
	).Scan(&redBaselineRows); err != nil {
		t.Fatal(err)
	}
	if redBaselineRows != 0 {
		t.Fatalf("RED: %d row(s) landed in testops_metric_baselines despite the refusal -- "+
			"the barrier check ran too late, after some write already happened", redBaselineRows)
	}

	// ---- GREEN: 3 of 3 partitions succeeded --------------------------------
	greenStore := &fakeStore{partitionTotal: 3, partitionSucceeded: 3}
	greenExecutor, err := NewBenchmarkingFinalizeExecutor(greenStore, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	greenRows, err := greenExecutor.ComputeFinalizeFamily(ctx, run)
	if err != nil {
		t.Fatalf("GREEN: ComputeFinalizeFamily failed with every partition succeeded (3/3): %v", err)
	}
	if greenRows == 0 {
		t.Fatal("GREEN: 0 rows reported with every partition succeeded -- expected real output")
	}

	var greenBaselineRows, distinctComputedAt uint64
	if err := conn.QueryRow(ctx,
		`SELECT count(), uniqExact(computed_at) FROM testops_metric_baselines WHERE org_id = ?`, org,
	).Scan(&greenBaselineRows, &distinctComputedAt); err != nil {
		t.Fatal(err)
	}
	if greenBaselineRows == 0 {
		t.Fatal("GREEN: no baseline rows written at all")
	}
	if distinctComputedAt != 1 {
		t.Errorf(
			"GREEN: testops_metric_baselines carries %d distinct computed_at values -- "+
				"expected exactly 1 (finalize scope runs exactly once per run)",
			distinctComputedAt,
		)
	}

	// Scope keys must be repo ids (scope_type 'repo'), which is what the
	// executed-proof readback's dead-id oracle cross-checks.
	var strayScopeKeys uint64
	if err := conn.QueryRow(ctx, `
SELECT count() FROM testops_metric_baselines
WHERE org_id = ? AND scope_type = 'repo'
  AND scope_key NOT IN (?, ?, ?)`, org, repos[0], repos[1], repos[2],
	).Scan(&strayScopeKeys); err != nil {
		t.Fatal(err)
	}
	if strayScopeKeys != 0 {
		t.Errorf("%d baseline row(s) carry a scope_key that is not one of the org's repos", strayScopeKeys)
	}

	// Maturity bands are derived from the baselines in the same call, so a
	// mismatch here means the two collections came from different runs.
	var bandRows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM testops_maturity_bands WHERE org_id = ?`, org,
	).Scan(&bandRows); err != nil {
		t.Fatal(err)
	}
	if bandRows != greenBaselineRows {
		t.Errorf("maturity_bands has %d rows but baselines has %d -- they are emitted 1:1", bandRows, greenBaselineRows)
	}
}

// benchmarkingWriteOrder mirrors the order of Writer.WriteOutputs' `steps`
// slice (benchmarking/clickhouse.go). A test copy of another function's
// ordering normally rots in silence, so it is not trusted on its own: the
// partial-write test below asserts the writer's own error text names the table
// AND its 1-based index within the six, so a reorder in WriteOutputs that this
// list did not follow fails loudly instead of quietly testing the wrong table.
var benchmarkingWriteOrder = []string{
	"testops_metric_baselines",
	"testops_maturity_bands",
	"testops_metric_anomalies",
	"testops_period_comparisons",
	"testops_metric_correlations",
	"testops_benchmark_insights",
}

// benchmarkingDDL returns the CREATE statement for one output table out of
// benchmarkingSchema, so a test can drop a table and put it back byte-identical
// rather than keeping a second, drifting copy of the DDL.
func benchmarkingDDL(t *testing.T, table string) string {
	t.Helper()
	for _, statement := range benchmarkingSchema {
		if strings.HasPrefix(statement, "CREATE TABLE "+table+" ") {
			return statement
		}
	}
	t.Fatalf("no CREATE statement for %q in benchmarkingSchema", table)
	return ""
}

const (
	benchmarkingOrgID     = "00000000-0000-4000-8000-0000000000a0"
	benchmarkingTargetDay = "2026-08-24"
)

// seedBenchmarkingOrg creates one org with THREE repos and twelve days of
// pipeline history for each, and returns the repo ids in sorted order -- so
// repos[0] is the lexicographically-first id, which is the anchor partition.
func seedBenchmarkingOrg(t *testing.T, ctx context.Context, conn driver.Conn) []string {
	t.Helper()
	repos := []string{
		"00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002",
		"00000000-0000-4000-8000-000000000003",
	}
	if err := conn.Exec(ctx, `
INSERT INTO repos (id, org_id, repo, settings, provider, last_synced) VALUES
(toUUID('`+repos[0]+`'), '`+benchmarkingOrgID+`', 'a/one',   '{}', 'github', '2026-08-24 00:00:00.000'),
(toUUID('`+repos[1]+`'), '`+benchmarkingOrgID+`', 'a/two',   '{}', 'github', '2026-08-24 00:00:00.000'),
(toUUID('`+repos[2]+`'), '`+benchmarkingOrgID+`', 'a/three', '{}', 'github', '2026-08-24 00:00:00.000')
`); err != nil {
		t.Fatal(err)
	}

	// Enough history for the 30-day baseline window across all three repos.
	for dayOffset := 0; dayOffset < 12; dayOffset++ {
		day := benchmarkingDay().AddDate(0, 0, -dayOffset)
		for index, repo := range repos {
			value := 0.5 + float64(index)*0.1 + float64(dayOffset)*0.01
			if err := conn.Exec(ctx, `
INSERT INTO testops_pipeline_metrics_daily
(repo_id, day, team_id, service_id, success_rate, failure_rate, rerun_rate,
 median_duration_seconds, p95_duration_seconds, avg_queue_seconds, p95_queue_seconds,
 computed_at, org_id) VALUES
(toUUID(?), ?, NULL, NULL, ?, 0.1, 0.05, 100.0, 200.0, 10.0, 20.0, ?, ?)`,
				repo, day, value, day, benchmarkingOrgID,
			); err != nil {
				t.Fatal(err)
			}
		}
	}
	return repos
}

func benchmarkingDay() time.Time {
	day, err := time.Parse("2006-01-02", benchmarkingTargetDay)
	if err != nil {
		panic(err)
	}
	return day.UTC()
}

// benchmarkingRun builds the Run the executor sees. DiscoveredRepoIDs is the
// union of the run's partition scopes and is REQUIRED: the anchor is chosen
// from it, and an empty set is a hard error by design (see
// anchorFromDiscoveredSet). Leaving it unset is what the first version of the
// once-per-org test did, which made every partition fail with ErrInvalidState.
func benchmarkingRun(id string, repos []string) Run {
	discovered := make([]RepositoryID, 0, len(repos))
	for _, repo := range repos {
		discovered = append(discovered, RepositoryID(repo))
	}
	return Run{
		ID:                id,
		OrganizationID:    benchmarkingOrgID,
		TargetDay:         benchmarkingDay(),
		DiscoveredRepoIDs: discovered,
	}
}

// benchmarkingCounts reads the row count of each named output table for one org.
func benchmarkingCounts(t *testing.T, ctx context.Context, conn driver.Conn, tables []string) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range tables {
		var rows uint64
		// The table name cannot be a bound parameter; it comes from this file's
		// own constant list, never from data.
		if err := conn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM %s WHERE org_id = ?", table), benchmarkingOrgID,
		).Scan(&rows); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		counts[table] = int(rows)
	}
	return counts
}

// TestBenchmarkingPartialWriteIsReportedWithItsTrueRowCount is the ONLY layer
// that can prove the partial-write contract, because the thing it has to
// establish is what is ON DISK after a mid-write failure. A fake writer can be
// made to return any (rows, error) pair the author likes; only a real
// ClickHouse can show that the earlier batches actually landed and that a
// fail-open re-drive would therefore duplicate them.
//
// It runs three phases against one container:
//
//	CONTROL   a healthy anchor run, to learn each output table's true row count.
//	FAULT     truncate all six, DROP the deepest table the control showed to be
//	          non-empty, re-run. The writer must fail ON that table with earlier
//	          batches already committed.
//	POSITIVE  put the dropped table back and re-run as a naive fail-open caller
//	          would. The earlier tables must now carry DOUBLE.
//
// The third phase is the one that makes the first two mean something. Without
// it this test would assert that an error has a particular wrapper on it and
// prove nothing about why that matters; with it, the duplication ErrPartialWrite
// exists to prevent is demonstrated on real tables rather than asserted in a
// comment.
//
// Which table is broken is DERIVED, not hardcoded. Hardcoding "the third" would
// silently become vacuous the day the fixture stops producing anomalies -- an
// empty collection is skipped without preparing a batch, so dropping its table
// causes no failure at all and every assertion below would pass on a run that
// never partially wrote anything.
func TestBenchmarkingPartialWriteIsReportedWithItsTrueRowCount(t *testing.T) {
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

	for _, statement := range benchmarkingSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	repos := seedBenchmarkingOrg(t, ctx, conn)
	// All-succeeded fake Store: this test's job is the partial-write/
	// ErrPartialWrite contract on real ClickHouse output, not the partition
	// barrier (covered by TestBenchmarkingFinalizeRunsOnlyAfterEveryPartitionSucceeded
	// and the pure-Go unit tests) -- so the barrier is held open throughout.
	store := &fakeStore{partitionTotal: 1, partitionSucceeded: 1}
	executor, err := NewBenchmarkingFinalizeExecutor(store, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	run := benchmarkingRun("run-partial", repos)

	// ---- CONTROL ----------------------------------------------------------
	controlRows, err := executor.ComputeFinalizeFamily(ctx, run)
	if err != nil {
		t.Fatalf("control run failed, so nothing below can be interpreted: %v", err)
	}
	control := benchmarkingCounts(t, ctx, conn, benchmarkingWriteOrder)
	controlTotal := 0
	for _, table := range benchmarkingWriteOrder {
		controlTotal += control[table]
	}
	if controlRows != controlTotal {
		t.Fatalf(
			"control run reported %d rows but %d are on disk across the six output tables %v -- "+
				"the reported count is not the written count even on the SUCCESS path",
			controlRows, controlTotal, control)
	}
	t.Logf("control row counts per table: %v (total %d)", control, controlTotal)

	// The deepest non-empty table: breaking it exercises the longest run of
	// successful writes this fixture can produce.
	breakIndex := -1
	for index, table := range benchmarkingWriteOrder {
		if control[table] > 0 {
			breakIndex = index
		}
	}
	if breakIndex < 1 {
		t.Fatalf(
			"the fixture produced non-empty output only up to index %d (%v) -- a partial write "+
				"needs at least one table to succeed BEFORE the one that fails, so this test "+
				"cannot express its own premise and must not report a pass",
			breakIndex, control)
	}
	breakTable := benchmarkingWriteOrder[breakIndex]
	before := benchmarkingWriteOrder[:breakIndex]
	after := benchmarkingWriteOrder[breakIndex+1:]

	expectedBefore := 0
	for _, table := range before {
		expectedBefore += control[table]
	}
	if expectedBefore == 0 {
		t.Fatalf("every table before %s is empty in the control (%v) -- nothing would land, so no "+
			"partial write is possible", breakTable, control)
	}

	// ---- FAULT ------------------------------------------------------------
	for _, table := range benchmarkingWriteOrder {
		if err := conn.Exec(ctx, "TRUNCATE TABLE "+table); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Exec(ctx, "DROP TABLE "+breakTable); err != nil {
		t.Fatal(err)
	}

	rows, err := executor.ComputeFinalizeFamily(ctx, run)
	if err == nil {
		t.Fatalf("writing to a DROPPED table %s succeeded -- the fault was not injected, so a "+
			"pass here would be vacuous", breakTable)
	}
	if !errors.Is(err, ErrPartialWrite) {
		t.Errorf(
			"failure on %s (table %d of %d) does not wrap ErrPartialWrite, so computeNativeFamilies "+
				"will FAIL OPEN to the Python bridge and the %d row(s) already written to %v will be "+
				"written a second time. err = %v",
			breakTable, breakIndex+1, len(benchmarkingWriteOrder), expectedBefore, before, err)
	}
	if rows != expectedBefore {
		t.Errorf(
			"executor reported %d row(s) written before failing, expected %d -- the count an "+
				"operator uses to size the duplication is wrong. err = %v",
			rows, expectedBefore, err)
	}
	if !strings.Contains(err.Error(), breakTable) {
		t.Errorf("error does not name the failing table %q: %v", breakTable, err)
	}
	// This is the assertion that keeps benchmarkingWriteOrder honest against
	// WriteOutputs' own `steps` slice.
	position := fmt.Sprintf("table %d of %d", breakIndex+1, len(benchmarkingWriteOrder))
	if !strings.Contains(err.Error(), position) {
		t.Errorf(
			"error does not report %q -- either WriteOutputs reordered its steps and this test's "+
				"benchmarkingWriteOrder no longer matches (so it broke a DIFFERENT table than it "+
				"believes), or the position was dropped from the message. err = %v",
			position, err)
	}

	partial := benchmarkingCounts(t, ctx, conn, append(append([]string{}, before...), after...))
	landed := 0
	for _, table := range before {
		if partial[table] != control[table] {
			t.Errorf("table %s holds %d row(s) after the partial write, control wrote %d",
				table, partial[table], control[table])
		}
		landed += partial[table]
	}
	for _, table := range after {
		if partial[table] != 0 {
			t.Errorf("table %s is AFTER the failing table but holds %d row(s) -- the writer did not "+
				"stop at the failure", table, partial[table])
		}
	}
	if landed != expectedBefore {
		t.Errorf("%d row(s) are on disk before %s but the writer reported %d",
			landed, breakTable, expectedBefore)
	}

	// ---- POSITIVE CONTROL -------------------------------------------------
	// A caller that ignored ErrPartialWrite would fail open and the bridge would
	// recompute the whole family. These tables are plain MergeTrees with no
	// version column, so the earlier rows are not replaced -- they are joined by
	// a second copy. If this phase does NOT double the counts, the duplication
	// hazard is not real on this schema and the sentinel above is guarding
	// nothing; that is a failure of this test's premise, not a pass.
	if err := conn.Exec(ctx, benchmarkingDDL(t, breakTable)); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ComputeFinalizeFamily(ctx, run); err != nil {
		t.Fatalf("re-drive after restoring %s failed: %v", breakTable, err)
	}
	redrive := benchmarkingCounts(t, ctx, conn, before)
	for _, table := range before {
		if redrive[table] != 2*control[table] {
			t.Errorf(
				"positive control: %s holds %d row(s) after a re-drive, expected %d (double the "+
					"control's %d). If this table does NOT duplicate, ErrPartialWrite is guarding a "+
					"hazard that does not exist here and the reasoning behind it needs revisiting",
				table, redrive[table], 2*control[table], control[table])
		}
	}
}
