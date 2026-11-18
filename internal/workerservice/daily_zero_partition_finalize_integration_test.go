//go:build integration

package workerservice

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// A daily run of the whole organization whose discovery found no repository
// has no partition. Its finalize is the only place that retires the rows an
// earlier compute left for the day: the end-of-run cleanup of the whole
// organization and every finalize family that does not need a partition. The
// finalize handler here is the production one (newDrainingDailyFinalizeHandler
// and the family set of dailyNativeFamilyRegistrations).

type zeroPartitionDrain struct {
	mu     sync.Mutex
	passes int
}

func (drain *zeroPartitionDrain) DrainTouchedDays(context.Context, string, string) {
	drain.mu.Lock()
	defer drain.mu.Unlock()
	drain.passes++
}

type zeroPartitionMeasure struct {
	table, dayColumn, held, filter string
	final                          bool
	// key, for a plain MergeTree table, is the key of a row: held is read
	// from the newest row of each key that is not a retraction.
	key string
}

var zeroPartitionMeasures = []zeroPartitionMeasure{
	{table: "team_metrics_daily", dayColumn: "day", held: "commits_count", final: true},
	{table: "user_metrics_daily", dayColumn: "day", held: "pr_interruption_load + review_request_load", final: true},
	{table: "work_item_metrics_daily", dayColumn: "day", held: "items_completed", final: true},
	{table: "team_cognitive_load_daily", dayColumn: "day", held: "pr_interruption_load + contributing_repo_count", final: true},
	{table: "team_complexity_daily", dayColumn: "day", held: "loc_total + contributing_repo_count", final: true},
	{table: "compounding_risk_daily", dayColumn: "day", held: "ifNull(compounding_risk, 0) + w_churn", filter: " AND scope = 'team'", final: true},
	{table: "ic_landscape_rolling_30d", dayColumn: "as_of_day", held: "x_raw + y_raw", final: true},
}

// benchmarkingMeasure is an output of benchmarking, the family that reads the
// partitions of a run. On a run of the whole organization with no repository
// it retires the rows of the day (a retraction row, retracted = 1).
var benchmarkingMeasure = zeroPartitionMeasure{table: "testops_maturity_bands", dayColumn: "period_end", held: "value", key: "metric_name, scope_type, scope_key"}

// capabilityMeasure is the output of work_item_measure_capability. The family
// retires each key of the day it no longer produces (item_count 0).
var capabilityMeasure = zeroPartitionMeasure{table: "work_item_measure_capability", dayColumn: "window_end", held: "tracked + item_count", final: true}

func seedZeroPartitionDay(t *testing.T, ctx context.Context, conn driver.Conn, org string, day time.Time, repo uuid.UUID) {
	t.Helper()
	stored := day.Add(30 * time.Hour)
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	exec("team_metrics_daily", `INSERT INTO team_metrics_daily
    (org_id, day, team_id, team_name, repo_id, commits_count, after_hours_commits_count, weekend_commits_count, computed_at)
    VALUES (?, ?, 'ENG', 'ENG', ?, 7, 1, 0, ?)`, org, day, repo.String(), stored)
	exec("user_metrics_daily", `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, pr_interruption_load, review_request_load, computed_at, org_id)
    VALUES (?, ?, 'dev@example.com', 3, 2, ?, ?)`, repo, day, stored, org)
	exec("work_item_metrics_daily", `INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day, computed_at, org_id)
    VALUES (?, 'linear', 'gone-linear', 'ENG', 'ENG', 10, 9, 2, ?, ?)`, day, stored, org)
	exec("team_cognitive_load_daily", `INSERT INTO team_cognitive_load_daily
    (org_id, team_id, day, pr_interruption_load, context_spread_count, review_request_load, contributing_repo_count, sample_author_count, computed_at)
    VALUES (?, 'platform', ?, 3, 1, 2, 1, 1, ?)`, org, day, stored)
	exec("team_complexity_daily", `INSERT INTO team_complexity_daily
    (org_id, team_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc, high_complexity_functions, very_high_complexity_functions, contributing_repo_count, computed_at)
    VALUES (?, 'platform', ?, 1000, 200, 200, 4, 1, 1, ?)`, org, day, stored)
	exec("compounding_risk_daily", `INSERT INTO compounding_risk_daily
    (org_id, day, scope, scope_id, compounding_risk, severity, w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at)
    VALUES (?, ?, 'team', 'platform', 0.8, 'high', 0.3, 0.3, 0.2, 0.2, 0.4, 0.65, ?)`, org, day, stored)
	exec("ic_landscape_rolling_30d", `INSERT INTO ic_landscape_rolling_30d
    (repo_id, as_of_day, identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm, org_id, computed_at)
    VALUES ('00000000-0000-0000-0000-000000000000', ?, 'dev@example.com', 'platform', 'churn_throughput', 4, 5, 0.5, 0.5, ?, ?)`, day, org, stored)
	exec("testops_maturity_bands", `INSERT INTO testops_maturity_bands
    (metric_name, scope_type, scope_key, period_start, period_end, value, percentile_rank, maturity_band, confidence, org_id, computed_at)
    VALUES ('pass_rate', 'org', ?, ?, ?, 0.9, 0.5, 'developing', 0.8, ?, ?)`, org, day, day, org, stored)
	exec("work_item_measure_capability", `INSERT INTO work_item_measure_capability
    (org_id, provider, measure, tracked, evidence_count, item_count, window_start, window_end, computed_at)
    VALUES (?, 'github', 'story_points_completed', 1, 3, 4, ?, ?, ?)`, org, day.AddDate(0, 0, -29), day, stored)
}

func zeroPartitionHeld(t *testing.T, ctx context.Context, conn driver.Conn, measure zeroPartitionMeasure, org string, day time.Time) float64 {
	t.Helper()
	if measure.key != "" {
		return zeroPartitionNewestLiveHeld(t, ctx, conn, measure, org, day)
	}
	final := ""
	if measure.final {
		final = " FINAL"
	}
	var held float64
	if err := conn.QueryRow(ctx, "SELECT toFloat64(ifNull(sum("+measure.held+"), 0)) FROM "+measure.table+final+
		" WHERE org_id = ? AND "+measure.dayColumn+" = ?"+measure.filter, org, day).Scan(&held); err != nil {
		t.Fatalf("read %s: %v", measure.table, err)
	}
	return held
}

// zeroPartitionNewestLiveHeld reads held from the newest row of each key that
// is not a retraction. On a schema without the retracted column every newest
// row is live.
func zeroPartitionNewestLiveHeld(t *testing.T, ctx context.Context, conn driver.Conn, measure zeroPartitionMeasure, org string, day time.Time) float64 {
	t.Helper()
	var markers uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.columns
WHERE database = currentDatabase() AND table = ? AND name = 'retracted'`, measure.table).Scan(&markers); err != nil {
		t.Fatalf("read the columns of %s: %v", measure.table, err)
	}
	marker := "toUInt8(0)"
	if markers == 1 {
		marker = "argMax(retracted, computed_at)"
	}
	var held float64
	if err := conn.QueryRow(ctx, "SELECT toFloat64(ifNull(sum(held), 0)) FROM (SELECT "+measure.key+
		", argMax(tuple("+measure.held+"), computed_at).1 AS held, "+marker+" AS retired FROM "+measure.table+
		" WHERE org_id = ? AND "+measure.dayColumn+" = ?"+measure.filter+" GROUP BY "+measure.key+") WHERE retired = 0",
		org, day).Scan(&held); err != nil {
		t.Fatalf("read %s: %v", measure.table, err)
	}
	return held
}

// captureDefaultLog sends the default logger to a buffer for the rest of the
// test: the families log through slog.Default.
func captureDefaultLog(t *testing.T) *lockedBuffer {
	t.Helper()
	buffer := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buffer
}

type zeroPartitionRig struct {
	*nilPartitionRig
	finalize *daily.FinalizeHandler
	drain    *zeroPartitionDrain
}

func newZeroPartitionRig(t *testing.T, ctx context.Context) *zeroPartitionRig {
	t.Helper()
	rig := newNilPartitionRig(t, ctx)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	drain := &zeroPartitionDrain{}
	handler, err := newDrainingDailyFinalizeHandler(rig.store, drain, rig.conn)
	if err != nil {
		t.Fatal(err)
	}
	engines := dailyWorkItemEnginesFrom(validGitHubWorkItemsRuntimeConfig(t))
	if engines.err != nil {
		t.Fatal(engines.err)
	}
	_, _, families, refusals := dailyNativeFamilyRegistrations(rig.store, rig.conn, engines, nil, logger)
	if len(refusals) != 0 {
		t.Fatalf("family refusals: %v", refusals)
	}
	registerNativeFinalizeFamilies(logger, handler, families)
	return &zeroPartitionRig{nilPartitionRig: rig, finalize: handler, drain: drain}
}

func (rig *zeroPartitionRig) insertRun(t *testing.T, ctx context.Context, org string, day time.Time, fullOrg bool) string {
	t.Helper()
	runID := uuid.NewString()
	now := time.Now().UTC()
	if _, err := rig.pool.Exec(ctx, `INSERT INTO daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at, full_org)
    VALUES ($1::uuid, $2::uuid, $3::date, $4, 'running', 'pending', $5, $5, $6)`,
		runID, org, day.Format("2006-01-02"), daily.ScheduledFanoutGenerationPrefix+runID[:8], now, fullOrg); err != nil {
		t.Fatal(err)
	}
	return runID
}

func (rig *zeroPartitionRig) runFinalize(ctx context.Context, org, runID string) error {
	execution := &jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs]{
		Attempt:        1,
		OrganizationID: &org,
		Envelope:       jobcontract.Envelope{OrganizationID: &org, Domain: jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID}},
		Args: jobruntime.DailyMetricsFinalizeArgs{EnvelopeArgs: func() jobruntime.EnvelopeArgs[jobcontract.DailyMetricsFinalizePayload] {
			args := nilPartitionExecution[jobcontract.DailyMetricsFinalizePayload](org, "daily_metrics_run", runID)
			args.Payload = jobcontract.DailyMetricsFinalizePayload{RunID: runID}
			return args
		}()},
		Definition: jobruntime.Descriptor{MaxAttempts: 4},
	}
	return rig.finalize.Work(ctx, execution)
}

func (rig *zeroPartitionRig) runState(t *testing.T, ctx context.Context, runID string) (status, finalization string, fences int) {
	t.Helper()
	if err := rig.pool.QueryRow(ctx, `SELECT status, finalization_status FROM daily_metrics_runs WHERE id = $1::uuid`, runID).
		Scan(&status, &finalization); err != nil {
		t.Fatal(err)
	}
	if err := rig.pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_completion_fences WHERE completion_key = $1`,
		"daily_metrics_run:"+runID).Scan(&fences); err != nil {
		t.Fatal(err)
	}
	return status, finalization, fences
}

func TestFinalizeOfAWholeOrganizationRunWithNoPartitionRetiresTheRowsOfItsDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newZeroPartitionRig(t, ctx)
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	before := day.AddDate(0, 0, -1)
	removed := uuid.MustParse("00000000-0000-4000-8000-00000014037a")
	org := "00000000-0000-4000-8000-000000140371"
	other := "00000000-0000-4000-8000-000000140372"
	seedZeroPartitionDay(t, ctx, rig.conn, org, day, removed)
	seedZeroPartitionDay(t, ctx, rig.conn, org, before, removed)
	seedZeroPartitionDay(t, ctx, rig.conn, other, day, removed)
	controls := []struct {
		org string
		day time.Time
	}{{other, day}, {org, before}}
	heldBefore := map[string]float64{}
	for _, measure := range append(zeroPartitionMeasures, benchmarkingMeasure, capabilityMeasure) {
		for _, control := range controls {
			heldBefore[measure.table+control.org+control.day.String()] = zeroPartitionHeld(t, ctx, rig.conn, measure, control.org, control.day)
		}
		if held := zeroPartitionHeld(t, ctx, rig.conn, measure, org, day); held == 0 {
			t.Fatalf("the seeded %s row holds 0: the case is not set", measure.table)
		}
	}
	runID := rig.insertRun(t, ctx, org, day, true)
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	rig.finalize.SetNativeFinalizeFamilyObserver(collector)
	logs := captureDefaultLog(t)

	workErr := rig.runFinalize(ctx, org, runID)

	status, finalization, fences := rig.runState(t, ctx, runID)
	t.Logf("run=%s finalize_err=%v status=%s finalization=%s fences=%d drain_passes=%d",
		runID, workErr, status, finalization, fences, rig.drain.passes)
	if workErr != nil {
		t.Errorf("the finalize failed: %v", workErr)
	}
	if status != "no_repositories" || finalization != "succeeded" || fences != 1 {
		t.Errorf("the run ended as status=%s finalization=%s with %d fence(s), want no_repositories, succeeded, 1", status, finalization, fences)
	}
	if rig.drain.passes != 1 {
		t.Errorf("the touched-day drain ran %d time(s) after the run, want 1", rig.drain.passes)
	}
	for _, measure := range append(zeroPartitionMeasures, benchmarkingMeasure, capabilityMeasure) {
		held := zeroPartitionHeld(t, ctx, rig.conn, measure, org, day)
		t.Logf("%s: the row of the day holds %v after the finalize", measure.table, held)
		if held != 0 {
			t.Errorf("%s: the row of the earlier compute still holds %v after the finalize", measure.table, held)
		}
	}
	capabilityLine := ""
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, `"msg":"`+daily.WorkItemMeasureCapabilityLogMessage+`"`) && strings.Contains(line, org) {
			capabilityLine = line
		}
	}
	t.Logf("capability log line: %s", capabilityLine)
	if !strings.Contains(capabilityLine, `"level":"INFO"`) || !strings.Contains(capabilityLine, `"rows":0`) ||
		!strings.Contains(capabilityLine, `"rows_retired":1`) {
		t.Errorf("no Info line %q with rows=0 and rows_retired=1 for the organization", daily.WorkItemMeasureCapabilityLogMessage)
	}
	exposition := collector.PrometheusText()
	for _, want := range []string{
		`worker_daily_metrics_native_family_outcome_total{family="` + daily.WorkItemMeasureCapabilityFamilyName + `",outcome="computed"} 1`,
		`worker_daily_metrics_native_family_outcome_total{family="` + daily.BenchmarkingFamilyName + `",outcome="computed"} 1`,
	} {
		t.Logf("counter %s present=%t", want, strings.Contains(exposition, want))
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing %q", want)
		}
	}
	skipped := `worker_daily_metrics_native_family_outcome_total{family="` + daily.BenchmarkingFamilyName + `",outcome="skipped_empty_run"} 1`
	if strings.Contains(exposition, skipped) {
		t.Errorf("benchmarking was skipped on the empty run: %q", skipped)
	}
	for _, measure := range append(zeroPartitionMeasures, benchmarkingMeasure, capabilityMeasure) {
		for _, control := range controls {
			want := heldBefore[measure.table+control.org+control.day.String()]
			if got := zeroPartitionHeld(t, ctx, rig.conn, measure, control.org, control.day); got != want {
				t.Errorf("%s: the control row of org %s day %s holds %v, want the stored %v",
					measure.table, control.org, control.day.Format("2006-01-02"), got, want)
			}
		}
	}
}

func TestFinalizeOfARunOfSomeRepositoriesWithNoPartitionFailsLoudly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newZeroPartitionRig(t, ctx)
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	org := "00000000-0000-4000-8000-000000140373"
	seedZeroPartitionDay(t, ctx, rig.conn, org, day, uuid.MustParse("00000000-0000-4000-8000-00000014037b"))
	heldBefore := map[string]float64{}
	for _, measure := range zeroPartitionMeasures {
		heldBefore[measure.table] = zeroPartitionHeld(t, ctx, rig.conn, measure, org, day)
	}
	runID := rig.insertRun(t, ctx, org, day, false)

	workErr := rig.runFinalize(ctx, org, runID)

	status, finalization, fences := rig.runState(t, ctx, runID)
	t.Logf("run=%s finalize_err=%v status=%s finalization=%s fences=%d", runID, workErr, status, finalization, fences)
	if workErr == nil || !errors.Is(workErr, daily.ErrInvalidState) || !strings.Contains(workErr.Error(), string(jobruntime.CategoryPermanent)) {
		t.Errorf("the finalize gave %v, want a permanent ErrInvalidState", workErr)
	}
	if status != "running" || finalization != "failed" || fences != 0 {
		t.Errorf("the run is status=%s finalization=%s with %d fence(s), want running, failed (released), 0", status, finalization, fences)
	}
	for _, measure := range zeroPartitionMeasures {
		if got := zeroPartitionHeld(t, ctx, rig.conn, measure, org, day); got != heldBefore[measure.table] {
			t.Errorf("%s: the refused finalize changed the row of the day to %v, want the stored %v", measure.table, got, heldBefore[measure.table])
		}
	}
}

// zeroPartitionParityTables are the tables whose keys the end-of-run cleanup
// computes again from the stored work items of the whole organization.
var zeroPartitionParityTables = []string{
	"work_item_metrics_daily", "work_item_user_metrics_daily", "work_item_state_durations_daily", "estimate_coverage_metrics_daily",
}

// zeroPartitionDayRows reads the newest row of every key of the organization's
// day, with every column but the organization and the version, in one order.
func zeroPartitionDayRows(t *testing.T, ctx context.Context, conn driver.Conn, table, org string, day time.Time) []string {
	t.Helper()
	columnRows, err := conn.Query(ctx, `SELECT name FROM system.columns
WHERE database = currentDatabase() AND table = ? AND name NOT IN ('org_id', 'computed_at') ORDER BY position`, table)
	if err != nil {
		t.Fatalf("read the columns of %s: %v", table, err)
	}
	var columns []string
	for columnRows.Next() {
		var name string
		if err := columnRows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, "`"+name+"`")
	}
	_ = columnRows.Close()
	rows, err := conn.Query(ctx, "SELECT toString(tuple("+strings.Join(columns, ", ")+")) AS row FROM "+table+
		" FINAL WHERE org_id = ? AND day = ? ORDER BY row", org, day)
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	return result
}

// A key whose source is still stored is computed again by the run, not
// retired: the run of an organization with no repository gives the day the
// same keys and values as a run of the whole organization that has a
// repository, on the same stored work items.
func TestFinalizeOfARunWithNoPartitionRecomputesTheDayAsAWholeOrganizationRunDoes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newZeroPartitionRig(t, ctx)
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	removed := uuid.MustParse("00000000-0000-4000-8000-00000014037c")
	held := uuid.MustParse("00000000-0000-4000-8000-00000014037d")
	empty := "00000000-0000-4000-8000-000000140374"
	whole := "00000000-0000-4000-8000-000000140375"
	stored := day.Add(30 * time.Hour)
	for _, org := range []string{empty, whole} {
		// The earlier compute of the day, with values the stored items do not give.
		if err := rig.conn.Exec(ctx, `INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day, computed_at, org_id)
    VALUES (?, 'linear', 'board-1', 'unassigned', 'unassigned', 40, 39, 12, ?, ?)`, day, stored, org); err != nil {
			t.Fatal(err)
		}
		// The items stay stored under the removed repository, which no repos row holds.
		batch, err := rig.conn.PrepareBatch(ctx, `INSERT INTO work_items (
    repo_id, work_item_id, provider, type, status, project_id, created_at, started_at, completed_at, org_id, last_synced)`)
		if err != nil {
			t.Fatal(err)
		}
		completed := day.Add(10 * time.Hour)
		if err := batch.Append(removed, "ITEM-1", "linear", "story", "in_progress", "board-1",
			day.Add(-48*time.Hour), ptrTime(day.Add(-24*time.Hour)), (*time.Time)(nil), org, stored); err != nil {
			t.Fatal(err)
		}
		if err := batch.Append(removed, "ITEM-2", "linear", "bug", "done", "board-1",
			day.Add(-72*time.Hour), ptrTime(day.Add(-30*time.Hour)), &completed, org, stored); err != nil {
			t.Fatal(err)
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rig.conn.Exec(ctx, `INSERT INTO repos (id, org_id, repo, provider, created_at, last_synced) VALUES (?, ?, 'acme/held', 'github', ?, ?)`,
		held, whole, day, day); err != nil {
		t.Fatal(err)
	}

	emptyRun := rig.insertRun(t, ctx, empty, day, true)
	if err := rig.runFinalize(ctx, empty, emptyRun); err != nil {
		t.Fatalf("finalize of the run with no partition: %v", err)
	}
	wholeRun := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
		return rig.store.StartScheduledFanoutRunTx(ctx, tx, daily.ScheduledFanoutRequest{
			OrganizationID: whole, TargetDay: day, Generation: daily.ScheduledFanoutGenerationPrefix + "2026-09-15T01:00:00Z",
		}, nilPartitionPublisher{})
	})
	partitions := rig.dispatchAndRun(t, ctx, wholeRun)
	if len(partitions) != 1 {
		t.Fatalf("the whole-organization run has %d partition(s), want 1", len(partitions))
	}
	if err := rig.runFinalize(ctx, whole, wholeRun.ID); err != nil {
		t.Fatalf("finalize of the whole-organization run: %v", err)
	}
	emptyStatus, _, emptyFences := rig.runState(t, ctx, emptyRun)
	wholeStatus, _, wholeFences := rig.runState(t, ctx, wholeRun.ID)
	t.Logf("run with no partition: status=%s fences=%d; whole-organization run: status=%s fences=%d",
		emptyStatus, emptyFences, wholeStatus, wholeFences)
	if emptyStatus != "no_repositories" || wholeStatus != "succeeded" || emptyFences != 1 || wholeFences != 1 {
		t.Fatalf("the runs ended as %s/%d and %s/%d, want no_repositories/1 and succeeded/1", emptyStatus, emptyFences, wholeStatus, wholeFences)
	}
	for _, table := range zeroPartitionParityTables {
		got := zeroPartitionDayRows(t, ctx, rig.conn, table, empty, day)
		want := zeroPartitionDayRows(t, ctx, rig.conn, table, whole, day)
		t.Logf("%s: %d key(s) after the run with no partition, %d after the whole-organization run", table, len(got), len(want))
		for _, row := range got {
			t.Logf("%s: %s", table, row)
		}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("%s differs:\nrun with no partition:\n%s\nwhole-organization run:\n%s", table, strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	}
	if rows := zeroPartitionDayRows(t, ctx, rig.conn, "work_item_metrics_daily", empty, day); len(rows) == 0 {
		t.Error("work_item_metrics_daily holds no key of the day: the case is not set")
	}
	var earlier float64
	if err := rig.conn.QueryRow(ctx, `SELECT toFloat64(ifNull(sum(items_started), 0)) FROM work_item_metrics_daily FINAL
WHERE org_id = ? AND day = ? AND items_started = 40`, empty, day).Scan(&earlier); err != nil {
		t.Fatal(err)
	}
	if earlier != 0 {
		t.Errorf("the values of the earlier compute are still live (%v)", earlier)
	}
}

func ptrTime(value time.Time) *time.Time { return &value }

// A repository written after the dispatch found none is back at the finalize
// of the empty run: benchmarking retires nothing, counts the skip and says why
// in its Info line, and the run still ends as no_repositories.
func TestFinalizeOfAnEmptyRunKeepsBenchmarkingRowsWhenARepositoryIsBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newZeroPartitionRig(t, ctx)
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	org := "00000000-0000-4000-8000-000000140525"
	seedZeroPartitionDay(t, ctx, rig.conn, org, day, uuid.MustParse("00000000-0000-4000-8000-00000014052c"))
	runID := rig.insertRun(t, ctx, org, day, true)
	now := time.Now().UTC()
	if err := rig.conn.Exec(ctx, `INSERT INTO repos (id, org_id, repo, provider, created_at, last_synced) VALUES (?, ?, 'acme/back', 'github', ?, ?)`,
		uuid.MustParse("00000000-0000-4000-8000-00000014052d"), org, now, now); err != nil {
		t.Fatal(err)
	}
	before := zeroPartitionHeld(t, ctx, rig.conn, benchmarkingMeasure, org, day)
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	rig.finalize.SetNativeFinalizeFamilyObserver(collector)
	logs := captureDefaultLog(t)

	workErr := rig.runFinalize(ctx, org, runID)

	status, finalization, fences := rig.runState(t, ctx, runID)
	after := zeroPartitionHeld(t, ctx, rig.conn, benchmarkingMeasure, org, day)
	t.Logf("run=%s finalize_err=%v status=%s finalization=%s fences=%d benchmarking held before=%v after=%v",
		runID, workErr, status, finalization, fences, before, after)
	if workErr != nil || status != "no_repositories" || finalization != "succeeded" || fences != 1 {
		t.Errorf("the run ended as err=%v status=%s finalization=%s fences=%d, want nil, no_repositories, succeeded, 1",
			workErr, status, finalization, fences)
	}
	if before == 0 || after != before {
		t.Errorf("benchmarking held %v before and %v after, want the stored row kept", before, after)
	}
	skipLine := ""
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, `"msg":"`+daily.ZeroPartitionFamilySkippedLogMessage+`"`) && strings.Contains(line, org) {
			skipLine = line
		}
	}
	t.Logf("skip log line: %s", skipLine)
	if !strings.Contains(skipLine, `"family":"`+daily.BenchmarkingFamilyName+`"`) ||
		!strings.Contains(skipLine, `"reason":"repository_present_at_finalize"`) {
		t.Errorf("no Info line %q for benchmarking with reason repository_present_at_finalize", daily.ZeroPartitionFamilySkippedLogMessage)
	}
	want := `worker_daily_metrics_native_family_outcome_total{family="` + daily.BenchmarkingFamilyName + `",outcome="skipped_empty_run"} 1`
	if !strings.Contains(collector.PrometheusText(), want) {
		t.Errorf("exposition is missing %q", want)
	}
}

// A Jira item with no repository (stored under the nil repository id) is
// synced after the dispatch found no partition: the organization still holds
// no repository, so benchmarking retires the rows of the day.
func TestFinalizeOfAnEmptyRunRetiresBenchmarkingWhenOnlyARepositorylessItemArrives(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newZeroPartitionRig(t, ctx)
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	org := "00000000-0000-4000-8000-000000140527"
	seedZeroPartitionDay(t, ctx, rig.conn, org, day, uuid.MustParse("00000000-0000-4000-8000-000000140530"))
	runID := rig.insertRun(t, ctx, org, day, true)
	created := day.Add(-48 * time.Hour)
	if err := rig.conn.Exec(ctx, `INSERT INTO work_items
    (repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, native_team_key, project_name,
     assignees, reporter, created_at, updated_at, labels, story_points, sprint_id, sprint_name, parent_id, epic_id, url, last_synced, org_id)
    VALUES (?, 'jira:PROJ-1', 'jira', 'item', 'bug', 'in_progress', 'In Progress', 'PROJ', 'p-1', '', 'Project', [], '', ?, ?, [], 2, '', '', '', '', '', ?, ?)`,
		uuid.Nil, created, created, created, org); err != nil {
		t.Fatal(err)
	}
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	rig.finalize.SetNativeFinalizeFamilyObserver(collector)

	workErr := rig.runFinalize(ctx, org, runID)

	status, finalization, fences := rig.runState(t, ctx, runID)
	held := zeroPartitionHeld(t, ctx, rig.conn, benchmarkingMeasure, org, day)
	t.Logf("run=%s finalize_err=%v status=%s finalization=%s fences=%d benchmarking held=%v", runID, workErr, status, finalization, fences, held)
	if workErr != nil {
		t.Errorf("the finalize failed: %v", workErr)
	}
	if held != 0 {
		t.Errorf("%s: the row of the day holds %v after the empty run, want 0", benchmarkingMeasure.table, held)
	}
	exposition := collector.PrometheusText()
	if want := `worker_daily_metrics_native_family_outcome_total{family="` + daily.BenchmarkingFamilyName + `",outcome="computed"} 1`; !strings.Contains(exposition, want) {
		t.Errorf("exposition is missing %q", want)
	}
}
