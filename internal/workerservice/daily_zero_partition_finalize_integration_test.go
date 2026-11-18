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

// benchmarkingMeasure is the output of the one finalize family that refuses a
// run with no partition. It is skipped on such a run, so its row stays.
var benchmarkingMeasure = zeroPartitionMeasure{table: "testops_maturity_bands", dayColumn: "period_end", held: "value"}

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
}

func zeroPartitionHeld(t *testing.T, ctx context.Context, conn driver.Conn, measure zeroPartitionMeasure, org string, day time.Time) float64 {
	t.Helper()
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
	for _, measure := range append(zeroPartitionMeasures, benchmarkingMeasure) {
		for _, control := range controls {
			heldBefore[measure.table+control.org+control.day.String()] = zeroPartitionHeld(t, ctx, rig.conn, measure, control.org, control.day)
		}
		if held := zeroPartitionHeld(t, ctx, rig.conn, measure, org, day); held == 0 {
			t.Fatalf("the seeded %s row holds 0: the case is not set", measure.table)
		}
	}
	runID := rig.insertRun(t, ctx, org, day, true)

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
	for _, measure := range zeroPartitionMeasures {
		held := zeroPartitionHeld(t, ctx, rig.conn, measure, org, day)
		t.Logf("%s: the row of the day holds %v after the finalize", measure.table, held)
		if held != 0 {
			t.Errorf("%s: the row of the earlier compute still holds %v after the finalize", measure.table, held)
		}
	}
	held := zeroPartitionHeld(t, ctx, rig.conn, benchmarkingMeasure, org, day)
	t.Logf("%s: the row of the day holds %v after the finalize (benchmarking is skipped on a run with no partition; ticket 14052)",
		benchmarkingMeasure.table, held)
	if held == 0 {
		t.Errorf("%s: the benchmarking row was changed, but the family must be skipped", benchmarkingMeasure.table)
	}
	for _, measure := range append(zeroPartitionMeasures, benchmarkingMeasure) {
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
