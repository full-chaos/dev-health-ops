//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// benchmarkingOutputKeys is, for each benchmarking output table, its day
// column and the columns of its key besides org_id.
var benchmarkingOutputKeys = map[string]struct{ day, key string }{
	"testops_metric_baselines":    {"period_end", "metric_name, scope_type, scope_key, rolling_window_days"},
	"testops_maturity_bands":      {"period_end", "metric_name, scope_type, scope_key"},
	"testops_metric_anomalies":    {"day", "metric_name, scope_type, scope_key, anomaly_type"},
	"testops_period_comparisons":  {"current_period_end", "metric_name, scope_type, scope_key, comparison_period_end"},
	"testops_metric_correlations": {"period_end", "metric_name, paired_metric_name, scope_type, scope_key"},
	"testops_benchmark_insights":  {"period_end", "insight_id"},
}

func seedBenchmarkingOutputs(t *testing.T, ctx context.Context, conn driver.Conn, org, repo string, day, computedAt time.Time) {
	t.Helper()
	exec := func(table, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
	}
	exec("testops_metric_baselines", `INSERT INTO testops_metric_baselines
    (metric_name, scope_type, scope_key, period_start, period_end, rolling_window_days, current_value, baseline_value,
     percentile_rank, p25_value, p50_value, p75_value, p90_value, sample_size, org_id, computed_at)
    VALUES ('pass_rate', 'repo', ?, ?, ?, 30, 0.9, 0.8, 0.5, 0.7, 0.8, 0.9, 0.95, 12, ?, ?)`,
		repo, day.AddDate(0, 0, -29), day, org, computedAt)
	exec("testops_maturity_bands", `INSERT INTO testops_maturity_bands
    (metric_name, scope_type, scope_key, period_start, period_end, value, percentile_rank, maturity_band, confidence, org_id, computed_at)
    VALUES ('pass_rate', 'repo', ?, ?, ?, 0.9, 0.5, 'developing', 0.8, ?, ?)`,
		repo, day.AddDate(0, 0, -29), day, org, computedAt)
	exec("testops_metric_anomalies", `INSERT INTO testops_metric_anomalies
    (metric_name, scope_type, scope_key, day, value, baseline_value, z_score, anomaly_type, direction, severity, volatility_score, org_id, computed_at)
    VALUES ('pass_rate', 'repo', ?, ?, 0.4, 0.9, -3.1, 'regression', 'down', 'critical', 0.2, ?, ?)`,
		repo, day, org, computedAt)
	exec("testops_period_comparisons", `INSERT INTO testops_period_comparisons
    (metric_name, scope_type, scope_key, current_period_start, current_period_end, comparison_period_start, comparison_period_end,
     current_value, comparison_value, absolute_delta, percentage_change, trend_direction, org_id, computed_at)
    VALUES ('pass_rate', 'repo', ?, ?, ?, ?, ?, 0.9, 0.8, 0.1, 12.5, 'up', ?, ?)`,
		repo, day.AddDate(0, 0, -6), day, day.AddDate(0, 0, -13), day.AddDate(0, 0, -7), org, computedAt)
	exec("testops_metric_correlations", `INSERT INTO testops_metric_correlations
    (metric_name, paired_metric_name, scope_type, scope_key, period_start, period_end, coefficient, p_value, sample_size,
     is_significant, interpretation, org_id, computed_at)
    VALUES ('pass_rate', 'failure_rate', 'repo', ?, ?, ?, -0.8, 0.01, 20, 1, 'strong negative', ?, ?)`,
		repo, day.AddDate(0, 0, -29), day, org, computedAt)
	exec("testops_benchmark_insights", `INSERT INTO testops_benchmark_insights
    (insight_id, insight_type, scope_type, scope_key, metric_name, paired_metric_name, period_start, period_end,
     severity, summary, evidence_json, org_id, computed_at)
    VALUES (?, 'anomaly', 'repo', ?, 'pass_rate', NULL, ?, ?, 'critical', 'pass rate fell', '{}', ?, ?)`,
		"insight-"+org+"-"+day.Format("20060102"), repo, day, day, org, computedAt)
}

func benchmarkingHasRetractedColumn(t *testing.T, ctx context.Context, conn driver.Conn, table string) bool {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.columns
WHERE database = currentDatabase() AND table = ? AND name = 'retracted'`, table).Scan(&count); err != nil {
		t.Fatalf("read the columns of %s: %v", table, err)
	}
	return count == 1
}

// benchmarkingLiveKeys counts the keys of (org, day) whose newest row is not a
// retraction. On a schema without the marker every newest row is live.
func benchmarkingLiveKeys(t *testing.T, ctx context.Context, conn driver.Conn, table, org string, day time.Time) uint64 {
	t.Helper()
	declaration := benchmarkingOutputKeys[table]
	marker := "0"
	if benchmarkingHasRetractedColumn(t, ctx, conn, table) {
		marker = "argMax(retracted, computed_at)"
	}
	var live uint64
	query := "SELECT count() FROM (SELECT " + declaration.key + " FROM " + table +
		" WHERE org_id = ? AND " + declaration.day + " = ? GROUP BY " + declaration.key +
		" HAVING " + marker + " = 0)"
	if err := conn.QueryRow(ctx, query, org, day).Scan(&live); err != nil {
		t.Fatalf("read the live keys of %s: %v", table, err)
	}
	return live
}

// On a run of the whole organization with no partition, and no repository at
// the finalize, benchmarking retires every row of the day in its six output
// tables: the key's newest row is a retraction, strictly newer than the stored
// row, also over a stored row whose computed_at is ahead of the clock. Other
// organizations and other days are not touched. A second finalize writes no
// row.
func TestBenchmarkingRetiresEveryLiveKeyOfTheDayOnAnEmptyRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := capabilityRetireClickHouse(t, ctx)
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	before := day.AddDate(0, 0, -1)
	org := "00000000-0000-4000-8000-000000140523"
	other := "00000000-0000-4000-8000-000000140524"
	repo := "00000000-0000-4000-8000-00000014052b"
	now := day.Add(40 * time.Hour)
	seedBenchmarkingOutputs(t, ctx, conn, org, repo, day, day.Add(30*time.Hour))
	seedBenchmarkingOutputs(t, ctx, conn, org, repo, before, day.Add(30*time.Hour))
	seedBenchmarkingOutputs(t, ctx, conn, other, repo, day, day.Add(30*time.Hour))
	if err := conn.Exec(ctx, `INSERT INTO testops_maturity_bands
    (metric_name, scope_type, scope_key, period_start, period_end, value, percentile_rank, maturity_band, confidence, org_id, computed_at)
    VALUES ('line_coverage_pct', 'repo', ?, ?, ?, 71, 0.5, 'developing', 0.8, ?, ?)`,
		repo, day.AddDate(0, 0, -29), day, org, now.Add(time.Hour)); err != nil {
		t.Fatalf("seed a row ahead of the clock: %v", err)
	}

	store := &fakeStore{noPartitions: true, run: Run{ID: "run-14052", OrganizationID: org, TargetDay: day, FullOrg: true}}
	executor, err := NewBenchmarkingFinalizeExecutor(store, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	executor.nowUTC = func() time.Time { return now }
	run := Run{ID: "run-14052", OrganizationID: org, TargetDay: day}

	written, computeErr := executor.ComputeFinalizeFamily(ctx, run)
	t.Logf("first finalize: written=%d err=%v", written, computeErr)
	if computeErr != nil {
		t.Errorf("benchmarking on a whole-organization run with no partition: %v, want the retirement", computeErr)
	}
	for table := range benchmarkingOutputKeys {
		live := benchmarkingLiveKeys(t, ctx, conn, table, org, day)
		t.Logf("%s: %d live key(s) of the day after the finalize", table, live)
		if live != 0 {
			t.Errorf("%s: %d key(s) of the day stay live after an empty run", table, live)
		}
		for name, control := range map[string]struct {
			org string
			day time.Time
		}{"same org day-1": {org, before}, "other org": {other, day}} {
			if got := benchmarkingLiveKeys(t, ctx, conn, table, control.org, control.day); got != 1 {
				t.Errorf("%s control %s: %d live key(s), want the stored 1", table, name, got)
			}
		}
	}
	if benchmarkingHasRetractedColumn(t, ctx, conn, "testops_maturity_bands") {
		var version time.Time
		if err := conn.QueryRow(ctx, `SELECT max(computed_at) FROM testops_maturity_bands
WHERE org_id = ? AND period_end = ? AND metric_name = 'line_coverage_pct' AND retracted = 1`, org, day).Scan(&version); err != nil {
			t.Fatalf("read the retraction of the row ahead of the clock: %v", err)
		}
		if want := now.Add(time.Hour + time.Second); !version.Equal(want) {
			t.Errorf("retraction of the row ahead of the clock at %s, want %s (one second after the stored row)", version, want)
		}
	}

	again, err := executor.ComputeFinalizeFamily(ctx, run)
	t.Logf("second finalize: written=%d err=%v", again, err)
	if err != nil || again != 0 {
		t.Errorf("second finalize: written=%d err=%v, want 0 nil", again, err)
	}
}

// A run for day D writes an anomaly at the last point of a series at or before
// D (anomalies.go:95-107), and the insight of that anomaly at the same day
// (insights.go:149-161). With no point on D the row is keyed to an earlier
// day. No reader reads these tables; the cell asserts the reader contract
// (the newest row of a key, left out when it holds retracted = 1). The empty
// run of D retires the keys of D and leaves the keys of the earlier day: they
// are the results of that day, and the run of that day decides them.
func TestBenchmarkingEmptyRunKeepsAnAnomalyOfAnEarlierDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := capabilityRetireClickHouse(t, ctx)
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	earlier := day.AddDate(0, 0, -3)
	org := "00000000-0000-4000-8000-000000140526"
	repo := "00000000-0000-4000-8000-00000014052e"
	writtenByTheRunOfD := day.Add(30 * time.Hour)
	seedBenchmarkingOutputs(t, ctx, conn, org, repo, day, writtenByTheRunOfD)
	if err := conn.Exec(ctx, `INSERT INTO testops_metric_anomalies
    (metric_name, scope_type, scope_key, day, value, baseline_value, z_score, anomaly_type, direction, severity, volatility_score, org_id, computed_at)
    VALUES ('failure_rate', 'repo', ?, ?, 0.6, 0.1, 4.2, 'regression', 'up', 'critical', 0.3, ?, ?)`,
		repo, earlier, org, writtenByTheRunOfD); err != nil {
		t.Fatalf("seed the anomaly of an earlier day: %v", err)
	}
	if err := conn.Exec(ctx, `INSERT INTO testops_benchmark_insights
    (insight_id, insight_type, scope_type, scope_key, metric_name, paired_metric_name, period_start, period_end,
     severity, summary, evidence_json, org_id, computed_at)
    VALUES (?, 'anomaly', 'repo', ?, 'failure_rate', NULL, ?, ?, 'critical', 'failure rate rose', '{}', ?, ?)`,
		"insight-earlier-"+org, repo, earlier, earlier, org, writtenByTheRunOfD); err != nil {
		t.Fatalf("seed the insight of an earlier day: %v", err)
	}

	store := &fakeStore{noPartitions: true, run: Run{ID: "run-14052-anomaly", OrganizationID: org, TargetDay: day, FullOrg: true}}
	executor, err := NewBenchmarkingFinalizeExecutor(store, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	executor.nowUTC = func() time.Time { return day.Add(40 * time.Hour) }
	written, err := executor.ComputeFinalizeFamily(ctx, Run{ID: "run-14052-anomaly", OrganizationID: org, TargetDay: day})
	t.Logf("empty run of %s: written=%d err=%v", day.Format("2006-01-02"), written, err)
	if err != nil {
		t.Fatalf("empty run: %v", err)
	}
	for _, table := range []string{"testops_metric_anomalies", "testops_benchmark_insights"} {
		ofD := benchmarkingLiveKeys(t, ctx, conn, table, org, day)
		ofEarlier := benchmarkingLiveKeys(t, ctx, conn, table, org, earlier)
		t.Logf("%s: live keys of D=%d, of D-3=%d after the empty run of D", table, ofD, ofEarlier)
		if ofD != 0 {
			t.Errorf("%s: %d key(s) of D stay live after the empty run of D", table, ofD)
		}
		if ofEarlier != 1 {
			t.Errorf("%s: %d live key(s) of D-3, want the 1 stored (a result of that day, not of D)", table, ofEarlier)
		}
	}
}
