//go:build integration

package benchmarking

import (
	"context"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The pipeline success and failure benchmark series are rates over the completed runs (succeeded or failed) of each
// repo-day, read from the counts. A repo-day with no completed run is no sample (never a 0, whatever the stored rate
// column holds), a day on which no repo completed a run has no point, and a newest row with no completed run is not
// replaced by an older row of the same key.
func TestPipelineRateSeriesLeaveOutARepoDayWithNoCompletedRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()

	const (
		org       = "org-testops-completed"
		completed = "00000000-0000-4000-8000-0000000000c1"
		none      = "00000000-0000-4000-8000-0000000000c2"
	)
	measured := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	noneDay := measured.AddDate(0, 0, 1)
	recomputed := measured.AddDate(0, 0, 2)
	older := time.Date(2026, 8, 5, 1, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	// storedRate is what the old all-runs formula wrote; the series must not read it.
	row := func(repo string, day time.Time, runs, success, failure, cancelled int, storedRate float64, computedAt time.Time) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO testops_pipeline_metrics_daily
    (repo_id, day, pipelines_count, success_count, failure_count, cancelled_count, success_rate, failure_rate,
     cancel_rate, rerun_rate, org_id, computed_at)
VALUES (toUUID(?), ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)`,
			repo, day, uint32(runs), uint32(success), uint32(failure), uint32(cancelled), storedRate, storedRate, org, computedAt); err != nil {
			t.Fatal(err)
		}
	}
	row(completed, measured, 6, 3, 1, 2, 0.5, older)
	row(none, measured, 3, 0, 0, 3, 0, older)
	row(none, noneDay, 2, 0, 0, 2, 0, older)
	row(completed, recomputed, 2, 2, 0, 0, 1, older)
	row(completed, recomputed, 1, 0, 0, 1, 0, newer)

	loader, err := NewClickHouseLoader(admin, org)
	if err != nil {
		t.Fatal(err)
	}
	for metric, want := range map[string]float64{"success_rate": 0.75, "failure_rate": 0.25} {
		for _, scope := range []string{ScopeGlobal, ScopeRepo} {
			series, err := loader.FetchMetricSeriesByScope(ctx, metric, measured, recomputed, scope)
			if err != nil {
				t.Fatalf("%s %s: %v", metric, scope, err)
			}
			var points []MetricPoint
			for _, scoped := range series {
				points = append(points, scoped...)
			}
			if len(points) != 1 || !points[0].Day.Equal(measured) || math.Abs(points[0].Value-want) > 1e-9 {
				t.Errorf("%s %s: %+v, want one point %s = %v (the repo with completed runs alone; no point where no run completed)",
					metric, scope, series, measured.Format(time.DateOnly), want)
			}
		}
	}
}
