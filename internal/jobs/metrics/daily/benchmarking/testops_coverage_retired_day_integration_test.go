//go:build integration

package benchmarking

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-14008 C3 (benchmarking): a coverage day whose newest row retires it is not a sample. coverage_regression_count is not
// Nullable, so the marker row's default 0 would be one more sample of 0 without the live-row rule.
func TestCoverageSeriesLeavesOutARetiredDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
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

	const org, repo = "org-14008", "14008000-0000-4000-8000-0000000000a1"
	day1 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)
	t1 := time.Date(2026, 8, 3, 1, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	measured := func(day time.Time, regressions uint32, at time.Time) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO testops_coverage_metrics_daily (repo_id, day, line_coverage_pct, uncovered_files_count,
 coverage_regression_count, org_id, computed_at) VALUES (toUUID(?), ?, 80, 0, ?, ?, ?)`, repo, day, regressions, org, at); err != nil {
			t.Fatal(err)
		}
	}
	measured(day1, 7, t1)
	measured(day2, 5, t1)
	// The newer row of day 1 retires it.
	if err := admin.Exec(ctx, `INSERT INTO testops_coverage_metrics_daily (repo_id, day, uncovered_files_count, coverage_regression_count,
 org_id, computed_at, retracted) VALUES (toUUID(?), ?, 0, 0, ?, ?, 1)`, repo, day1, org, t2); err != nil {
		t.Fatal(err)
	}

	loader, err := NewClickHouseLoader(admin, org)
	if err != nil {
		t.Fatal(err)
	}
	series, err := loader.FetchMetricSeriesByScope(ctx, "coverage_regression_count", day1, day2, ScopeRepo)
	if err != nil {
		t.Fatal(err)
	}
	points := series[repo]
	t.Logf("C3CELL benchmarking points of the repository: %v", points)
	if len(points) != 1 || points[0].Value != 5 {
		t.Fatalf("the series is %v, want the one live day (value 5) and no sample of 0 for the retired day", points)
	}
}
