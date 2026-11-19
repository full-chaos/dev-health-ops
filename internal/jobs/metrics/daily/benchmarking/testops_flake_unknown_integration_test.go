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

// A TestOps test repo-day with no measured case stores NULL in flake_rate,
// retry_dependency_rate and failure_recurrence_score. A benchmark series of the
// three reads such a row as no sample: the day's value is the measured repos'
// mean, a day on which no repo measured a case has no point (never a 0), and a
// newest NULL row is not replaced by an older measured row of the same key.
func TestTestOpsRateSeriesLeaveOutARepoDayWithNoMeasuredCase(t *testing.T) {
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
		org     = "org-testops-unknown"
		known   = "00000000-0000-4000-8000-0000000000b1"
		unknown = "00000000-0000-4000-8000-0000000000b2"
	)
	measured := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	noneDay := measured.AddDate(0, 0, 1)
	recomputed := measured.AddDate(0, 0, 2)
	older := time.Date(2026, 8, 5, 1, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	row := func(repo string, day time.Time, rates *float64, computedAt time.Time) {
		t.Helper()
		// 5 distinct cases of which 1 (0.2) or 0 are flaky; a NULL rate is a day with no measured case.
		var flakeCases, distinctCases *uint32
		if rates != nil {
			one, five := uint32(1), uint32(5)
			flakeCases, distinctCases = &one, &five
		}
		if err := admin.Exec(ctx, `INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count, pass_rate, failure_rate,
     flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, org_id, computed_at,
     flake_cases, distinct_cases)
VALUES (toUUID(?), ?, 10, 10, 0, 0, 0, 1, 0, ?, ?, 1, ?, ?, ?, ?, ?)`,
			repo, day, rates, rates, rates, org, computedAt, flakeCases, distinctCases); err != nil {
			t.Fatal(err)
		}
	}
	value := func(v float64) *float64 { return &v }
	row(known, measured, value(0.2), older)
	row(unknown, measured, nil, older)
	row(unknown, noneDay, nil, older)
	row(known, recomputed, value(0.3), older)
	row(known, recomputed, nil, newer)

	loader, err := NewClickHouseLoader(admin, org)
	if err != nil {
		t.Fatal(err)
	}
	for _, metric := range []string{"flake_rate", "retry_dependency_rate", "failure_recurrence_score"} {
		for _, scope := range []string{ScopeGlobal, ScopeRepo} {
			series, err := loader.FetchMetricSeriesByScope(ctx, metric, measured, recomputed, scope)
			if err != nil {
				t.Fatalf("%s %s: %v", metric, scope, err)
			}
			var points []MetricPoint
			for _, scoped := range series {
				points = append(points, scoped...)
			}
			if len(points) != 1 || !points[0].Day.Equal(measured) || math.Abs(points[0].Value-0.2) > 1e-9 {
				t.Errorf("%s %s: %+v, want one point %s = 0.2 (the measured repo alone; no point where no case was measured)",
					metric, scope, series, measured.Format(time.DateOnly))
			}
		}
	}
}
