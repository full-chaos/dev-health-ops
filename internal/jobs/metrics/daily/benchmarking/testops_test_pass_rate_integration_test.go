//go:build integration

package benchmarking

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The pass rate benchmark series is each repo-day's passed cases over its passed and failed cases, read from the
// counts. A repo-day where every case was skipped is no sample (never the stored 0), and a newest all-skipped row is
// not replaced by an older measured row of the same key.
func TestBenchmarkPassRateSeriesLeavesOutAnAllSkippedDay(t *testing.T) {
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
		org   = "org-testops-all-skipped"
		repoA = "00000000-0000-4000-8000-0000000000e1"
		repoB = "00000000-0000-4000-8000-0000000000e2"
	)
	allSkipped := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	mixed := allSkipped.AddDate(0, 0, 1)
	recomputed := allSkipped.AddDate(0, 0, 2)
	older := time.Date(2026, 8, 5, 1, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	// storedRate is the writer's passed / total_cases; the series must not read it.
	row := func(repo string, day time.Time, passed, failed, skipped uint32, storedRate float64, computedAt time.Time) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count, pass_rate, failure_rate,
     flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, org_id, computed_at)
VALUES (toUUID(?), ?, ?, ?, ?, ?, 0, ?, 0, 0, 0, 1, 0, ?, ?)`,
			repo, day, passed+failed+skipped, passed, failed, skipped, storedRate, org, computedAt); err != nil {
			t.Fatal(err)
		}
	}
	row(repoA, allSkipped, 0, 0, 2, 0, older)
	row(repoA, mixed, 3, 1, 4, 0.375, older)
	row(repoB, mixed, 1, 1, 0, 0.5, older)
	row(repoA, recomputed, 4, 0, 0, 1, older)
	row(repoA, recomputed, 0, 0, 3, 0, newer)

	loader, err := NewClickHouseLoader(admin, org)
	if err != nil {
		t.Fatal(err)
	}
	series, err := loader.FetchMetricSeriesByScope(ctx, "pass_rate", allSkipped, recomputed, ScopeRepo)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pass_rate repo series=%+v", series)
	want := map[string]float64{repoA: 0.75, repoB: 0.5}
	got := map[string]float64{}
	for repo, points := range series {
		if len(points) != 1 || !points[0].Day.Equal(mixed) {
			t.Errorf("repo %s: %+v, want one point on %s (no sample on the all-skipped days)", repo, points, mixed.Format(time.DateOnly))
			continue
		}
		got[repo] = math.Round(points[0].Value*1e9) / 1e9
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("pass_rate repo values %v, want %v (passed / (passed + failed))", got, want)
	}
}
