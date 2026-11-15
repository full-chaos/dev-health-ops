//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// On the real schema: a day that measured no case is stored with NULL in
// flake_rate, retry_dependency_rate and failure_recurrence_score, a measured day
// with its rates. A batch that appends the three as plain float64 values (the
// form of a writer built before the columns were Nullable) still stores them.
func TestTestopsTestMetricsStoreNullRatesForADayWithNoMeasuredCase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := startDefaultBranchClickHouse(ctx, t)
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	unknown := uuid.MustParse("00000000-0000-4000-8000-0000000000c1")
	measured := uuid.MustParse("00000000-0000-4000-8000-0000000000c2")
	plain := uuid.MustParse("00000000-0000-4000-8000-0000000000c3")
	metrics := []testops.TestMetric{
		{RepoID: unknown, TotalCases: 12, FlakeUnknown: true, OrgID: defaultBranchTestOrg},
		{RepoID: measured, TotalCases: 12, FlakeCases: 2, DistinctCases: 8, FlakeRate: 0.25,
			RetryDependencyRate: 0.125, FailureRecurrence: 0.5, OrgID: defaultBranchTestOrg},
	}
	if _, err := writeTestopsTestMetrics(ctx, conn, defaultBranchTestOrg, day, day, metrics); err != nil {
		t.Fatal(err)
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO testops_test_metrics_daily (
		repo_id, day, total_cases, flake_rate, retry_dependency_rate, failure_recurrence_score, org_id, computed_at)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(plain, chDate(day), uint32(12), 0.75, 0.25, 0.125, defaultBranchTestOrg, day); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}

	read := func(repo uuid.UUID) [3]*float64 {
		t.Helper()
		var flake, retry, recurrence *float64
		if err := conn.QueryRow(ctx, `SELECT flake_rate, retry_dependency_rate, failure_recurrence_score
FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ?`, defaultBranchTestOrg, repo).
			Scan(&flake, &retry, &recurrence); err != nil {
			t.Fatal(err)
		}
		return [3]*float64{flake, retry, recurrence}
	}
	for name, test := range map[string]struct {
		repo uuid.UUID
		want *[3]float64
	}{
		"no measured case":           {repo: unknown},
		"measured":                   {repo: measured, want: &[3]float64{0.25, 0.125, 0.5}},
		"plain float64 append (old)": {repo: plain, want: &[3]float64{0.75, 0.25, 0.125}},
	} {
		got := read(test.repo)
		for index, value := range got {
			switch {
			case test.want == nil && value != nil:
				t.Errorf("%s: column %d = %v, want NULL", name, index, *value)
			case test.want != nil && (value == nil || *value != test.want[index]):
				t.Errorf("%s: column %d = %v, want %v", name, index, value, test.want[index])
			}
		}
	}
}
