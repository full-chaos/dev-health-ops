//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// The daily writer stores the two counts of failure_recurrence_score: the failed names of the day that also failed in
// the history window, and the distinct failed names of the day. A day with no failed name stores 0 and 0.
func TestTestopsTestMetricsStoreFailureRecurrenceCounts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := startDefaultBranchClickHouse(ctx, t)
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	failing := uuid.MustParse("00000000-0000-4000-8000-0000000000e1")
	green := uuid.MustParse("00000000-0000-4000-8000-0000000000e2")
	status := func(s string) *string { return &s }

	// Three distinct failed names (one failed twice, a retry), one of them failed in the history window: 1 of 3.
	failingDay := testops.NewTestAccumulator(failing, "repo-e1", nil)
	for _, row := range []struct{ name, status string }{
		{"a", "failed"}, {"a", "failed"}, {"b", "failed"}, {"c", "failed"}, {"d", "passed"},
	} {
		failingDay.AddCaseRow(testops.CaseRow{RepoID: failing, CaseName: row.name, Status: status(row.status)})
	}
	greenDay := testops.NewTestAccumulator(green, "repo-e2", nil)
	greenDay.AddCaseRow(testops.CaseRow{RepoID: green, CaseName: "a", Status: status("passed")})
	history := map[string]struct{}{"a": {}, "z": {}}
	var metrics []testops.TestMetric
	for _, accumulator := range []*testops.TestAccumulator{failingDay, greenDay} {
		for _, metric := range accumulator.Finish(history) {
			metric.OrgID = defaultBranchTestOrg
			metrics = append(metrics, metric)
		}
	}
	if len(metrics) != 2 || metrics[0].FailureRecurrence != 1.0/3 {
		t.Fatalf("metrics = %+v, want 2 with recurrence 1/3 first", metrics)
	}
	if _, err := writeTestopsTestMetrics(ctx, conn, defaultBranchTestOrg, day, day, metrics); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name                  string
		repo                  uuid.UUID
		recurrent, distinctOf uint32
	}{
		{"three failed names, one recurrent", failing, 1, 3},
		{"no failed name", green, 0, 0},
	} {
		var recurrent, distinct *uint32
		if err := conn.QueryRow(ctx, `SELECT recurrent_failed_names, distinct_failed_names
FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ?`, defaultBranchTestOrg, c.repo).
			Scan(&recurrent, &distinct); err != nil {
			t.Fatal(err)
		}
		if recurrent == nil || distinct == nil || *recurrent != c.recurrent || *distinct != c.distinctOf {
			t.Errorf("FAIL %s: counts = %v/%v, want %d/%d", c.name, recurrenceCount(recurrent), recurrenceCount(distinct), c.recurrent, c.distinctOf)
		}
	}
}

func recurrenceCount(v *uint32) any {
	if v == nil {
		return "NULL"
	}
	return *v
}
