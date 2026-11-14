//go:build integration

package analytics

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// A repo-day whose flake is unknown is written with a 0/0 flake_cases/distinct_cases
// pair (the TestOps test-metrics writer). The org series sums the pairs: the
// unknown repo adds nothing, so the org value is the known repo's, and a day on
// which every repo is unknown has no denominator and reads unavailable. A NULL
// pair would blank the org day whatever the other repos hold, which is why the
// writer does not use one.
func TestOrgFlakeSeriesIgnoresAnUnknownRepoDayAndIsUnavailableWhenAllAreUnknown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	const orgID = "seeded-flake-unknown-org"
	day := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	seed := func(day time.Time, repo string, flake, distinct any) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO testops_test_metrics_daily
(repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count, pass_rate, failure_rate,
 flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, org_id, computed_at, flake_cases, distinct_cases)
VALUES (?, ?, 10, 10, 0, 0, 0, 1, 0, 0, 0, 1, 0, ?, now(), ?, ?)`,
			uuid.MustParse(repo), day, orgID, flake, distinct); err != nil {
			t.Fatalf("seed %s: %v", repo, err)
		}
	}
	known := "00000000-0000-4000-8000-0000000000a1"
	unknown := "00000000-0000-4000-8000-0000000000a2"
	// Day 1-2: one known repo (2 flaky of 8 distinct) and one unknown repo (0/0).
	seed(day, known, uint32(2), uint32(8))
	seed(day, unknown, uint32(0), uint32(0))
	// Day 1-3: every repo unknown.
	seed(day.AddDate(0, 0, 1), known, uint32(0), uint32(0))
	seed(day.AddDate(0, 0, 1), unknown, uint32(0), uint32(0))
	// Day 1-4: a NULL pair next to a known repo, the shape the writer must not produce.
	seed(day.AddDate(0, 0, 2), known, uint32(2), uint32(8))
	seed(day.AddDate(0, 0, 2), unknown, nil, nil)

	req, err := TimeseriesRequestFromInput(model.TimeseriesRequestInput{
		Dimension: model.DimensionInputTeam, Measure: model.MeasureInputTestFlakeRate, Interval: model.BucketIntervalInputDay,
		DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
	})
	if err != nil {
		t.Fatal(err)
	}
	query, ok, err := CompileOrgTimeseries(req, orgID, 120, false, nil)
	if err != nil || !ok {
		t.Fatalf("CompileOrgTimeseries ok=%v err=%v", ok, err)
	}
	series, err := ExecuteOrgTimeseries(ctx, client, query, "team", "test_flake_rate", MeasureTestFlakeRate)
	if err != nil {
		t.Fatal(err)
	}
	byDay := map[string]*float64{}
	for _, bucket := range series.Buckets {
		byDay[time.Time(bucket.Date).UTC().Format(time.DateOnly)] = bucket.Value
	}
	if v := byDay["2026-01-02"]; v == nil || math.Abs(*v-25) > 1e-9 {
		t.Errorf("one known repo (2 of 8) and one unknown repo: org value = %v, want 25", v)
	}
	if v := byDay["2026-01-03"]; v != nil {
		t.Errorf("every repo unknown: org value = %v, want unavailable (nil)", *v)
	}
	if v := byDay["2026-01-04"]; v != nil {
		t.Errorf("a NULL pair next to a known repo: org value = %v, want nil (it blanks the day, the reason the writer writes 0/0)", *v)
	}
}
