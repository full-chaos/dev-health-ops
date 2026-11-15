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

// The TEST_FLAKE_RATE team series (the TestOps flake tile) reads the stored
// flake counts, never flake_rate: an unknown repo-day (0 flake cases of 0
// distinct cases, NULL rates) adds nothing, so the team's value is the known
// repo's, and a day on which every repo is unknown is unavailable, never 0.
func TestTeamFlakeSeriesIgnoresAnUnknownRepoDayAndIsUnavailableWhenAllAreUnknown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	const orgID = "seeded-flake-unknown-tile"
	day := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	seed := func(day time.Time, repo string, flakeRate *float64, flake, distinct uint32) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO testops_test_metrics_daily
(repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count, pass_rate, failure_rate,
 flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, team_id, org_id, computed_at, flake_cases, distinct_cases)
VALUES (?, ?, 10, 10, 0, 0, 0, 1, 0, ?, ?, 1, ?, 'team-1', ?, now(), ?, ?)`,
			uuid.MustParse(repo), day, flakeRate, flakeRate, flakeRate, orgID, flake, distinct); err != nil {
			t.Fatalf("seed %s: %v", repo, err)
		}
	}
	known := "00000000-0000-4000-8000-0000000000d1"
	unknown := "00000000-0000-4000-8000-0000000000d2"
	quarter := 0.25
	seed(day, known, &quarter, 2, 8)
	seed(day, unknown, nil, 0, 0)
	seed(day.AddDate(0, 0, 1), known, nil, 0, 0)
	seed(day.AddDate(0, 0, 1), unknown, nil, 0, 0)

	req, err := TimeseriesRequestFromInput(model.TimeseriesRequestInput{
		Dimension: model.DimensionInputTeam, Measure: model.MeasureInputTestFlakeRate, Interval: model.BucketIntervalInputDay,
		DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
	})
	if err != nil {
		t.Fatal(err)
	}
	query, err := CompileTimeseries(req, orgID, 120, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	results, err := ExecuteTimeseries(ctx, client, query, "team", "test_flake_rate")
	if err != nil {
		t.Fatal(err)
	}
	byDay := map[string]*float64{}
	for _, result := range results {
		if result.DimensionValue != "team-1" {
			continue
		}
		for _, bucket := range result.Buckets {
			byDay[time.Time(bucket.Date).UTC().Format(time.DateOnly)] = bucket.Value
		}
	}
	if v := byDay["2026-01-02"]; v == nil || math.Abs(*v-25) > 1e-9 {
		t.Errorf("one known repo (2 of 8) and one unknown repo: team value = %v, want 25", v)
	}
	if v := byDay["2026-01-03"]; v != nil {
		t.Errorf("every repo unknown: team value = %v, want unavailable (nil)", *v)
	}
}
