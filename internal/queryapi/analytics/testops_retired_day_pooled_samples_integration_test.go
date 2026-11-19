//go:build integration

package analytics

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// The pooled suite duration reads a repository-day only when its NEWEST testops_test_metrics_daily row is live: a retired
// day (the newest row is a marker, retracted = 1, holding no samples) admits neither the samples an older row stored nor its
// raw suites, and a day measured again after its marker reads the newest row's samples.
func TestTestopsSuiteDurationPoolsNoSampleOfARetiredDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	const orgID = "seeded-retired-pooled"
	const repo = "13928000-0000-4000-8000-0000000000e2"
	storedRetired := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	legacyRetired := storedRetired.AddDate(0, 0, 1)
	storedLive := storedRetired.AddDate(0, 0, 2)
	remeasured := storedRetired.AddDate(0, 0, 3)
	stored := time.Date(2026, 1, 8, 10, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	if err := admin.Exec(ctx, "SYSTEM STOP MERGES testops_test_metrics_daily"); err != nil {
		t.Fatal(err)
	}
	id := uuid.MustParse(repo)
	measured := func(day time.Time, at time.Time, samplesStored uint8, durations []float64) {
		t.Helper()
		services := make([]*string, len(durations))
		samples := make([]*float64, len(durations))
		for index := range durations {
			samples[index] = &durations[index]
		}
		exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, passed_count, failed_count, pass_rate, failure_rate,
 flake_rate, total_suites, team_id, org_id, computed_at, flake_cases, distinct_cases, pooled_service_ids, pooled_durations,
 pooled_samples_stored) VALUES (?, ?, 2, 2, 0, 1, 0, 0, 1, 'team-1', ?, ?, 0, 2, ?, ?, ?)`,
			id, day, orgID, at, services, samples, samplesStored)
	}
	marker := func(day time.Time, at time.Time) {
		t.Helper()
		exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, team_id, org_id, computed_at, retracted) VALUES (?, ?, 'team-1', ?, ?, 1)`,
			id, day, orgID, at)
	}
	suite := func(run string, day time.Time, duration float64) {
		t.Helper()
		exec(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count,
 duration_seconds, started_at, finished_at, org_id, last_synced) VALUES (?, ?, ?, 'pkg', 1, 1, 0, 0, ?, ?, ?, ?, ?)`,
			id, run, run+"-s", duration, day.Add(10*time.Hour), day.Add(11*time.Hour), orgID, stored)
	}

	// (a) A day whose older row stored its samples, then retired.
	measured(storedRetired, stored, 1, []float64{30})
	marker(storedRetired, stored.Add(time.Hour))
	suite("r-stored-retired", storedRetired, 31)
	// (b) A legacy day (no stored samples, so the raw suites would be read), then retired.
	measured(legacyRetired, stored, 0, nil)
	marker(legacyRetired, stored.Add(time.Hour))
	suite("r-legacy-retired", legacyRetired, 32)
	// (c) A live day with stored samples: its samples, never the raw suites.
	measured(storedLive, stored, 1, []float64{40})
	suite("r-stored-live", storedLive, 41)
	// (d) A retired day measured again after its marker: the newest row's samples.
	measured(remeasured, stored, 1, []float64{50})
	marker(remeasured, stored.Add(time.Hour))
	measured(remeasured, stored.Add(2*time.Hour), 1, []float64{55})
	suite("r-remeasured", remeasured, 56)
	for _, q := range ownerSeedSQL(orgID, repo, "team-1") {
		exec(q)
	}

	req, err := TimeseriesRequestFromInput(model.TimeseriesRequestInput{
		Dimension: model.DimensionInputTeam, Measure: model.MeasureInputTestSuiteDurationP95, Interval: model.BucketIntervalInputDay,
		DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
	})
	if err != nil {
		t.Fatal(err)
	}
	query, err := CompileTimeseries(req, orgID, 120, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	results, err := ExecuteTimeseries(ctx, client, query, "team", strings.ToLower(string(model.MeasureInputTestSuiteDurationP95)))
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
	for _, absent := range []struct {
		name string
		day  time.Time
	}{{"a stored-samples day retired", storedRetired}, {"a legacy day retired", legacyRetired}} {
		if v := byDay[absent.day.Format(time.DateOnly)]; v != nil {
			t.Errorf("%s has the value %v, want none", absent.name, *v)
		}
	}
	for _, present := range []struct {
		name string
		day  time.Time
		want float64
	}{{"a live day with stored samples", storedLive, 40}, {"a retired day measured again", remeasured, 55}} {
		v := byDay[present.day.Format(time.DateOnly)]
		if v == nil {
			t.Errorf("%s has no value, want %v", present.name, present.want)
			continue
		}
		if math.Abs(*v-present.want) > 1e-9 {
			t.Errorf("%s = %v, want %v (the newest row's stored samples)", present.name, *v, present.want)
		}
	}
}
