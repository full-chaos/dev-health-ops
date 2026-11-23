//go:build integration

package throughputforecast

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-7508: staleWip, estimateCoverage and wipCongestion are read from each key's newest day, and the first two have
// no window bound. The answer keeps every value and says how the newest days stand against the window start (today
// minus historyWeeks weeks) and which day is the oldest of them. The values are checked first: they must equal what
// the reads served before the new fields existed.
func TestThroughputForecastLatestDayFieldsReadOutsideTheWindow(t *testing.T) {
	ctx, admin, client := startThroughputForecastSchema(t)

	now := time.Date(2026, 10, 2, 21, 35, 0, 0, time.UTC)
	at := time.Date(2026, 10, 2, 1, 0, 0, 0, time.UTC)
	day := func(value string) time.Time {
		parsed, err := time.Parse("2006-01-02", value)
		if err != nil {
			t.Fatalf("day %q: %v", value, err)
		}
		return parsed
	}

	workItem := `INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, team_name, items_started, items_completed, items_started_unassigned,
		 items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day, bug_completed_ratio,
		 story_points_completed, wip_age_p50_hours, wip_age_p90_hours, computed_at, org_id, row_view)
		VALUES (?, 'jira', 'S', ?, 'T', 0, ?, 0, 0, ?, 0, 0, 0, ?, ?, ?, ?, '')`
	coverage := `INSERT INTO estimate_coverage_metrics_daily
		(day, provider, work_scope_id, team_id, team_name, estimated_count, unestimated_count, backlog_size, ratio,
		 computed_at, org_id)
		VALUES (?, 'jira', 'S', ?, 'T', ?, ?, ?, NULL, ?, ?)`

	type workRow struct {
		team           string
		day            string
		completed, wip uint32
		p50, p90       *float64
	}
	type coverageRow struct {
		team                            string
		day                             string
		estimated, unestimated, backlog uint32
	}
	age := func(v float64) *float64 { return &v }

	// Team a: a row on every day of the two-week window (2026-09-18 .. 2026-10-01), WIP 2, and WIP 4 with wip ages
	// on its newest day. Team b: ONE work-item row and ONE coverage row on 2026-09-02, 30 days back, before the window.
	teamA := func(team string) []workRow {
		var rows []workRow
		for d := day("2026-09-18"); d.Before(day("2026-10-01")); d = d.AddDate(0, 0, 1) {
			rows = append(rows, workRow{team, d.Format("2006-01-02"), 1, 2, nil, nil})
		}
		return append(rows, workRow{team, "2026-10-01", 1, 4, age(10), age(20)})
	}
	teamB := workRow{"tb", "2026-09-02", 1, 3, age(100), age(150)}
	coverageA := coverageRow{"ta", "2026-10-01", 1, 1, 2}
	coverageB := coverageRow{"tb", "2026-09-02", 2, 3, 5}

	type want struct {
		// values served before this change
		staleP50, staleP90 string // "<absent>" when staleWip is null
		estimated          int
		unestimated        int
		coverageBacklog    int
		ratio              string
		backlog            int
		wipValue           float64
		// the new fields
		staleState, staleAsOf, staleGrains string
		coverageState, coverageAsOf        string
		coverageGrains                     int
		wipState, wipAsOf, wipGrains       string
		backlogState, backlogAsOf          string
	}
	for index, tc := range []struct {
		name     string
		teams    []string
		work     []workRow
		coverage []coverageRow
		want     want
	}{
		{
			name:     "a fresh team and a team whose newest day is 30 days back",
			teams:    []string{"ta", "tb"},
			work:     append(teamA("ta"), teamB),
			coverage: []coverageRow{coverageA, coverageB},
			want: want{
				staleP50: "55", staleP90: "85", estimated: 3, unestimated: 4, coverageBacklog: 7, ratio: "0.42857142857142855",
				backlog: 7, wipValue: 4.0 / (30.0 / 14.0),
				staleState: "STALE", staleAsOf: "2026-09-02", staleGrains: "2",
				coverageState: "STALE", coverageAsOf: "2026-09-02", coverageGrains: 2,
				wipState: "MEASURED", wipAsOf: "2026-10-01", wipGrains: "1",
				backlogState: "STALE", backlogAsOf: "2026-09-02",
			},
		},
		{
			name:     "the ticket shape: one team, every newest row before the window",
			teams:    []string{"tb"},
			work:     []workRow{teamB},
			coverage: []coverageRow{coverageB},
			want: want{
				staleP50: "<absent>", estimated: 2, unestimated: 3, coverageBacklog: 5, ratio: "0.4", backlog: 3,
				coverageState: "STALE", coverageAsOf: "2026-09-02", coverageGrains: 1,
				wipState: "NO_ROWS", wipAsOf: "<nil>", wipGrains: "0",
				backlogState: "STALE", backlogAsOf: "2026-09-02",
			},
		},
		{
			name:     "every newest day inside the window",
			teams:    []string{"ta"},
			work:     teamA("ta"),
			coverage: []coverageRow{coverageA},
			want: want{
				staleP50: "10", staleP90: "20", estimated: 1, unestimated: 1, coverageBacklog: 2, ratio: "0.5",
				backlog: 4, wipValue: 4.0 / (30.0 / 14.0),
				staleState: "MEASURED", staleAsOf: "2026-10-01", staleGrains: "1",
				coverageState: "MEASURED", coverageAsOf: "2026-10-01", coverageGrains: 1,
				wipState: "MEASURED", wipAsOf: "2026-10-01", wipGrains: "1",
				backlogState: "MEASURED", backlogAsOf: "2026-10-01",
			},
		},
		{
			name:  "two teams inside the window: current WIP is as of the older newest day",
			teams: []string{"ta", "tc"},
			work: append(teamA("ta"),
				workRow{"tc", "2026-09-19", 1, 5, nil, nil}, workRow{"tc", "2026-09-20", 1, 5, nil, nil}),
			coverage: []coverageRow{coverageA},
			want: want{
				staleP50: "10", staleP90: "20", estimated: 1, unestimated: 1, coverageBacklog: 2, ratio: "0.5",
				backlog: 9, wipValue: 9.0 / (40.0 / 14.0),
				staleState: "MEASURED", staleAsOf: "2026-10-01", staleGrains: "1",
				coverageState: "MEASURED", coverageAsOf: "2026-10-01", coverageGrains: 1,
				wipState: "MEASURED", wipAsOf: "2026-09-20", wipGrains: "2",
				backlogState: "MEASURED", backlogAsOf: "2026-09-20",
			},
		},
		{
			name:  "a key whose newest row in the window is a retraction shape is not a measured key",
			teams: []string{"ta", "tr"},
			work: append(teamA("ta"),
				workRow{"tr", "2026-09-21", 1, 6, nil, nil}, workRow{"tr", "2026-09-25", 0, 0, nil, nil}),
			coverage: []coverageRow{coverageA},
			want: want{
				staleP50: "10", staleP90: "20", estimated: 1, unestimated: 1, coverageBacklog: 2, ratio: "0.5",
				backlog: 4, wipValue: 4.0 / (36.0 / 14.0),
				staleState: "MEASURED", staleAsOf: "2026-10-01", staleGrains: "1",
				coverageState: "MEASURED", coverageAsOf: "2026-10-01", coverageGrains: 1,
				wipState: "MEASURED", wipAsOf: "2026-10-01", wipGrains: "1",
				backlogState: "MEASURED", backlogAsOf: "2026-10-01",
			},
		},
		{
			name:  "no coverage row: the zero object says NO_ROWS",
			teams: []string{"tz"},
			work:  teamA("tz"),
			want: want{
				staleP50: "10", staleP90: "20", ratio: "0",
				backlog: 4, wipValue: 4.0 / (30.0 / 14.0),
				staleState: "MEASURED", staleAsOf: "2026-10-01", staleGrains: "1",
				coverageState: "NO_ROWS", coverageAsOf: "<nil>", coverageGrains: 0,
				wipState: "MEASURED", wipAsOf: "2026-10-01", wipGrains: "1",
				backlogState: "MEASURED", backlogAsOf: "2026-10-01",
			},
		},
		{
			name:     "no work-item row: the no-history answer reads no current WIP",
			teams:    []string{"td"},
			coverage: []coverageRow{{"td", "2026-10-01", 1, 0, 1}},
			want: want{
				staleP50: "<absent>", estimated: 1, unestimated: 0, coverageBacklog: 1, ratio: "1",
				coverageState: "MEASURED", coverageAsOf: "2026-10-01", coverageGrains: 1,
				wipState: "NO_ROWS", wipAsOf: "<nil>", wipGrains: "0",
				backlogState: "NO_ROWS", backlogAsOf: "<nil>",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			org := fmt.Sprintf("org-7508-%d", index)
			for _, row := range tc.work {
				if err := admin.Exec(ctx, workItem, day(row.day), row.team, row.completed, row.wip, row.p50, row.p90, at, org); err != nil {
					t.Fatalf("insert work item row %+v: %v", row, err)
				}
			}
			for _, row := range tc.coverage {
				if err := admin.Exec(ctx, coverage, day(row.day), row.team, row.estimated, row.unestimated, row.backlog, at, org); err != nil {
					t.Fatalf("insert coverage row %+v: %v", row, err)
				}
			}

			forecast, err := Resolve(ctx, client, org, model.ThroughputForecastInput{TeamIds: tc.teams, HistoryWeeks: 2}, now)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			checkLatestDayValues(t, forecast, tc.want.staleP50, tc.want.staleP90, tc.want.estimated, tc.want.unestimated,
				tc.want.coverageBacklog, tc.want.ratio, tc.want.backlog, tc.want.wipValue, tc.want.wipState == "NO_ROWS")

			checks := []latestDayFieldCheck{
				{forecast, "backlogState", tc.want.backlogState},
				{forecast, "backlogAsOfDay", tc.want.backlogAsOf},
				{forecast.EstimateCoverage, "state", tc.want.coverageState},
				{forecast.EstimateCoverage, "asOfDay", tc.want.coverageAsOf},
				{forecast, "wipCongestionState", tc.want.wipState},
				{forecast, "wipCongestionAsOfDay", tc.want.wipAsOf},
				{forecast, "wipCongestionGrainCount", tc.want.wipGrains},
			}
			if forecast.StaleWip != nil {
				checks = append(checks,
					latestDayFieldCheck{forecast.StaleWip, "state", tc.want.staleState},
					latestDayFieldCheck{forecast.StaleWip, "asOfDay", tc.want.staleAsOf},
					latestDayFieldCheck{forecast.StaleWip, "grainCount", tc.want.staleGrains})
			}
			for _, check := range checks {
				got, ok := wireField(check.of, check.name)
				if !ok {
					t.Fatalf("the answer has no %s field: a value read from a newest day cannot say which day it is", check.name)
				}
				if got != check.want {
					t.Fatalf("%s = %s, want %s", check.name, got, check.want)
				}
			}
			if forecast.EstimateCoverage.GrainCount != tc.want.coverageGrains {
				t.Fatalf("estimateCoverage.grainCount = %d, want %d", forecast.EstimateCoverage.GrainCount, tc.want.coverageGrains)
			}
		})
	}
}

type latestDayFieldCheck struct {
	of   any
	name string
	want string
}

// checkLatestDayValues holds the values the reads served before CHAOS-7508: the new fields must not move them.
func checkLatestDayValues(t *testing.T, forecast *model.ThroughputForecast, staleP50, staleP90 string,
	estimated, unestimated, coverageBacklog int, ratio string, backlog int, wipValue float64, noHistory bool,
) {
	t.Helper()
	switch {
	case staleP50 == "<absent>":
		if forecast.StaleWip != nil {
			t.Fatalf("staleWip = %+v, want absent", *forecast.StaleWip)
		}
	case forecast.StaleWip == nil:
		t.Fatalf("staleWip is absent, want p50 %s p90 %s", staleP50, staleP90)
	default:
		p50, _ := wireField(forecast.StaleWip, "p50AgeHours")
		p90, _ := wireField(forecast.StaleWip, "p90AgeHours")
		if p50 != staleP50 || p90 != staleP90 {
			t.Fatalf("staleWip ages = %s / %s, want %s / %s", p50, p90, staleP50, staleP90)
		}
	}
	coverage := forecast.EstimateCoverage
	if coverage == nil {
		t.Fatal("estimateCoverage is null: it is never null (the zero object when no coverage row)")
	}
	gotRatio, _ := wireField(coverage, "ratio")
	if coverage.EstimatedCount != estimated || coverage.UnestimatedCount != unestimated ||
		coverage.BacklogSize != coverageBacklog || gotRatio != ratio {
		t.Fatalf("estimateCoverage = %d / %d / %d ratio %s, want %d / %d / %d ratio %s", coverage.EstimatedCount,
			coverage.UnestimatedCount, coverage.BacklogSize, gotRatio, estimated, unestimated, coverageBacklog, ratio)
	}
	if forecast.BacklogSize != backlog {
		t.Fatalf("backlogSize = %d, want %d", forecast.BacklogSize, backlog)
	}
	if noHistory {
		if forecast.WipCongestion.Value != 0 {
			t.Fatalf("wipCongestion.value = %v, want 0 (no current WIP read)", forecast.WipCongestion.Value)
		}
		return
	}
	if math.Abs(forecast.WipCongestion.Value-wipValue) > 1e-9 {
		t.Fatalf("wipCongestion.value = %v, want %v", forecast.WipCongestion.Value, wipValue)
	}
}
