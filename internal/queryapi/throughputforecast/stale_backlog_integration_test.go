//go:build integration

package throughputforecast

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// wireField reads the field of v whose json tag is name, as the wire shows it: "<nil>" for a nil pointer, the
// String() of a value that has one, else fmt's form. ok is false when the type has no such field. It goes through
// reflection so this file compiles against a model that does not declare the field yet.
func wireField(v any, name string) (string, bool) {
	value := reflect.Indirect(reflect.ValueOf(v))
	for i := 0; i < value.NumField(); i++ {
		if strings.Split(value.Type().Field(i).Tag.Get("json"), ",")[0] != name {
			continue
		}
		field := value.Field(i)
		if field.Kind() == reflect.Pointer {
			if field.IsNil() {
				return "<nil>", true
			}
			field = field.Elem()
		}
		if stringer, isStringer := field.Interface().(fmt.Stringer); isStringer {
			return stringer.String(), true
		}
		return fmt.Sprint(field.Interface()), true
	}
	return "", false
}

// CHAOS-8167: a backlog read from a work-item day older than the forecast window is not a healthy number. The answer
// says STALE and names the day. CHAOS-8172: estimateCoverage says how many coverage grains it read, so "no coverage
// rows" (0) is not the same answer as a measured zero. The window start is today minus historyWeeks weeks, the bound
// the throughput history and the current WIP reads already use.
func TestThroughputForecastMarksAStaleBacklogDay(t *testing.T) {
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
		 story_points_completed, computed_at, org_id, row_view)
		VALUES (?, 'jira', 'S', ?, 'T', 0, ?, 0, 0, ?, 0, 0, 0, ?, ?, '')`
	coverage := `INSERT INTO estimate_coverage_metrics_daily
		(day, provider, work_scope_id, team_id, team_name, estimated_count, unestimated_count, backlog_size, ratio,
		 computed_at, org_id)
		VALUES (?, 'jira', 'S', ?, 'T', ?, ?, ?, NULL, ?, ?)`

	type workRow struct {
		team           string
		day            string
		completed, wip uint32
	}
	type coverageRow struct {
		team                            string
		day                             string
		estimated, unestimated, backlog uint32
	}
	supplied := 0
	for _, tc := range []struct {
		name         string
		teams        []string
		backlogInput *int
		work         []workRow
		coverage     []coverageRow
		wantState    string
		wantAsOf     string
		wantBacklog  int
		wantGrains   string
	}{
		{
			name:  "the ticket shape: one team whose newest work-item day is 2025-05-03, coverage rows from 2026-10-01",
			teams: []string{"t7"},
			work:  []workRow{{"t7", "2025-05-03", 1, 0}},
			coverage: []coverageRow{
				{"t7", "2026-10-01", 0, 1, 1},
			},
			wantState: "STALE", wantAsOf: "2025-05-03", wantBacklog: 0, wantGrains: "1",
		},
		{
			name:  "two teams, one stale: the sum is kept, the answer names the stale day",
			teams: []string{"ta", "tb"},
			work: []workRow{
				{"ta", "2026-09-29", 3, 2}, {"ta", "2026-09-30", 2, 3}, {"ta", "2026-10-01", 2, 4},
				{"tb", "2026-05-01", 1, 3},
			},
			wantState: "STALE", wantAsOf: "2026-05-01", wantBacklog: 7, wantGrains: "0",
		},
		{
			name:      "a measured zero inside the window is a healthy zero",
			teams:     []string{"tc"},
			work:      []workRow{{"tc", "2026-09-30", 1, 1}, {"tc", "2026-10-01", 2, 0}},
			coverage:  []coverageRow{{"tc", "2026-10-01", 0, 0, 0}},
			wantState: "MEASURED", wantAsOf: "2026-10-01", wantBacklog: 0, wantGrains: "1",
		},
		{
			name:      "no work-item row and no coverage row",
			teams:     []string{"td"},
			wantState: "NO_ROWS", wantAsOf: "<nil>", wantBacklog: 0, wantGrains: "0",
		},
		{
			name:      "a newest row with every measure 0 is a retraction shape, not a measurement: NO_ROWS, not STALE",
			teams:     []string{"tf"},
			work:      []workRow{{"tf", "2025-05-03", 0, 0}},
			wantState: "NO_ROWS", wantAsOf: "<nil>", wantBacklog: 0, wantGrains: "0",
		},
		{
			name:         "a supplied backlog of 0 is not read",
			teams:        []string{"te"},
			backlogInput: &supplied,
			work:         []workRow{{"te", "2025-05-03", 1, 9}},
			wantState:    "SUPPLIED", wantAsOf: "<nil>", wantBacklog: 0, wantGrains: "0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			org := "org-8167-" + tc.teams[0]
			for _, row := range tc.work {
				if err := admin.Exec(ctx, workItem, day(row.day), row.team, row.completed, row.wip, at, org); err != nil {
					t.Fatalf("insert work item row %+v: %v", row, err)
				}
			}
			for _, row := range tc.coverage {
				if err := admin.Exec(ctx, coverage, day(row.day), row.team, row.estimated, row.unestimated, row.backlog, at, org); err != nil {
					t.Fatalf("insert coverage row %+v: %v", row, err)
				}
			}

			forecast, err := Resolve(ctx, client, org, model.ThroughputForecastInput{
				TeamIds: tc.teams, BacklogSize: tc.backlogInput, HistoryWeeks: 12,
			}, now)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if forecast.BacklogSize != tc.wantBacklog {
				t.Fatalf("backlogSize = %d, want %d", forecast.BacklogSize, tc.wantBacklog)
			}
			for _, check := range []struct {
				of   any
				name string
				want string
			}{
				{forecast, "backlogState", tc.wantState},
				{forecast, "backlogAsOfDay", tc.wantAsOf},
				{forecast.EstimateCoverage, "grainCount", tc.wantGrains},
			} {
				got, ok := wireField(check.of, check.name)
				if !ok {
					t.Fatalf("the answer has no %s field: a backlog read cannot say it is %s", check.name, tc.wantState)
				}
				if got != check.want {
					t.Fatalf("%s = %s, want %s", check.name, got, check.want)
				}
			}
		})
	}
}
