//go:build integration

package capacityforecast

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// backlogWireField reads the field of v whose json tag is name, as the wire shows it: "<nil>" for a nil pointer, the
// String() of a value that has one, else fmt's form. ok is false when the type has no such field. It goes through
// reflection so this file compiles against a model that does not declare the field yet.
func backlogWireField(v any, name string) (string, bool) {
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

// CHAOS-7508: capacityForecast.backlogSize sums each key's newest work-item row with no window bound. The answer keeps
// the sum and says how the newest days stand against the window start (today minus historyDays days) and which day is
// the oldest of them. The window here is 14 days (start 2026-09-18) and team b's one row is 30 days back: STALE for
// the 14-day bound, MEASURED for any bound of 30 days or more.
func TestCapacityForecastBacklogReadOutsideTheWindowSaysSo(t *testing.T) {
	ctx, admin, client := startCapacityTeamView(t)

	now := time.Date(2026, 10, 2, 21, 35, 0, 0, time.UTC)
	at := time.Date(2026, 10, 2, 1, 0, 0, 0, time.UTC)
	day := func(value string) time.Time {
		parsed, err := time.Parse("2006-01-02", value)
		if err != nil {
			t.Fatalf("day %q: %v", value, err)
		}
		return parsed
	}

	type workRow struct {
		team           string
		day            string
		completed, wip uint32
	}
	teamA := func() []workRow {
		var rows []workRow
		for d := day("2026-09-18"); d.Before(day("2026-10-01")); d = d.AddDate(0, 0, 1) {
			rows = append(rows, workRow{"ta", d.Format("2006-01-02"), 1, 2})
		}
		return append(rows, workRow{"ta", "2026-10-01", 1, 4})
	}
	teamB := workRow{"tb", "2026-09-02", 1, 3}

	target := 5
	for index, tc := range []struct {
		name        string
		teams       []string
		work        []workRow
		targetItems *int
		wantBacklog int
		wantState   string
		wantAsOf    string
	}{
		{
			name:        "a fresh team and a team whose newest day is 30 days back",
			teams:       []string{"ta", "tb"},
			work:        append(teamA(), teamB),
			wantBacklog: 7, wantState: "STALE", wantAsOf: "2026-09-02",
		},
		{
			name:        "one team, its newest day inside the window",
			teams:       []string{"ta"},
			work:        teamA(),
			wantBacklog: 4, wantState: "MEASURED", wantAsOf: "2026-10-01",
		},
		{
			name:  "two teams inside the window: the backlog is as of the older newest day",
			teams: []string{"ta", "tc"},
			work: append(teamA(),
				workRow{"tc", "2026-09-19", 1, 5}, workRow{"tc", "2026-09-20", 1, 5}),
			wantBacklog: 9, wantState: "MEASURED", wantAsOf: "2026-09-20",
		},
		{
			name:        "a newest row with every measure 0 is a retraction shape: NO_ROWS",
			teams:       []string{"te"},
			work:        []workRow{{"te", "2026-09-20", 2, 0}, {"te", "2026-09-25", 0, 0}},
			targetItems: &target,
			wantBacklog: 0, wantState: "NO_ROWS", wantAsOf: "<nil>",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			org := fmt.Sprintf("org-7508-capacity-%d", index)
			for _, row := range tc.work {
				if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
					(day, provider, work_scope_id, team_id, team_name, items_completed, wip_count_end_of_day, computed_at, org_id, row_view)
					VALUES (?, 'jira', 'S', ?, ?, ?, ?, ?, ?, '')`,
					day(row.day), row.team, row.team, row.completed, row.wip, at, org); err != nil {
					t.Fatalf("insert %+v: %v", row, err)
				}
			}

			forecast, err := ResolveForecast(ctx, client, org, &model.CapacityForecastInput{
				TeamIds: tc.teams, TargetItems: tc.targetItems, HistoryDays: 14, Simulations: 200,
			}, now)
			if err != nil {
				t.Fatalf("ResolveForecast: %v", err)
			}
			if forecast == nil {
				t.Fatal("ResolveForecast answered null: the cell needs a forecast")
			}
			if forecast.BacklogSize != tc.wantBacklog {
				t.Fatalf("backlogSize = %d, want %d (the sum served before the new fields)", forecast.BacklogSize, tc.wantBacklog)
			}
			for _, check := range []struct{ name, want string }{
				{"backlogState", tc.wantState},
				{"backlogAsOfDay", tc.wantAsOf},
			} {
				got, ok := backlogWireField(forecast, check.name)
				if !ok {
					t.Fatalf("the answer has no %s field: a backlog read from an old day cannot say so", check.name)
				}
				if got != check.want {
					t.Fatalf("%s = %s, want %s", check.name, got, check.want)
				}
			}
		})
	}
}
