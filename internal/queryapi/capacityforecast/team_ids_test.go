package capacityforecast

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-7964: capacityForecast reads EVERY selected team. Before, only the
// single teamId reached ClickHouse and the list of teams the web sends was
// reduced to its first element.

func strPtr(value string) *string { return &value }

func TestSelectedTeams(t *testing.T) {
	cases := []struct {
		name      string
		input     model.CapacityForecastInput
		wantIDs   []string
		wantLabel *string
	}{
		{"nothing is org-wide", model.CapacityForecastInput{}, nil, nil},
		{"deprecated teamId is a one-item list", model.CapacityForecastInput{TeamID: strPtr("a")}, []string{"a"}, strPtr("a")},
		{"deprecated empty teamId is org-wide and echoed as sent", model.CapacityForecastInput{TeamID: strPtr("")}, nil, strPtr("")},
		{"one teamIds item is labelled", model.CapacityForecastInput{TeamIds: []string{"a"}}, []string{"a"}, strPtr("a")},
		{"two teams carry no label", model.CapacityForecastInput{TeamIds: []string{"a", "b"}}, []string{"a", "b"}, nil},
		{"teamIds wins over teamId", model.CapacityForecastInput{TeamID: strPtr("z"), TeamIds: []string{"a", "b"}}, []string{"a", "b"}, nil},
		{"repeats and empties collapse, order kept", model.CapacityForecastInput{TeamIds: []string{"b", "", "a", "b"}}, []string{"b", "a"}, nil},
		{"repeats of one team are that team", model.CapacityForecastInput{TeamIds: []string{"a", "a"}}, []string{"a"}, strPtr("a")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ids, label := selectedTeams(&tc.input)
			if !reflect.DeepEqual(ids, tc.wantIDs) {
				t.Errorf("ids: got %v, want %v", ids, tc.wantIDs)
			}
			switch {
			case label == nil && tc.wantLabel != nil, label != nil && tc.wantLabel == nil:
				t.Errorf("label: got %v, want %v", label, tc.wantLabel)
			case label != nil && *label != *tc.wantLabel:
				t.Errorf("label: got %q, want %q", *label, *tc.wantLabel)
			}
		})
	}
}

func runWith(t *testing.T, input *model.CapacityForecastInput) (*model.CapacityForecast, *fakeClient) {
	t.Helper()
	input.HistoryDays, input.Simulations = 30, 50
	client := &fakeClient{responses: []*fakeRowScanner{
		throughputRows(t, 2, 4, 6, 8, 10),
		{rows: [][]any{{uint64(30)}}},
	}}
	got, err := ResolveForecast(context.Background(), client, "org-7", input, day(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("ResolveForecast: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want a forecast")
	}
	return got, client
}

func TestResolveForecastBindsEverySelectedTeamUnderIN(t *testing.T) {
	got, client := runWith(t, &model.CapacityForecastInput{TeamIds: []string{"team-a", "team-b"}})

	if len(client.statements) != 2 {
		t.Fatalf("queries: got %d, want throughput and backlog", len(client.statements))
	}
	for index, statement := range client.statements {
		if !strings.Contains(statement, "team_id IN {team_ids:Array(String)}") {
			t.Errorf("statement %d has no typed IN predicate:\n%s", index, statement)
		}
		if strings.Contains(statement, "team_id = {team_id:String}") {
			t.Errorf("statement %d still filters on one team:\n%s", index, statement)
		}
		if strings.Contains(statement, "team-a") || strings.Contains(statement, "team-b") {
			t.Errorf("statement %d carries a team id in its text; ids must be bound:\n%s", index, statement)
		}
		value, ok := bindingValue(client.bindings[index], "team_ids")
		if !ok {
			t.Fatalf("statement %d binds no team_ids", index)
		}
		if want := []string{"team-a", "team-b"}; !reflect.DeepEqual(value, want) {
			t.Errorf("statement %d team_ids: got %#v, want a typed []string %v", index, value, want)
		}
	}
	// The backlog statement repeats its predicate in the max(day) subquery.
	if n := strings.Count(client.statements[1], "team_id IN {team_ids:Array(String)}"); n != 2 {
		t.Errorf("backlog: the IN predicate appears %d times, want 2 (outer and max(day) subquery)", n)
	}
	if got.TeamID != nil {
		t.Errorf("teamId: got %q, want null for several teams (no invented first team)", *got.TeamID)
	}
}

func TestResolveForecastOneTeamKeepsTheSingleBind(t *testing.T) {
	for name, input := range map[string]*model.CapacityForecastInput{
		"teamIds one item":     {TeamIds: []string{"team-a"}},
		"deprecated teamId":    {TeamID: strPtr("team-a")},
		"teamIds beats teamId": {TeamID: strPtr("team-z"), TeamIds: []string{"team-a"}},
	} {
		t.Run(name, func(t *testing.T) {
			got, client := runWith(t, input)
			for index := range client.statements {
				if !strings.Contains(client.statements[index], "team_id = {team_id:String}") {
					t.Errorf("statement %d: single-team text changed:\n%s", index, client.statements[index])
				}
				if value, _ := bindingValue(client.bindings[index], "team_id"); value != "team-a" {
					t.Errorf("statement %d team_id: got %v, want team-a", index, value)
				}
			}
			if got.TeamID == nil || *got.TeamID != "team-a" {
				t.Errorf("teamId: got %v, want team-a", got.TeamID)
			}
		})
	}
}

func TestResolveForecastNoTeamIsOrgWide(t *testing.T) {
	_, client := runWith(t, &model.CapacityForecastInput{TeamIds: []string{"", ""}})
	for index, statement := range client.statements {
		if strings.Contains(statement, "{team_id") {
			t.Errorf("statement %d filters on a team for an empty selection:\n%s", index, statement)
		}
	}
}

// The table holds one pre-aggregated row per (org, provider, day, work_scope,
// team) with no item key (chmigrate/baseline/head.json: ORDER BY org_id,
// provider, day, work_scope_id, team_id). So the only double-count risk under
// IN is a join or a second source multiplying rows; pin that the read stays a
// single-table SUM, deduplicated by FINAL.
func TestMultiTeamReadsAreASingleTableSumWithoutAJoin(t *testing.T) {
	_, client := runWith(t, &model.CapacityForecastInput{TeamIds: []string{"team-a", "team-b"}})
	for index, statement := range client.statements {
		if strings.Contains(strings.ToUpper(statement), "JOIN") {
			t.Errorf("statement %d joins; a join could count a row once per team:\n%s", index, statement)
		}
		if got := strings.Count(statement, "FROM work_item_metrics_daily FINAL"); got < 1 {
			t.Errorf("statement %d does not read work_item_metrics_daily FINAL:\n%s", index, statement)
		}
	}
	if !strings.Contains(client.statements[0], "GROUP BY day") || strings.Contains(client.statements[0], "GROUP BY day,") {
		t.Errorf("throughput must group by day alone so teams add up per day:\n%s", client.statements[0])
	}
}
