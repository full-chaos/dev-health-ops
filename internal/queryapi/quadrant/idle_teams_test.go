package quadrant

import (
	"context"
	"errors"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

type teamCatalogCalls struct {
	calls    int
	selected any
}

// idleFixture answers the catalog read with teams, the label read with nothing,
// and each metric read with one plotted entity ("team-a", both axes).
func idleFixture(catalog [][]any, calls *teamCatalogCalls, catalogErr error) fakeQueryClient {
	return fakeQueryClient{handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "is_active = 1") && strings.Contains(query, "name AS team_name") {
			calls.calls++
			calls.selected = bindingValue(bindings, "scope_team_ids")
			if catalogErr != nil {
				return nil, catalogErr
			}
			return &fixtureRowScanner{rows: catalog}, nil
		}
		if strings.Contains(query, "SELECT toString(id) AS team_id") {
			return &fixtureRowScanner{}, nil
		}
		return &fixtureRowScanner{rows: [][]any{{day(2024, 1, 1), "team-a", "team-a", 5.0}}}, nil
	}}
}

func idleParams(quadrantType, scope string) Params {
	return Params{
		Type: quadrantType, ScopeType: scope, RangeDays: 30, Bucket: "week",
		StartDate: timePtr(day(2024, 1, 1)), EndDate: timePtr(day(2024, 1, 21)),
	}
}

func TestATeamInScopeWithoutAPointIsANoDataPoint(t *testing.T) {
	catalog := [][]any{{"team-b", "Beta"}, {"team-a", "Alpha"}, {"team-d", "Beta"}, {"team-c", ""}}
	for _, tc := range []struct{ quadrantType, scope string }{
		{"wip_throughput", "team"},
		{"wip_throughput", "org"},
		{"cycle_throughput", "team"},
		{"review_load_latency", "team"},
		{"churn_throughput", "service"},
	} {
		t.Run(tc.quadrantType+" "+tc.scope, func(t *testing.T) {
			var calls teamCatalogCalls
			client := idleFixture(catalog, &calls, nil)
			client.t = t
			resp, err := BuildResponse(context.Background(), client, "org-1", idleParams(tc.quadrantType, tc.scope))
			if err != nil {
				t.Fatalf("BuildResponse: %v", err)
			}
			var got []string
			for _, p := range resp.Points {
				if p.NoData != (p.X == nil) || p.NoData != (p.Y == nil) {
					t.Errorf("%s: NoData = %v with x = %v, y = %v: the flag and the null values must agree", p.EntityID, p.NoData, p.X, p.Y)
				}
				got = append(got, p.EntityID)
			}
			// team-a is plotted; the others follow, by label then id (team-c has no name: its id is its label).
			if want := "team-a team-b team-d team-c"; strings.Join(got, " ") != want {
				t.Fatalf("points = %v, want %s", got, want)
			}
			idle := resp.Points[1]
			if idle.EntityLabel != "Beta" || idle.WindowStart != "2024-01-01" || idle.WindowEnd != "2024-01-22" || idle.Trajectory != nil || idle.EvidenceLink == "" {
				t.Errorf("no-data point = %+v, want its label, the requested window, no trajectory and the evidence link", idle)
			}
			if label := resp.Points[3].EntityLabel; label != "team-c" {
				t.Errorf("unnamed team label = %q, want its id", label)
			}
		})
	}
}

func TestOnlyTheTeamGrainListsTeams(t *testing.T) {
	for _, tc := range []struct{ name, quadrantType, scope, scopeID string }{
		{"repository grain", "wip_throughput", "repo", ""},
		{"churn at team scope is the repository grain", "churn_throughput", "team", ""},
		{"churn at org scope is the repository grain", "churn_throughput", "org", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls teamCatalogCalls
			client := idleFixture([][]any{{"team-z", "Zed"}}, &calls, nil)
			client.t = t
			resp, err := BuildResponse(context.Background(), client, "org-1", idleParams(tc.quadrantType, tc.scope))
			if err != nil {
				t.Fatalf("BuildResponse: %v", err)
			}
			if calls.calls != 0 {
				t.Errorf("the team catalog was read %d times at a grain that plots no teams", calls.calls)
			}
			for _, p := range resp.Points {
				if p.NoData {
					t.Errorf("no-data point %+v at a grain that plots no teams", p)
				}
			}
		})
	}
}

func TestSelectedTeamsNarrowTheNoDataPoints(t *testing.T) {
	var calls teamCatalogCalls
	client := idleFixture([][]any{{"team-b", "Beta"}}, &calls, nil)
	client.t = t
	params := idleParams("wip_throughput", "team")
	params.TeamIDs = []string{"team-b"}
	// resolveSelection reads the route filter first; the fixture answers every other read with the plotted row.
	resp, err := BuildResponse(context.Background(), client, "org-1", params)
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if calls.calls != 1 || calls.selected == nil {
		t.Fatalf("catalog calls = %d, scope_team_ids = %v, want one read narrowed to the selected teams", calls.calls, calls.selected)
	}
	var idle int
	for _, p := range resp.Points {
		if p.NoData {
			idle++
		}
	}
	if idle != 1 {
		t.Errorf("no-data points = %d, want the one selected idle team", idle)
	}
}

func TestAFailedTeamCatalogReadFailsTheRequest(t *testing.T) {
	var calls teamCatalogCalls
	client := idleFixture(nil, &calls, errors.New("boom"))
	client.t = t
	if _, err := BuildResponse(context.Background(), client, "org-1", idleParams("wip_throughput", "team")); err == nil {
		t.Fatal("BuildResponse succeeded with the team catalog unreadable: a missing team list must not read as no idle teams")
	}
}

func TestTheNoDataPointIsNullOnTheWireAndPlottedPointsKeepTheirBytes(t *testing.T) {
	var calls teamCatalogCalls
	client := idleFixture([][]any{{"team-b", "Beta"}}, &calls, nil)
	client.t = t
	resp, err := BuildResponse(context.Background(), client, "org-1", idleParams("wip_throughput", "team"))
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	value, err := pyjson.FromGoModel(resp)
	if err != nil {
		t.Fatalf("FromGoModel: %v", err)
	}
	body, err := pyjson.MarshalModel(value)
	if err != nil {
		t.Fatalf("MarshalModel: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, `"entity_id":"team-b","entity_label":"Beta","x":null,"y":null,`) || !strings.Contains(text, `"no_data":true`) {
		t.Errorf("no-data point not null on the wire: %s", text)
	}
	if strings.Count(text, "no_data") != 1 {
		t.Errorf("no_data must appear on the idle point only: %s", text)
	}
}
