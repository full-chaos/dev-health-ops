package quadrant

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// CHAOS-9693: a churn_throughput point is a repository, and the Landscape Teams tab needs a team's churn. The point
// names the active teams that own the repository with their share of it, so a consumer sums churn x share per team
// (D5998: an additive measure is the sum of value x share). The point's own x and y stay the whole repository's.
func churnClient(t *testing.T, owners func(bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)) fakeQueryClient {
	return fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "AS repo_name"):
			return owners(bindings)
		case strings.Contains(query, "total_loc_touched"):
			return &fixtureRowScanner{rows: [][]any{
				{day(2024, 1, 1), "acme/shared", "acme/shared", 100.0},
				{day(2024, 1, 1), "acme/solo", "acme/solo", 40.0},
				{day(2024, 1, 1), "acme/orphan", "acme/orphan", 7.0},
			}}, nil
		case strings.Contains(query, "prs_merged"):
			return &fixtureRowScanner{rows: [][]any{
				{day(2024, 1, 1), "acme/shared", "acme/shared", 5.0},
				{day(2024, 1, 1), "acme/solo", "acme/solo", 2.0},
				{day(2024, 1, 1), "acme/orphan", "acme/orphan", 1.0},
			}}, nil
		}
		t.Fatalf("unexpected query:\n%s", query)
		return nil, nil
	}}
}

func churnParams(teamIDs ...string) Params {
	return Params{
		Type: "churn_throughput", ScopeType: "org", RangeDays: 30, Bucket: "week",
		StartDate: timePtr(day(2024, 1, 1)), EndDate: timePtr(day(2024, 1, 8)), TeamIDs: teamIDs,
	}
}

func TestChurnPointsNameTheirOwningTeamsWithShares(t *testing.T) {
	var boundNames any
	client := churnClient(t, func(bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		boundNames = bindingValue(bindings, "owner_repo_names")
		return &fixtureRowScanner{rows: [][]any{
			{"acme/shared", "team-a", "Team A", 0.7},
			{"acme/shared", "team-b", "Team B", 0.3},
			{"acme/solo", "team-a", "Team A", 1.0},
		}}, nil
	})
	resp, err := BuildResponse(context.Background(), client, "org-1", churnParams())
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	got := map[string][]OwnerShare{}
	for _, point := range resp.Points {
		got[point.EntityID] = point.OwnerShares
	}
	want := map[string][]OwnerShare{
		"acme/shared": {{TeamID: "team-a", TeamLabel: "Team A", Share: 0.7}, {TeamID: "team-b", TeamLabel: "Team B", Share: 0.3}},
		"acme/solo":   {{TeamID: "team-a", TeamLabel: "Team A", Share: 1.0}},
		"acme/orphan": nil,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("owner shares = %+v, want %+v", got, want)
	}
	if !reflect.DeepEqual(boundNames, []string{"acme/orphan", "acme/shared", "acme/solo"}) {
		t.Fatalf("owner lookup bound repositories %v", boundNames)
	}
	for _, point := range resp.Points {
		if point.EntityID == "acme/shared" && point.X != 100.0 {
			t.Fatalf("the point's x must stay the whole repository's churn, got %v", point.X)
		}
	}
}

func TestChurnOwnerSharesNarrowToTheSelectedTeams(t *testing.T) {
	var teams any
	client := churnClient(t, func(bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		teams = bindingValue(bindings, "owner_shares_team_ids")
		return &fixtureRowScanner{}, nil
	})
	if _, err := BuildResponse(context.Background(), client, "org-1", churnParams("team-b")); err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if !reflect.DeepEqual(teams, []string{"team-b"}) {
		t.Fatalf("owner shares not narrowed to the selected team, bound %v", teams)
	}
}

func TestChurnOwnerLookupFailureDegradesToPointsWithoutOwners(t *testing.T) {
	client := churnClient(t, func([]dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return nil, errors.New("clickhouse down")
	})
	resp, err := BuildResponse(context.Background(), client, "org-1", churnParams())
	if err != nil {
		t.Fatalf("a failed owner lookup must not fail the quadrant: %v", err)
	}
	if len(resp.Points) != 3 {
		t.Fatalf("points = %d, want 3", len(resp.Points))
	}
	for _, point := range resp.Points {
		if len(point.OwnerShares) != 0 {
			t.Fatalf("point %s carries owners after a failed lookup: %+v", point.EntityID, point.OwnerShares)
		}
	}
}

func TestOtherQuadrantTypesServeNoOwnerShares(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "AS repo_name") {
			t.Fatalf("owner shares read for a type that does not serve them:\n%s", query)
		}
		switch {
		case strings.Contains(query, "reviews_given"):
			return &fixtureRowScanner{rows: [][]any{{day(2024, 1, 1), "api", "api", 6.0}}}, nil
		case strings.Contains(query, "pr_first_review_p50_hours"):
			return &fixtureRowScanner{rows: [][]any{{day(2024, 1, 1), "api", "api", 5.0}}}, nil
		}
		return nil, fmt.Errorf("unexpected query:\n%s", query)
	}}
	resp, err := BuildResponse(context.Background(), client, "org-1", Params{
		Type: "review_load_latency", ScopeType: "repo", RangeDays: 30, Bucket: "week",
		StartDate: timePtr(day(2024, 1, 1)), EndDate: timePtr(day(2024, 1, 8)),
	})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if len(resp.Points) != 1 || resp.Points[0].OwnerShares != nil {
		t.Fatalf("points = %+v", resp.Points)
	}
}
