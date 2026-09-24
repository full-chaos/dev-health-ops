// Team- and repo-scope golden parity tests -- siblings of
// TestGoldenOrgScopeDefault (golden_test.go), same capture technique and
// the same range_days=7/compare_days=7/end_date=2024-01-08 window.
//
// CAPTURE COMMANDS: identical structure to golden_test.go's own, with
// filters.scope set to ScopeFilter(level="team", ids=["team-1"]) (team
// fixture) or ScopeFilter(level="repo", ids=["checkout-service"]) (repo
// fixture) respectively. The team fixture additionally seeds a real
// recommendations_daily row (recommendations only fire at team scope);
// team-1 has no matching repos in the captured scenario, so repo-grain
// metrics stay unscoped for this fixture -- the fake client dispatches
// purely on a query's column marker, not on its WHERE clause, so this
// fixture's canned column values answer every repo-grain metric read
// regardless of the (pushed-down, not a separate round trip) team->repo
// condition scopeFilterForMetric adds to each one. The repo fixture
// seeds a resolve_repo_id name-lookup row ("checkout-service" ->
// "repo-1") and asserts recommendations_daily is never queried at all
// (the Python guard returns [] before any read runs at that scope).
package home

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

func teamGoldenHandler(t *testing.T) func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	base := orgGoldenHandler(t)
	return func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		q := query
		switch {
		case strings.Contains(q, "FROM recommendations_daily"):
			return &fixtureRowScanner{rows: [][]any{
				{
					"team-1", seededOrgIDUnused, "wip-saturation", true, "critical",
					"WIP saturation has been elevated for a week",
					"Team One is carrying more active work than its cadence supports.",
					"Bring WIP below the team's own historical p50 for three consecutive days.",
					`[{"ref":"a"},{"ref":"b"}]`,
					day(2024, 1, 2), day(2024, 1, 8),
					time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC),
				},
			}}, nil
		}
		return base(t, query, bindings)
	}
}

// seededOrgIDUnused: the recommendations_daily fixture row's org_id
// column is scanned but never asserted on by this test (org scope is
// bound by the request's own org_id argument, not by this column's
// value) -- named instead of a bare literal so its own unused-ness at
// the call site is self-explanatory.
const seededOrgIDUnused = "org-1"

func TestGoldenTeamScoped(t *testing.T) {
	client := fakeQueryClient{t: t, handler: teamGoldenHandler(t)}

	f := Filters{
		Time:  TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day(2024, 1, 8))},
		Scope: ScopeFilter{Level: "team", IDs: []string{"team-1"}},
	}
	now := time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC)

	got, err := BuildResponse(context.Background(), client, nil, "org-1", f, now)
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	want := loadGolden(t, "team_scoped.json")
	assertResponseEqual(t, *got, want)
}

func repoGoldenHandler(t *testing.T) func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	base := orgGoldenHandler(t)
	return func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		q := query
		switch {
		case strings.Contains(q, "FROM repos FINAL") && strings.Contains(q, "WHERE repo = "):
			return &fixtureRowScanner{rows: [][]any{{"repo-1"}}}, nil
		case strings.Contains(q, "FROM recommendations_daily"):
			t.Fatal("recommendations_daily must not be read at repo scope")
			return nil, nil
		}
		return base(t, query, bindings)
	}
}

func TestGoldenRepoScoped(t *testing.T) {
	client := fakeQueryClient{t: t, handler: repoGoldenHandler(t)}

	f := Filters{
		Time:  TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day(2024, 1, 8))},
		Scope: ScopeFilter{Level: "repo", IDs: []string{"checkout-service"}},
	}
	now := time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC)

	got, err := BuildResponse(context.Background(), client, nil, "org-1", f, now)
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	want := loadGolden(t, "repo_scoped.json")
	assertResponseEqual(t, *got, want)
}
