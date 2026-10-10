// Team- and repo-scope snapshot tests -- siblings of
// TestGoldenOrgScopeDefault (golden_test.go), with the same
// range_days=7/compare_days=7/end_date=2024-01-08 window. The two files are
// GO SNAPSHOTS since CHAOS-8178 (kind go-generated; see golden_test.go for
// what changed and why): regression snapshots, not parity with Python.
//
// HOW THE FILES WERE FIRST MADE (the Python captures, before CHAOS-8178):
// identical structure to golden_test.go's own, with
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
		case strings.Contains(q, "link_repo_ids") || strings.Contains(q, "countIf(work_item_id IN"):
			// CHAOS-9094: a repository filter scopes the work-item metrics through
			// the items linked to the repository; the recorded org has none.
			return &fixtureRowScanner{rows: nil}, nil
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
	alignSignalRepoFilter(t, got, &want)
	alignRepoLinks(t, got, &want)
	assertResponseEqual(t, *got, want)
}

// The repo-link fields of a delta (CHAOS-9094) are GraphQL only (json "-"), so the
// golden cannot hold them. The four work-item metrics under a repository filter
// carry them, the others must not; the recorded organization has no linked item, so
// each of the four says no_links with empty counts. They are then copied onto the
// golden's delta so the rest of the response is compared as recorded.
func alignRepoLinks(t *testing.T, got, want *Response) {
	t.Helper()
	for i, d := range got.Deltas {
		if !repoLinkedMetrics[d.Metric] {
			if d.RepoLinkState != nil || d.RepoLinkBasis != nil || d.RepoLinkCoverage != nil || d.RepoLinkMultiRepoItems != nil {
				t.Errorf("delta %s is not a work-item metric but carries repo-link fields", d.Metric)
			}
			continue
		}
		if d.RepoLinkState == nil || *d.RepoLinkState != repoLinkNoLinks || d.RepoLinkBasis == nil || *d.RepoLinkBasis != (RepoLinkBasis{}) ||
			d.RepoLinkMultiRepoItems == nil || *d.RepoLinkMultiRepoItems != 0 || d.RepoLinkCoverage == nil || *d.RepoLinkCoverage != (RepoLinkCoverage{}) {
			t.Errorf("delta %s repo-link fields = %+v, want no_links with empty counts", d.Metric, d)
		}
		if i < len(want.Deltas) {
			want.Deltas[i].RepoLinkState, want.Deltas[i].RepoLinkBasis = d.RepoLinkState, d.RepoLinkBasis
			want.Deltas[i].RepoLinkMultiRepoItems, want.Deltas[i].RepoLinkCoverage = d.RepoLinkMultiRepoItems, d.RepoLinkCoverage
		}
	}
}

// A Home signal's repoFilterApplied is GraphQL only (json "-"), so the golden file
// cannot hold it. Each signal built from a metric must carry the value of that
// metric's delta (the golden holds the delta's); it is then copied onto the
// golden's signal so the rest of the response is compared as recorded.
func alignSignalRepoFilter(t *testing.T, got, want *Response) {
	t.Helper()
	byMetric := map[string]*bool{}
	for _, d := range got.Deltas {
		byMetric[d.Metric] = d.RepoFilterApplied
	}
	for i, signal := range got.Signals {
		flag, fromMetric := byMetric[signal.Metric]
		if !fromMetric {
			if signal.RepoFilterApplied != nil {
				t.Errorf("signal %s does not come from a metric but carries repoFilterApplied", signal.ID)
			}
			continue
		}
		if (flag == nil) != (signal.RepoFilterApplied == nil) || (flag != nil && *flag != *signal.RepoFilterApplied) {
			t.Errorf("signal %s repoFilterApplied = %v, want the delta's %v", signal.ID, signal.RepoFilterApplied, flag)
		}
		if i < len(want.Signals) {
			want.Signals[i].RepoFilterApplied = signal.RepoFilterApplied
		}
	}
}
