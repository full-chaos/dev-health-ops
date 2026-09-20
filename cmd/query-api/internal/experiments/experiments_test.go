package experiments

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/home"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/opportunities"
)

// The expected ids are sha256("<metric>:<suggestion>")[:16] computed by the
// reference implementation over the same UTF-8 bytes.
func TestStableID_MatchesTheReferenceDigest(t *testing.T) {
	cases := []struct{ metric, suggestion, want string }{
		{"cycle_time", "Trace the oldest active items to their current waiting state.", "052ebcaf263b2b0a"},
		{"", "Share the current playbook with new teams.", "476b399965696939"},
		{"é_x", "ü", "1771e416b7b33812"},
	}
	for _, tc := range cases {
		if got := StableID(tc.metric, tc.suggestion); got != tc.want {
			t.Errorf("StableID(%q,%q) = %s, want %s", tc.metric, tc.suggestion, got, tc.want)
		}
	}
}

func TestMetricFromTitle(t *testing.T) {
	cases := map[string]string{
		"Reduce PR Review Latency": "pr_review_latency",
		"Reduce Cycle Time":        "cycle_time",
		"Reduce ":                  "",
		"Reduce":                   "",
		"reduce Cycle Time":        "",
		"Maintain steady flow":     "",
		"":                         "",
	}
	for title, want := range cases {
		if got := metricFromTitle(title); got != want {
			t.Errorf("metricFromTitle(%q) = %q, want %q", title, got, want)
		}
	}
}

func TestFiltersFrom_ScopeAndFixedWindow(t *testing.T) {
	level := func(l model.ScopeLevelInput, ids ...string) *model.FilterInput {
		return &model.FilterInput{Scope: &model.ScopeFilterInput{Level: l, Ids: ids}}
	}
	cases := []struct {
		name string
		in   *model.FilterInput
		want home.ScopeFilter
	}{
		{"nil filter", nil, home.ScopeFilter{Level: "org", IDs: []string{}}},
		{"nil scope", &model.FilterInput{}, home.ScopeFilter{Level: "org", IDs: []string{}}},
		{"org no ids", level(model.ScopeLevelInputOrg), home.ScopeFilter{Level: "org", IDs: []string{}}},
		{"team ids", level(model.ScopeLevelInputTeam, "a", "b"), home.ScopeFilter{Level: "team", IDs: []string{"a", "b"}}},
		{"repo", level(model.ScopeLevelInputRepo, "r"), home.ScopeFilter{Level: "repo", IDs: []string{"r"}}},
		{"service", level(model.ScopeLevelInputService, "s"), home.ScopeFilter{Level: "service", IDs: []string{"s"}}},
		{"developer", level(model.ScopeLevelInputDeveloper, "d"), home.ScopeFilter{Level: "developer", IDs: []string{"d"}}},
		{"org with ids", level(model.ScopeLevelInputOrg, "x"), home.ScopeFilter{Level: "org", IDs: []string{"x"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := FiltersFrom(tc.in)
			if !reflect.DeepEqual(got.Scope, tc.want) {
				t.Fatalf("scope = %+v, want %+v", got.Scope, tc.want)
			}
			if got.Time.RangeDays != 30 || got.Time.CompareDays != 30 || got.Time.StartDate != nil || got.Time.EndDate != nil {
				t.Fatalf("window = %+v, want 30/30 with no dates", got.Time)
			}
		})
	}
}

func TestFromResponse_PromotesEverySuggestion(t *testing.T) {
	resp := &opportunities.Response{Items: []opportunities.Card{
		{ID: "opp-1", Title: "Reduce Cycle Time", SuggestedExperiments: []string{"Trace the oldest active items to their current waiting state.", "second"}},
		{ID: "opp-0", Title: "Maintain steady flow", SuggestedExperiments: []string{"Share the current playbook with new teams."}},
		{ID: "opp-2", Title: "Reduce X", SuggestedExperiments: nil},
	}}
	got := FromResponse(resp)
	if !got.DerivedFromOpportunities || len(got.Items) != 3 {
		t.Fatalf("derived=%v items=%d", got.DerivedFromOpportunities, len(got.Items))
	}
	first := got.Items[0]
	want := model.Experiment{ID: "052ebcaf263b2b0a", OpportunityID: "opp-1", Hypothesis: "Trace the oldest active items to their current waiting state.", Metric: "cycle_time", Status: model.ExperimentStatusSuggested}
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("first = %+v, want %+v", first, want)
	}
	if got.Items[1].Metric != "cycle_time" || got.Items[1].ID != StableID("cycle_time", "second") {
		t.Fatalf("second = %+v", got.Items[1])
	}
	if got.Items[2].Metric != "" || got.Items[2].ID != "476b399965696939" || got.Items[2].OpportunityID != "opp-0" {
		t.Fatalf("third = %+v", got.Items[2])
	}
}

func TestFromResponse_EmptyIsAnEmptyNonNilList(t *testing.T) {
	got := FromResponse(&opportunities.Response{})
	if got.Items == nil || len(got.Items) != 0 || !got.DerivedFromOpportunities {
		t.Fatalf("got %+v", got)
	}
}

func TestResolve_BuildFailureAnswersEmptyNotDerived(t *testing.T) {
	for name, build := range map[string]Builder{
		"error": func(context.Context, string, home.Filters, time.Time) (*opportunities.Response, error) {
			return nil, errors.New("clickhouse down")
		},
		"nil response": func(context.Context, string, home.Filters, time.Time) (*opportunities.Response, error) {
			return nil, nil
		},
	} {
		got := Resolve(context.Background(), build, "org-1", nil, time.Unix(0, 0))
		if got.DerivedFromOpportunities || got.Items == nil || len(got.Items) != 0 {
			t.Errorf("%s: got %+v", name, got)
		}
	}
}

func TestResolve_PassesOrgAndScopeToTheBuilder(t *testing.T) {
	var gotOrg string
	var gotFilters home.Filters
	build := func(_ context.Context, org string, f home.Filters, _ time.Time) (*opportunities.Response, error) {
		gotOrg, gotFilters = org, f
		return &opportunities.Response{}, nil
	}
	in := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"t1"}}}
	Resolve(context.Background(), build, "org-9", in, time.Unix(0, 0))
	if gotOrg != "org-9" || gotFilters.Scope.Level != "team" || !reflect.DeepEqual(gotFilters.Scope.IDs, []string{"t1"}) {
		t.Fatalf("org=%q filters=%+v", gotOrg, gotFilters)
	}
}
