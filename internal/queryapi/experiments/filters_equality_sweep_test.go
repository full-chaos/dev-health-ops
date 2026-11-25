package experiments

import (
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
)

// With no what.repos and no why.workCategory the filters are exactly the scope-only filters of main: every arm
// below answers the whole home.Filters value main answered, and the new fields stay at their zero value.
func TestFiltersFrom_NoRepoOrCategoryAnswersAsBefore(t *testing.T) {
	base := func(level string, ids ...string) home.Filters {
		return home.Filters{
			Time:  home.TimeFilter{RangeDays: defaultRangeDays, CompareDays: defaultCompareDays},
			Scope: home.ScopeFilter{Level: level, IDs: ids},
		}
	}
	scope := func(l model.ScopeLevelInput, ids ...string) *model.ScopeFilterInput {
		return &model.ScopeFilterInput{Level: l, Ids: ids}
	}
	cases := []struct {
		name string
		in   *model.FilterInput
		want home.Filters
	}{
		{"nil filter", nil, base("org", []string{}...)},
		{"empty filter", &model.FilterInput{}, base("org", []string{}...)},
		{"scope only", &model.FilterInput{Scope: scope(model.ScopeLevelInputTeam, "t-1")}, base("team", "t-1")},
		{"empty what and why", &model.FilterInput{Scope: scope(model.ScopeLevelInputRepo, "r-1"), What: &model.WhatFilterInput{}, Why: &model.WhyFilterInput{}}, base("repo", "r-1")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FiltersFrom(tc.in); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("filters = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// A repository or category filter reaches the build with no scope too, and the request's slices are copied.
func TestFiltersFrom_RepoAndCategoryWithoutScopeAreCopied(t *testing.T) {
	repos, cats := []string{"repo-synthetic-a"}, []string{"category-synthetic-a"}
	got := FiltersFrom(&model.FilterInput{What: &model.WhatFilterInput{Repos: repos}, Why: &model.WhyFilterInput{WorkCategory: cats}})
	if got.Scope.Level != "org" || len(got.Scope.IDs) != 0 {
		t.Fatalf("scope = %+v, want the org scope", got.Scope)
	}
	if !reflect.DeepEqual(got.What.Repos, repos) || !reflect.DeepEqual(got.Why.WorkCategory, cats) {
		t.Fatalf("what.repos %v, why.workCategory %v", got.What.Repos, got.Why.WorkCategory)
	}
	repos[0], cats[0] = "changed", "changed"
	if got.What.Repos[0] != "repo-synthetic-a" || got.Why.WorkCategory[0] != "category-synthetic-a" {
		t.Fatal("the filters alias the request's slices")
	}
}
