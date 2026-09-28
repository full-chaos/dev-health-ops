package graph

// Translation-correctness tests for home_translate.go (CHAOS-6084 /
// CHAOS-7042). home.BuildResponse itself is already golden-parity-proven
// against the real Python build_home_response (internal/queryapi/home's
// own golden_test.go et al) -- these tests are NOT a second oracle
// against Python. They exist to pin the NEW code this port adds: the
// GraphQL FilterInput -> home.Filters mapping, and the home.Response ->
// model.HomeResult mapping (the field set is derived from the GraphQL
// HomeResult type itself -- Freshness.LastIngestedAt, Deltas,
// ReworkThemeAllocation -- schema.py's Strawberry construction shows
// only these three sub-fields were ever exposed).

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
)

func TestHomeFiltersFromGraphQL_NilInputUsesDefaults(t *testing.T) {
	got := homeFiltersFromGraphQL(nil)
	want := home.Filters{
		Time:  home.TimeFilter{RangeDays: 14, CompareDays: 14},
		Scope: home.ScopeFilter{Level: "org"},
	}
	if got.Time != want.Time {
		t.Errorf("Time = %+v, want %+v", got.Time, want.Time)
	}
	if got.Scope.Level != want.Scope.Level || len(got.Scope.IDs) != 0 {
		t.Errorf("Scope = %+v, want %+v", got.Scope, want.Scope)
	}
}

func TestHomeFiltersFromGraphQL_MapsScopeWhatWhy(t *testing.T) {
	in := &model.FilterInput{
		Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-1"}},
		What:  &model.WhatFilterInput{Repos: []string{"repo-1", "repo-2"}},
		Why:   &model.WhyFilterInput{WorkCategory: []string{"feature_delivery"}},
	}
	got := homeFiltersFromGraphQL(in)

	if got.Scope.Level != "team" {
		t.Errorf("Scope.Level = %q, want %q (lower-cased GraphQL enum)", got.Scope.Level, "team")
	}
	if len(got.Scope.IDs) != 1 || got.Scope.IDs[0] != "team-1" {
		t.Errorf("Scope.IDs = %v, want [team-1]", got.Scope.IDs)
	}
	if len(got.What.Repos) != 2 || got.What.Repos[0] != "repo-1" {
		t.Errorf("What.Repos = %v, want [repo-1 repo-2]", got.What.Repos)
	}
	if len(got.Why.WorkCategory) != 1 || got.Why.WorkCategory[0] != "feature_delivery" {
		t.Errorf("Why.WorkCategory = %v, want [feature_delivery]", got.Why.WorkCategory)
	}
	// Time is never carried by FilterInput -- must still default, same as nil.
	if got.Time.RangeDays != 14 || got.Time.CompareDays != 14 {
		t.Errorf("Time = %+v, want the 14/14 default (FilterInput has no time range field)", got.Time)
	}
}

func TestHomeFiltersFromGraphQL_ScopeWithoutLevelDefaultsToOrg(t *testing.T) {
	// A zero-value ScopeLevelInput ("") is not one of the five valid
	// enum values -- scopeLevelToHomeLevel returns "", and the default
	// "org" must survive rather than being overwritten with an invalid
	// empty level home.BuildResponse's own scope-filter code has no
	// case for.
	in := &model.FilterInput{Scope: &model.ScopeFilterInput{Ids: []string{"x"}}}
	got := homeFiltersFromGraphQL(in)
	if got.Scope.Level != "org" {
		t.Errorf("Scope.Level = %q, want the org default preserved", got.Scope.Level)
	}
}

func TestHomeResultFromResponse_MapsAllThreeGraphQLFields(t *testing.T) {
	ingested := pytime.NaiveDateTime(time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC))
	resp := &home.Response{
		Freshness: home.Freshness{
			LastIngestedAt: &ingested,
			Coverage: home.Coverage{
				ReposCoveredPct: 80, PRsLinkedToIssuesPct: 60, IssuesWithCycleStatesPct: 40,
			},
		},
		Deltas: []home.MetricDelta{
			{
				Metric: "throughput", Label: "Throughput", Value: 42, Unit: "units", DeltaPct: 12.5,
				Spark: []home.SparkPoint{{TS: pytime.NaiveDateTime(time.Date(2024, 1, 7, 0, 0, 0, 0, time.UTC)), Value: 40}},
			},
		},
		ReworkThemeAllocation: []home.ReworkThemeAllocation{
			{Theme: "feature_delivery", Label: "Feature Delivery", Allocation: 10, AllocationPct: 50, PRsMerged: 3, ChurnLOC: 100},
		},
	}

	got := homeResultFromResponse(resp)

	if got.Freshness == nil || got.Freshness.LastIngestedAt == nil {
		t.Fatal("Freshness.LastIngestedAt must be populated when the Response carries one")
	}
	if want := "2024-01-08T12:00:00"; *got.Freshness.LastIngestedAt != want {
		t.Errorf("Freshness.LastIngestedAt = %q, want %q", *got.Freshness.LastIngestedAt, want)
	}
	if got.Freshness.Coverage == nil {
		t.Fatal("Freshness.Coverage must be populated -- home.BuildResponse always computes it")
	}
	if c := got.Freshness.Coverage; c.ReposCoveredPct != 80 || c.PrsLinkedToIssuesPct != 60 || c.IssuesWithCycleStatesPct != 40 {
		t.Errorf("Freshness.Coverage = %+v, want the mapped Coverage", c)
	}

	if len(got.Deltas) != 1 {
		t.Fatalf("Deltas = %d entries, want 1", len(got.Deltas))
	}
	d := got.Deltas[0]
	if d.Metric != "throughput" || d.Label != "Throughput" || d.Value != 42 || d.Unit != "units" || d.DeltaPct != 12.5 {
		t.Errorf("Deltas[0] = %+v, want the mapped MetricDelta", d)
	}
	if len(d.Spark) != 1 || d.Spark[0].Value != 40 || d.Spark[0].Ts != "2024-01-07T00:00:00" {
		t.Errorf("Deltas[0].Spark = %+v, want one point at 2024-01-07T00:00:00 value 40", d.Spark)
	}

	if len(got.ReworkThemeAllocation) != 1 {
		t.Fatalf("ReworkThemeAllocation = %d entries, want 1", len(got.ReworkThemeAllocation))
	}
	a := got.ReworkThemeAllocation[0]
	if a.Theme != "feature_delivery" || a.Label != "Feature Delivery" || a.Allocation != 10 || a.AllocationPct != 50 || a.PrsMerged != 3 || a.ChurnLoc != 100 {
		t.Errorf("ReworkThemeAllocation[0] = %+v, want the mapped ReworkThemeAllocation", a)
	}
}

func TestHomeResultFromResponse_NilLastIngestedAtStaysNil(t *testing.T) {
	resp := &home.Response{}
	got := homeResultFromResponse(resp)
	if got.Freshness == nil {
		t.Fatal("Freshness must never be nil")
	}
	if got.Freshness.LastIngestedAt != nil {
		t.Errorf("LastIngestedAt = %v, want nil when the Response carries none", *got.Freshness.LastIngestedAt)
	}
	if got.Deltas == nil {
		t.Error("Deltas must be an empty slice, not nil, matching schema.py's list comprehension over an empty input")
	}
	if got.ReworkThemeAllocation == nil {
		t.Error("ReworkThemeAllocation must be an empty slice, not nil, for the same reason")
	}
}
