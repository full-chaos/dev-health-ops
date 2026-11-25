package experiments

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/opportunities"
)

// experiments builds on the home filters, so the request's what.repos and why.workCategory reach the build, as its
// scope does, and as they reach home for the same FilterInput.
func TestExperimentsFiltersReachTheBuild(t *testing.T) {
	var got home.Filters
	build := func(_ context.Context, _ string, f home.Filters, _ time.Time) (*opportunities.Response, error) {
		got = f
		return &opportunities.Response{}, nil
	}
	in := &model.FilterInput{
		Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-synthetic-a"}},
		What:  &model.WhatFilterInput{Repos: []string{"repo-synthetic-a"}},
		Why:   &model.WhyFilterInput{WorkCategory: []string{"category-synthetic-a"}},
	}
	Resolve(context.Background(), build, "org-synthetic-1", in, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	if got.Scope.Level != "team" || !slices.Equal(got.Scope.IDs, []string{"team-synthetic-a"}) {
		t.Fatalf("control: scope reached the build as %q %v, want team [team-synthetic-a]", got.Scope.Level, got.Scope.IDs)
	}
	t.Logf("control: scope team [team-synthetic-a] reaches the build")
	if !slices.Equal(got.What.Repos, []string{"repo-synthetic-a"}) {
		t.Errorf("what.repos [repo-synthetic-a] reached the build as %v: the suggestions are org-wide under a repository filter", got.What.Repos)
	}
	if !slices.Equal(got.Why.WorkCategory, []string{"category-synthetic-a"}) {
		t.Errorf("why.workCategory [category-synthetic-a] reached the build as %v", got.Why.WorkCategory)
	}
}
