package analytics

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

func TestAsUUIDString(t *testing.T) {
	id, ok := asUUIDString("550e8400-e29b-41d4-a716-446655440000")
	if !ok || id != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("asUUIDString(valid) = (%q, %v)", id, ok)
	}
	if _, ok := asUUIDString("not-a-uuid"); ok {
		t.Fatal("expected asUUIDString to reject a non-UUID string")
	}
	if _, ok := asUUIDString("myorg/myrepo"); ok {
		t.Fatal("expected asUUIDString to reject a repo slug")
	}
}

func TestDedupePreservingOrder(t *testing.T) {
	got := dedupePreservingOrder([]string{"b", "a", "b", "c", "a"})
	want := []string{"b", "a", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestResolveRepoFilterRefs_MixedUUIDsAndNames(t *testing.T) {
	client := &fakeSingleClient{
		response: &fakeRowScanner{rows: [][]any{
			{"550e8400-e29b-41d4-a716-446655440000", "org/myrepo"},
		}},
	}
	refs := []string{
		"11111111-1111-1111-1111-111111111111", // already a UUID -- passes through
		"org/myrepo",                           // resolves via the query
		"org/unknown-repo",                     // no match -- sentinel
	}
	resolved, err := resolveRepoFilterRefs(context.Background(), client, "org-1", refs)
	if err != nil {
		t.Fatalf("resolveRepoFilterRefs error = %v", err)
	}
	if len(resolved) != 3 {
		t.Fatalf("got %d resolved refs, want 3: %v", len(resolved), resolved)
	}
	if resolved[0] != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("resolved[0] = %q, want the passthrough UUID", resolved[0])
	}
	if resolved[1] != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("resolved[1] = %q, want the resolved UUID", resolved[1])
	}
	if resolved[2] != unmatchedRepoFilterID {
		t.Errorf("resolved[2] = %q, want the unmatched sentinel %q", resolved[2], unmatchedRepoFilterID)
	}
}

func TestResolveRepoFilterRefs_AllUUIDsSkipsQuery(t *testing.T) {
	client := &fakeSingleClient{err: nil, response: nil}
	refs := []string{"11111111-1111-1111-1111-111111111111"}
	resolved, err := resolveRepoFilterRefs(context.Background(), client, "org-1", refs)
	if err != nil {
		t.Fatalf("resolveRepoFilterRefs error = %v", err)
	}
	if client.statement != "" {
		t.Fatal("expected no query to be issued when every ref is already a UUID")
	}
	if len(resolved) != 1 || resolved[0] != refs[0] {
		t.Fatalf("resolved = %v", resolved)
	}
}

func TestResolveAnalyticsRepoFilters_NilPassesThrough(t *testing.T) {
	client := &fakeSingleClient{}
	out, err := ResolveAnalyticsRepoFilters(context.Background(), client, "org-1", nil)
	if err != nil || out != nil {
		t.Fatalf("expected (nil, nil), got (%v, %v)", out, err)
	}
}

func TestResolveAnalyticsRepoFilters_RewritesRepoScopeAndWhatRepos(t *testing.T) {
	client := &fakeSingleClient{
		response: &fakeRowScanner{rows: [][]any{
			{"550e8400-e29b-41d4-a716-446655440000", "org/myrepo"},
		}},
	}
	filters := &model.FilterInput{
		Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputRepo, Ids: []string{"org/myrepo"}},
		What:  &model.WhatFilterInput{Repos: []string{"org/myrepo"}, Services: []string{"svc-a"}},
		Who:   &model.WhoFilterInput{Developers: []string{"dev@example.com"}},
	}
	out, err := ResolveAnalyticsRepoFilters(context.Background(), client, "org-1", filters)
	if err != nil {
		t.Fatalf("ResolveAnalyticsRepoFilters error = %v", err)
	}
	if len(out.Scope.Ids) != 1 || out.Scope.Ids[0] != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("Scope.Ids = %v", out.Scope.Ids)
	}
	if len(out.What.Repos) != 1 || out.What.Repos[0] != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("What.Repos = %v", out.What.Repos)
	}
	if len(out.What.Services) != 1 || out.What.Services[0] != "svc-a" {
		t.Errorf("What.Services should be untouched, got %v", out.What.Services)
	}
	if out.Who == nil || len(out.Who.Developers) != 1 {
		t.Errorf("Who should be untouched, got %v", out.Who)
	}
}

func TestResolveAnalyticsRepoFilters_NonRepoScopeUnchanged(t *testing.T) {
	client := &fakeSingleClient{}
	filters := &model.FilterInput{
		Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-1"}},
	}
	out, err := ResolveAnalyticsRepoFilters(context.Background(), client, "org-1", filters)
	if err != nil {
		t.Fatalf("ResolveAnalyticsRepoFilters error = %v", err)
	}
	if client.statement != "" {
		t.Fatal("expected no repo-resolution query for a non-repo scope")
	}
	if out.Scope.Ids[0] != "team-1" {
		t.Errorf("Scope.Ids = %v, want unchanged", out.Scope.Ids)
	}
}
