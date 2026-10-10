package home

import (
	"context"
	"testing"
	"time"
)

// An empty string in a repository list names nothing (D5855): no repository is
// named, so there is no reason to give and nothing is read.
func TestFilterEmptyReasonIgnoresAnEmptyRepositoryReference(t *testing.T) {
	for name, f := range map[string]Filters{
		"what.repos with an empty string":   {What: WhatFilter{Repos: []string{""}}},
		"a repo scope with an empty id":     {Scope: ScopeFilter{Level: "repo", IDs: []string{""}}},
		"a team scope and an empty string":  {Scope: ScopeFilter{Level: "team", IDs: []string{"team-one"}}, What: WhatFilter{Repos: []string{""}}},
		"no filter":                         {},
		"a team scope alone (not a filter)": {Scope: ScopeFilter{Level: "team", IDs: []string{"team-one"}}},
	} {
		if refs := namedRepoRefs(f); len(refs) != 0 {
			t.Errorf("%s: namedRepoRefs = %v, want none", name, refs)
		}
		// A nil client proves nothing is read: a query would panic.
		reason, err := filterEmptyReason(context.Background(), nil, f, "org", time.Now())
		if err != nil || reason != nil {
			t.Errorf("%s: reason %v err %v, want null and no error", name, reason, err)
		}
	}
}

// Repositories named by a repo-level scope and by what.repos are both named.
func TestNamedRepoRefsAreTheScopeIDsAndWhatRepos(t *testing.T) {
	f := Filters{Scope: ScopeFilter{Level: "repo", IDs: []string{"a", ""}}, What: WhatFilter{Repos: []string{"b", ""}}}
	if refs := namedRepoRefs(f); len(refs) != 2 || refs[0] != "a" || refs[1] != "b" {
		t.Errorf("namedRepoRefs = %v, want [a b]", refs)
	}
	team := Filters{Scope: ScopeFilter{Level: "team", IDs: []string{"a"}}, What: WhatFilter{Repos: []string{"b"}}}
	if refs := namedRepoRefs(team); len(refs) != 1 || refs[0] != "b" {
		t.Errorf("a team scope's ids are teams, not repositories: namedRepoRefs = %v, want [b]", refs)
	}
}
