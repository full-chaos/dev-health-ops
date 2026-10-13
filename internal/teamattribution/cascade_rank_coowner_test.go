package teamattribution

import "testing"

// A co-owner ownership fact (is_primary = 2, ruling D5988) ranks with the
// facts that are not primary: only is_primary = 1 ranks first, so a co-owner
// never outranks the main owner, and it carries the confidence of a fact that
// is not primary.
func TestACoOwnerFactNeverOutranksTheMainOwner(t *testing.T) {
	team := func(id string) *string { return &id }
	ranked := RankDerivationCandidates([]GithubWorkItemDerivationCandidate{
		{Source: "repo_ownership", TeamID: team("co-owner"), IsPrimary: 2, Specificity: 900},
		{Source: "repo_ownership", TeamID: team("main"), IsPrimary: 1, Specificity: 150},
		{Source: "repo_ownership", TeamID: team("grant"), IsPrimary: 0, Specificity: 100},
	})
	got := []string{*ranked[0].TeamID, *ranked[1].TeamID, *ranked[2].TeamID}
	if got[0] != "main" || got[1] != "co-owner" || got[2] != "grant" {
		t.Fatalf("rank = %v, want [main co-owner grant] (main first; then specificity)", got)
	}
	if ConfidenceForPrimary(2) != "medium" || ConfidenceForPrimary(1) != "high" || ConfidenceForPrimary(0) != "medium" {
		t.Fatalf("confidence 2/1/0 = %s/%s/%s, want medium/high/medium", ConfidenceForPrimary(2), ConfidenceForPrimary(1), ConfidenceForPrimary(0))
	}
}
