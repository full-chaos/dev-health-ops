package teamattribution

import (
	"testing"
	"time"
)

// LatestDependencyEdgeWins over every pair of relationship types: a newer row
// wins whatever its type; on an equal last_synced an inheritable type beats a
// non-inheritable one, then the lower type wins; exactly one row of a tied pair
// wins, whatever the order.
func TestLatestDependencyEdgeWinsRule(t *testing.T) {
	types := []string{"relates_to", "relates", "duplicates", "external_issue_key", "blocks", "blocked_by", "is_blocked_by", "parent_of", "child_of"}
	synced := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	newer := synced.Add(time.Second)
	for _, candidate := range types {
		for _, held := range types {
			if !LatestDependencyEdgeWins(newer, candidate, synced, held) {
				t.Errorf("newer %s does not replace older %s", candidate, held)
			}
			if LatestDependencyEdgeWins(synced, candidate, newer, held) {
				t.Errorf("older %s replaces newer %s", candidate, held)
			}
			if candidate == held {
				if LatestDependencyEdgeWins(synced, candidate, synced, held) {
					t.Errorf("%s replaces the same type at the same last_synced", candidate)
				}
				continue
			}
			candidateInherits := GithubWorkItemDerivationInheritableRelationships[candidate]
			want := candidate < held
			if candidateInherits != GithubWorkItemDerivationInheritableRelationships[held] {
				want = candidateInherits
			}
			got := LatestDependencyEdgeWins(synced, candidate, synced, held)
			if got != want {
				t.Errorf("tie %s over %s: %v, want %v", candidate, held, got, want)
			}
			if got == LatestDependencyEdgeWins(synced, held, synced, candidate) {
				t.Errorf("tie %s / %s: both orders give %v", candidate, held, got)
			}
		}
	}
	if !LatestDependencyEdgeWins(synced, "relates_to", synced, "blocks") || LatestDependencyEdgeWins(synced, "blocks", synced, "relates_to") {
		t.Error("a tied relates_to and blocks: relates_to must win in both orders")
	}
}
