package daily

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// D7013b: team_wellbeing takes the team of a repository from its owner rows
// only. An unowned repository whose name a team's repo_patterns matches gets
// no team, the same rule as the TestOps families.
func TestWellbeingRepoOwnersOwnerOnlyNoPatternFallback(t *testing.T) {
	owned := uuid.MustParse("00000000-0000-4000-8000-0000000007a1")
	patterned := uuid.MustParse("00000000-0000-4000-8000-0000000007a2")
	names := map[string]string{owned.String(): "acme/owned", patterned.String(): "acme/patterned"}
	ownerRows := map[string][]teamownership.OwnerShare{
		owned.String(): {{TeamID: "github:owner", Main: true, Share: 1}},
	}
	teams := []WellbeingTeam{
		{ID: "github:owner", Name: "Owner"},
		{ID: "github:pattern", Name: "Pattern", RepoPatterns: []string{"acme/owned", "acme/patterned"}},
	}

	got := wellbeingRepoOwners(ownerRows, []uuid.UUID{owned, patterned}, names)
	if len(got) != 1 || len(got[owned.String()]) != 1 || got[owned.String()][0].TeamID != "github:owner" {
		t.Fatalf("owners = %v, want only %s -> github:owner", got, owned)
	}
	if _, ok := got[patterned.String()]; ok {
		t.Fatalf("unowned repository %s got a team from repo_patterns: %v", patterned, got)
	}

	resolver, _ := newOwnershipRepoResolver(got, teams)
	if id, name := resolver.ResolveRepo(owned.String()); id != "github:owner" || name != "Owner" {
		t.Errorf("owned repository resolves to (%q, %q), want (github:owner, Owner)", id, name)
	}
	if id, _ := resolver.ResolveRepo(patterned.String()); id != "" {
		t.Errorf("unowned repository resolves to %q, want no team (unassigned)", id)
	}
}
