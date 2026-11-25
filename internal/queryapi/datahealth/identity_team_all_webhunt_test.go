package datahealth

import (
	"context"
	"testing"
)

func teamScopedIdentityRows() *routingCH {
	return &routingCH{rules: []chRule{
		{match: "FROM git_commits", rows: [][]any{
			{"git", "scoped@example.com", "Scoped", uint64(5)},
			{"git", "free@example.com", "Free", uint64(4)},
			{"git", "stray@example.com", "Stray", uint64(3)},
		}},
		{match: "FROM identities FINAL", rows: [][]any{
			{"scoped-c", "scoped@example.com", "Scoped", `{}`, []string{"team-a"}},
			{"free-c", "free@example.com", "Free", `{}`, []string{}},
		}},
	}}
}

// The web identity table sends team "ALL" for the whole org (IdentityGapsTable.tsx).
func TestWebhuntIdentityMappingTeamAllEqualsNoTeam(t *testing.T) {
	ctx := context.Background()
	none := (&Reader{ClickHouse: teamScopedIdentityRows()}).IdentityMapping(ctx, "org-1", "")
	if none.UnmappedCount != 1 {
		t.Fatalf("control: no team unmapped = %d, want 1 (stray only)", none.UnmappedCount)
	}
	inTeam := (&Reader{ClickHouse: teamScopedIdentityRows()}).IdentityMapping(ctx, "org-1", "team-a")
	if inTeam.UnmappedCount != none.UnmappedCount {
		t.Fatalf("control: team-a unmapped = %d, want %d", inTeam.UnmappedCount, none.UnmappedCount)
	}
	all := (&Reader{ClickHouse: teamScopedIdentityRows()}).IdentityMapping(ctx, "org-1", "ALL")
	if all.UnmappedCount != none.UnmappedCount {
		t.Fatalf("team ALL unmapped = %d, want %d (same as no team filter)", all.UnmappedCount, none.UnmappedCount)
	}
}
