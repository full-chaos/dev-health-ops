//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/google/uuid"
)

// One sync writes two dependency rows for the same pull request and issue with
// the same last_synced: an inheritable one and a lower non-inheritable one. The
// rows are read back from the store and go through the cascade's edge collapse
// and through the repository-ownership derivation; both must keep the same edge,
// so the issue's team owns the pull request's repository.
func TestARepoOwnershipDonorSurvivesASameSyncTieWithABlockingEdge(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	now := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	for _, tc := range []struct {
		name       string
		types      [2]string
		wantWinner string
	}{
		{"relates_to ties blocks", [2]string{"relates_to", "blocks"}, "relates_to"},
		{"blocks ties relates_to", [2]string{"blocks", "relates_to"}, "relates_to"},
		{"duplicates ties child_of", [2]string{"duplicates", "child_of"}, "duplicates"},
		{"control: duplicates ties is_blocked_by", [2]string{"duplicates", "is_blocked_by"}, "duplicates"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orgID := "edge-tie-" + uuid.NewString()
			repoA := uuid.New()
			seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{repoA: "acme/a"})
			seedTeamProjectOwnership(t, ctx, conn, orgID, "linear", "proj-donor", "team-donor", true, now)
			seedWorkItemOfType(t, ctx, conn, orgID, "ghpr:acme/a#1", "github", "pr", repoA, "", now)
			seedWorkItemOfType(t, ctx, conn, orgID, "linear:PLAT-1", "linear", "issue", uuid.Nil, "proj-donor", now)
			for _, relationshipType := range tc.types {
				seedWorkItemDependency(t, ctx, conn, orgID, "ghpr:acme/a#1", "linear:PLAT-1", relationshipType, now)
			}

			stored, err := loadTeamRepoOwnershipDependencyEdges(ctx, conn, orgID)
			if err != nil {
				t.Fatalf("load edges: %v", err)
			}
			if len(stored) != 2 || !stored[0].LastSynced.Equal(stored[1].LastSynced) {
				t.Fatalf("want 2 stored edges with one last_synced, got %+v", stored)
			}
			converted := make([]teamattribution.GithubWorkItemDerivationDependencyEdge, 0, len(stored))
			for _, edge := range stored {
				converted = append(converted, teamattribution.GithubWorkItemDerivationDependencyEdge{
					SourceWorkItemID: edge.SourceWorkItemID, TargetWorkItemID: edge.TargetWorkItemID,
					RelationshipType: edge.RelationshipType, LastSynced: edge.LastSynced,
				})
			}
			cascadeWinner := ""
			for _, edge := range teamattribution.LatestGitHubWorkItemDerivationDependencies(converted) {
				cascadeWinner = edge.RelationshipType
			}

			service := TeamRepoOwnershipDerivationService{Conn: conn}
			if _, _, _, _, err := service.Derive(ctx, orgID); err != nil {
				t.Fatalf("Derive: %v", err)
			}
			repoTeam := readTeamRepoOwnership(t, ctx, conn, orgID)["acme/a"].teamID

			t.Logf("TIE case=%q cascade_winner=%q repo_ownership_team=%q", tc.name, cascadeWinner, repoTeam)
			if cascadeWinner != tc.wantWinner {
				t.Errorf("cascade kept %q, want %q", cascadeWinner, tc.wantWinner)
			}
			if repoTeam != "team-donor" {
				t.Errorf("acme/a owner %q, want team-donor (the cascade keeps %q)", repoTeam, cascadeWinner)
			}
		})
	}
}
