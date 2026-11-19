package providersync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

func dependencyTieWorld() ([]TeamRepoOwnershipProjectLink, []TeamRepoOwnershipWorkItem) {
	return []TeamRepoOwnershipProjectLink{
		{Provider: "linear", ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}, []TeamRepoOwnershipWorkItem{
		{WorkItemID: "ghpr:acme/repo-a#7", Provider: "github", Type: "pr", RepoID: "repo-a"},
		{WorkItemID: "linear:PLAT-9", Provider: "linear", Type: "issue", ProjectID: "proj-1"},
	}
}

// A pull request has a native relates_to link to an issue and a text "Blocks"
// link to it, written by one sync with the same last_synced: the inheritable
// edge stays, as in the cascade, so the issue's team owns the repository. A
// newer blocking edge still replaces an older inheritable one.
func TestTheRepoOwnershipDonorWalkKeepsTheInheritableEdgeOnATie(t *testing.T) {
	links, items := dependencyTieWorld()
	synced := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	relates := TeamRepoOwnershipDependencyEdge{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:PLAT-9", RelationshipType: "relates_to", LastSynced: synced}
	blocks := relates
	blocks.RelationshipType = "blocks"
	for _, edges := range [][]TeamRepoOwnershipDependencyEdge{{relates, blocks}, {blocks, relates}} {
		got := deriveTeamRepoOwnership("org-1", links, items, edges, nil, nil).Rows
		if len(got) != 1 || got[0].TeamID != "team-platform" || got[0].RepoID != "repo-a" {
			t.Errorf("edges %s then %s: rows %+v, want repo-a -> team-platform", edges[0].RelationshipType, edges[1].RelationshipType, got)
		}
	}

	newerBlocks := blocks
	newerBlocks.LastSynced = synced.Add(time.Minute)
	if got := deriveTeamRepoOwnership("org-1", links, items, []TeamRepoOwnershipDependencyEdge{relates, newerBlocks}, nil, nil).Rows; len(got) != 0 {
		t.Errorf("a newer blocks edge: rows %+v, want none", got)
	}
}

// Every pair of relationship types, tied on last_synced, through the cascade's
// collapse and through the derivation's donor walk: the walk finds the donor
// exactly when the cascade keeps an inheritable edge.
func TestTheCascadeAndTheDonorWalkPickTheSameEdgeOnATie(t *testing.T) {
	links, items := dependencyTieWorld()
	byID := map[string]TeamRepoOwnershipWorkItem{}
	for _, item := range items {
		byID[item.WorkItemID] = item
	}
	projectToTeam := resolveProjectToTeam(links)
	synced := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	types := []string{"relates_to", "relates", "duplicates", "external_issue_key", "blocks", "blocked_by", "is_blocked_by", "parent_of", "child_of"}
	for _, first := range types {
		for _, second := range types {
			if first == second {
				continue
			}
			var walkEdges []TeamRepoOwnershipDependencyEdge
			var cascadeEdges []teamattribution.GithubWorkItemDerivationDependencyEdge
			for _, relationshipType := range []string{first, second} {
				walkEdges = append(walkEdges, TeamRepoOwnershipDependencyEdge{
					SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:PLAT-9", RelationshipType: relationshipType, LastSynced: synced,
				})
				cascadeEdges = append(cascadeEdges, teamattribution.GithubWorkItemDerivationDependencyEdge{
					SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:PLAT-9", RelationshipType: relationshipType, LastSynced: synced,
				})
			}
			cascadeWinner := ""
			for _, edge := range teamattribution.LatestGitHubWorkItemDerivationDependencies(cascadeEdges) {
				cascadeWinner = edge.RelationshipType
			}
			wantTeam := ""
			if teamattribution.GithubWorkItemDerivationInheritableRelationships[cascadeWinner] {
				wantTeam = "team-platform"
			}
			gotTeam, _ := buildDonorTeamIDResolver(byID, walkEdges, projectToTeam, nil, nil)("ghpr:acme/repo-a#7")
			if gotTeam != wantTeam {
				t.Errorf("%s then %s: donor walk team %q, cascade keeps %q (want team %q)", first, second, gotTeam, cascadeWinner, wantTeam)
			}
		}
	}
}
