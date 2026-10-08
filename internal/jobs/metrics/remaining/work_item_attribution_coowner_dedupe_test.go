package remaining

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// Team B owns the item's project through two ownership facts at the top rank
// (project p1 by id, project p2 by the same key); team A owns p1 at the same
// rank. The real row builder emits B's co-owner row (2) and B's provenance
// row (0) under one sort key. The write dedupe, in either batch order, keeps
// A's primary row and B's co-owner row.
func TestTheAttributionWriteDedupeKeepsACoOwnerRowOverAProvenanceRow(t *testing.T) {
	for _, provider := range []string{"jira", "gitlab", "github", "linear"} {
		t.Run(provider, func(t *testing.T) {
			key := "PK"
			fact := func(team, projectID string) teamattribution.GithubWorkItemDerivationProjectFact {
				return teamattribution.GithubWorkItemDerivationProjectFact{
					Provider: provider, TeamID: team, TeamName: team, ProjectID: projectID, ProjectKey: &key,
					IsPrimary: 1, Specificity: 110, Priority: 10,
				}
			}
			derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
				Projects: []teamattribution.GithubWorkItemDerivationProjectFact{
					fact("team-a", "p1"), fact("team-b", "p1"), fact("team-b", "p2"),
				},
			})
			repoID, projectID, projectKey := uuid.NewString(), "p1", key
			id := provider + ":PK-1"
			rows := BuildWorkItemAttributionRows("org", time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
				map[string]struct{}{id: {}},
				map[string]teamattribution.GithubWorkItemDerivationSubject{id: {
					WorkItemID: id, Provider: provider, RepoID: &repoID,
					ProjectKey: &projectKey, ProjectID: &projectID, OrgID: "org",
				}}, derived)
			teamB := 0
			for _, row := range rows {
				if row.TeamID != nil && *row.TeamID == "team-b" {
					teamB++
				}
			}
			if teamB != 2 {
				t.Fatalf("builder gave team-b %d rows, want a co-owner and a provenance row under one key", teamB)
			}
			reversed := slices.Clone(rows)
			slices.Reverse(reversed)
			for name, batch := range map[string][]WorkItemAttributionRow{"builder order": slices.Clone(rows), "reversed": reversed} {
				kept := map[string]int{}
				for _, row := range workItemAttributionSortingKeyDedupe(batch) {
					if row.TeamID != nil {
						kept[*row.TeamID] = row.IsPrimary
					}
				}
				if kept["team-a"] != teamattribution.AttributionPrimary || kept["team-b"] != teamattribution.AttributionCoOwner {
					t.Errorf("%s: kept is_primary by team = %v, want team-a 1 and team-b 2", name, kept)
				}
			}
		})
	}
}
