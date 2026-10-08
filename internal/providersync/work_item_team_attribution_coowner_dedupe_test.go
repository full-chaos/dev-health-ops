package providersync

import (
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// Team B owns the item's project through two ownership facts at the top rank
// (project p1 by id, project p2 by the same key); team A owns p1 at the same
// rank. The real producer emits B's co-owner row (2) and B's provenance row
// (0) under one sort key (repo, item, team-b, project_ownership). The write
// dedupe, in either batch order, keeps A's primary row and B's co-owner row.
func TestTheWriteDedupeKeepsACoOwnerRowOverAProvenanceRowOfTheSameKey(t *testing.T) {
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
			claim := githubWorkItemOracleClaim()
			claim.Provider = provider
			computedAt := time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC)
			surfaces, err := buildWorkItemDerivedSurfacesForProvider(provider, claim, githubWorkItemRows{WorkItems: []githubWorkItemRow{{
				WorkItemID: provider + ":PK-1", Provider: provider, Title: "t", Type: "issue", Status: "todo",
				ProjectID: stringPointer("p1"), ProjectKey: stringPointer(key),
				CreatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
				UpdatedAt: time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
				OrgID:     claim.OrgID,
			}}}, time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC), computedAt, derived, nil)
			if err != nil {
				t.Fatal(err)
			}
			rows := surfaces.TeamAttributions
			teamB := 0
			for _, row := range rows {
				if githubWorkItemDerivedNullableString(row.TeamID) == "team-b" {
					teamB++
				}
			}
			if teamB != 2 {
				t.Fatalf("producer gave team-b %d rows, want a co-owner and a provenance row under one key: %+v", teamB, rows)
			}
			for name, batch := range map[string][]githubWorkItemTeamAttributionRow{
				"producer order": slices.Clone(rows),
				"reversed":       reversedRows(rows),
			} {
				kept := map[string]int{}
				for _, row := range githubWorkItemDerivedSortingKeyDedupe(
					batch, githubTeamAttributionSortingKey, githubTeamAttributionVersion, githubTeamAttributionPreference,
				) {
					kept[githubWorkItemDerivedNullableString(row.TeamID)] = row.IsPrimary
				}
				if kept["team-a"] != teamattribution.AttributionPrimary || kept["team-b"] != teamattribution.AttributionCoOwner {
					t.Errorf("%s: kept is_primary by team = %v, want team-a 1 and team-b 2", name, kept)
				}
			}
		})
	}
}

func reversedRows(rows []githubWorkItemTeamAttributionRow) []githubWorkItemTeamAttributionRow {
	result := slices.Clone(rows)
	slices.Reverse(result)
	return result
}
