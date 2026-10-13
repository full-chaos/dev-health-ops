package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// The GitHub/GitLab/Jira/Linear work item derivation gives a pull request the
// team of its linked donor's stored row, not of a resolution of the donor
// with this sync's facts (CHAOS-9460). The donor's project is owned by
// team-linked in this sync's facts; its stored row names team-row. A donor
// with no stored row gives nothing. A donor that this sync writes itself
// gives the row this sync writes for it, never its older stored row, and so
// nothing when it has no team of its own now.
func TestAPullRequestCopiesItsDonorsStoredRow(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim := githubWorkItemOracleClaim()
	donorProject := "linear-project-1"
	facts := func(rows map[string]teamattribution.GithubWorkItemDerivationDonorRow) teamattribution.GithubWorkItemDerivationFacts {
		return teamattribution.GithubWorkItemDerivationFacts{
			Projects: []teamattribution.GithubWorkItemDerivationProjectFact{{
				Provider: "linear", TeamID: "team-linked", TeamName: "Linked Team",
				ProjectID: donorProject, IsPrimary: 1, Specificity: 80, UpdatedAt: now,
			}},
			DonorItems: []teamattribution.GithubWorkItemDerivationSubject{{
				WorkItemID: "linear:CHAOS-42", Provider: "linear", ProjectID: &donorProject, OrgID: claim.OrgID,
			}},
			DonorRows: rows,
		}
	}
	pr := githubWorkItemRow{WorkItemID: "gh:acme/api#7", Provider: "github", OrgID: claim.OrgID}
	edge := githubWorkItemDependencyRow{
		SourceWorkItemID: "gh:acme/api#7", TargetWorkItemID: "extkey:CHAOS-42",
		RelationshipType: "external_issue_key", LastSynced: now, OrgID: claim.OrgID,
	}
	freshDonor := githubWorkItemRow{WorkItemID: "linear:CHAOS-42", Provider: "linear", ProjectID: &donorProject, OrgID: claim.OrgID}
	unownedProject := "linear-project-nobody-owns"
	freshDonorWithNoTeam := githubWorkItemRow{WorkItemID: "linear:CHAOS-42", Provider: "linear", ProjectID: &unownedProject, OrgID: claim.OrgID}
	stored := map[string]teamattribution.GithubWorkItemDerivationDonorRow{
		"linear:CHAOS-42": {TeamID: "team-row", TeamName: "Row Team", Source: "project_ownership"},
	}
	for _, tc := range []struct {
		name  string
		rows  map[string]teamattribution.GithubWorkItemDerivationDonorRow
		items []githubWorkItemRow
		want  string
	}{
		{"the donor's stored row", stored, []githubWorkItemRow{pr}, "team-row"},
		{"a donor with no stored row", map[string]teamattribution.GithubWorkItemDerivationDonorRow{}, []githubWorkItemRow{pr}, ""},
		{"a donor this sync writes", stored, []githubWorkItemRow{pr, freshDonor}, "team-linked"},
		{"a donor this sync writes with no team of its own now", stored, []githubWorkItemRow{pr, freshDonorWithNoTeam}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &fakeGitHubWorkItemDerivationContextSource{facts: facts(tc.rows)}
			derived, err := loadGitHubWorkItemDerivationContext(context.Background(), claim,
				githubWorkItemRows{WorkItems: tc.items, Dependencies: []githubWorkItemDependencyRow{edge}}, source, now)
			if err != nil {
				t.Fatal(err)
			}
			got := ""
			_, _, candidates := derived.Resolve(githubWorkItemDerivationSubjectFromRow(pr))
			for _, candidate := range candidates {
				if candidate.Source == "linked_issue" {
					got = teamattribution.GithubWorkItemDerivationStringValue(candidate.TeamID)
				}
			}
			if got != tc.want {
				t.Errorf("linked_issue team = %q, want %q", got, tc.want)
			}
		})
	}
}
