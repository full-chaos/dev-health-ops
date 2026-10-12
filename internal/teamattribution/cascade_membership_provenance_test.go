package teamattribution

import "testing"

// CHAOS-9414 (D7): membership is provenance only. A member's team never
// becomes the primary team; a higher-ranked ownership source wins, and the
// membership row stays a non-primary provenance row.
func TestMembershipTiersAreProvenanceOnly(t *testing.T) {
	repoID := "repo-1"
	member := []GithubWorkItemDerivationMemberFact{{
		Provider: "github", TeamID: "team-m", TeamName: "Member Team",
		MemberID: "alice", IsPrimary: 1, Specificity: 60,
	}}
	cases := []struct {
		name    string
		subject GithubWorkItemDerivationSubject
		source  string
	}{
		{"assignee", GithubWorkItemDerivationSubject{
			WorkItemID: "gh:a#1", Provider: "github", Assignees: []string{"alice"}, OrgID: "o",
		}, "assignee_membership"},
		{"author", GithubWorkItemDerivationSubject{
			WorkItemID: "gh:a#2", Provider: "github", Type: "pr", Reporter: strPtr("alice"), OrgID: "o",
		}, "author_membership"},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/only-signal-stays-unassigned", func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Members: member})
			team, _, candidates := derived.Resolve(tc.subject)
			if got := GithubWorkItemDerivationStringValue(team); got != "" {
				t.Fatalf("primary team = %q, want none", got)
			}
			seen := false
			for _, c := range candidates {
				if c.Source == tc.source {
					seen = true
					if c.IsPrimary != AttributionNotPrimary {
						t.Errorf("%s is_primary = %d, want 0", tc.source, c.IsPrimary)
					}
				}
				if c.Source == "unassigned" && c.IsPrimary != AttributionPrimary {
					t.Errorf("unassigned is_primary = %d, want 1", c.IsPrimary)
				}
			}
			if !seen {
				t.Errorf("no %s provenance row in %+v", tc.source, candidates)
			}
		})
	}
	t.Run("ownership-wins-membership-stays-provenance", func(t *testing.T) {
		derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
			Members: member,
			Repos: []GithubWorkItemDerivationRepoFact{{
				Provider: "github", TeamID: "team-m", TeamName: "Member Team",
				RepoID: &repoID, RepoFullName: "a/b", IsPrimary: 1, Specificity: 70,
			}},
		})
		team, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{
			WorkItemID: "gh:a#3", Provider: "github", RepoID: &repoID, Assignees: []string{"alice"}, OrgID: "o",
		})
		if got := GithubWorkItemDerivationStringValue(team); got != "team-m" {
			t.Fatalf("primary team = %q, want team-m from repo_ownership", got)
		}
		for _, c := range candidates {
			if c.Source == "assignee_membership" && c.IsPrimary != AttributionNotPrimary {
				t.Errorf("assignee_membership is_primary = %d, want 0", c.IsPrimary)
			}
		}
	})
}

func strPtr(s string) *string { return &s }
