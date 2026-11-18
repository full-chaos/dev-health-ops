package teamattribution

import "testing"

// A team is reached through ownership only: a fallback rule on an assignee (a
// person) and an issue's link to another issue never give the primary team.
// Their candidates stay as is_primary = 0 provenance rows.

func ownershipOnlyRow(t *testing.T, candidates []GithubWorkItemDerivationCandidate, source string) GithubWorkItemDerivationCandidate {
	t.Helper()
	for _, candidate := range candidates {
		if candidate.Source == source {
			return candidate
		}
	}
	t.Fatalf("no %s row in %+v", source, candidates)
	return GithubWorkItemDerivationCandidate{}
}

func TestAMemberFallbackRuleIsProvenanceOnly(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			member := GithubWorkItemDerivationManualFallback{Provider: provider, ScopeType: "member", ScopeID: "user-bob", TeamID: "team-rule", Priority: 1}
			subject := GithubWorkItemDerivationSubject{
				WorkItemID: "item-m", Provider: provider, ProjectID: strPtr("project-m"), Assignees: []string{"user-bob"}, OrgID: "org",
			}

			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{ManualFallbacks: []GithubWorkItemDerivationManualFallback{member}})
			team, _, candidates := derived.Resolve(subject)
			if got := GithubWorkItemDerivationStringValue(team); got != "" {
				t.Fatalf("primary team = %q, want none: a rule on an assignee is not ownership", got)
			}
			// The evidence of an item whose assignee has no team is unchanged.
			if row := ownershipOnlyRow(t, candidates, "unassigned"); row.IsPrimary != AttributionPrimary || row.Evidence != "no_candidate:no_membership" {
				t.Errorf("unassigned row = %+v, want is_primary 1, evidence no_candidate:no_membership", row)
			}
			row := ownershipOnlyRow(t, candidates, "manual_fallback")
			if row.IsPrimary != AttributionNotPrimary || GithubWorkItemDerivationStringValue(row.TeamID) != "team-rule" || row.Evidence != "manual_fallback:member=user-bob" {
				t.Errorf("member rule row = %+v, want team-rule, is_primary 0, evidence manual_fallback:member=user-bob", row)
			}

			project := GithubWorkItemDerivationManualFallback{Provider: provider, ScopeType: "project", ScopeID: "project-m", TeamID: "team-proj", Priority: 9}
			derived = NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{ManualFallbacks: []GithubWorkItemDerivationManualFallback{member, project}})
			team, _, candidates = derived.Resolve(subject)
			if got := GithubWorkItemDerivationStringValue(team); got != "team-proj" {
				t.Fatalf("primary team = %q, want team-proj from the project rule", got)
			}
			for _, candidate := range candidates {
				if candidate.Source == "manual_fallback" && GithubWorkItemDerivationStringValue(candidate.TeamID) == "team-rule" && candidate.IsPrimary != AttributionNotPrimary {
					t.Errorf("member rule row is_primary = %d, want 0", candidate.IsPrimary)
				}
			}
		})
	}
}

func TestALinkedIssueOverAnIssueEdgeIsProvenanceOnly(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
			subjects := map[string]GithubWorkItemDerivationSubject{
				"issue-l": {WorkItemID: "issue-l", Provider: provider, Type: "issue", OrgID: "org"},
				"issue-d": {WorkItemID: "issue-d", Provider: provider, Type: "issue", OrgID: "org"},
			}
			rows := map[string]GithubWorkItemDerivationDonorRow{"issue-d": {TeamID: "team-donor", Source: "project_ownership"}}
			edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "issue-l", TargetWorkItemID: "issue-d", RelationshipType: "relates_to"}}
			derived.LinkedIssue, derived.LinkedIssueEvidence, _, _ = derived.BuildLinkedIssueIndexFromRows(provider, subjects, rows, edges, nil)

			team, _, candidates := derived.Resolve(subjects["issue-l"])
			if got := GithubWorkItemDerivationStringValue(team); got != "" {
				t.Fatalf("primary team = %q, want none: another issue's team is not ownership", got)
			}
			if row := ownershipOnlyRow(t, candidates, "unassigned"); row.IsPrimary != AttributionPrimary || row.Evidence != "no_candidate" {
				t.Errorf("unassigned row = %+v, want is_primary 1, evidence no_candidate", row)
			}
			row := ownershipOnlyRow(t, candidates, "linked_issue")
			if row.IsPrimary != AttributionNotPrimary || GithubWorkItemDerivationStringValue(row.TeamID) != "team-donor" {
				t.Errorf("linked_issue row = %+v, want team-donor, is_primary 0", row)
			}
		})
	}
}

// pullRequestType is the item type of a pull or merge request of the provider
// (GithubWorkItemDerivationIsPullOrMergeRequestType); jira and linear have none.
func pullRequestType(provider string) string {
	return map[string]string{"github": "pr", "gitlab": "merge_request"}[provider]
}

func TestAPullRequestKeepsItsLinkedIssueDonor(t *testing.T) {
	for _, pr := range []struct{ provider, kind string }{{"github", pullRequestType("github")}, {"gitlab", pullRequestType("gitlab")}} {
		t.Run(pr.provider, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
			subjects := map[string]GithubWorkItemDerivationSubject{
				"pr-p":       {WorkItemID: "pr-p", Provider: pr.provider, Type: pr.kind, OrgID: "org"},
				"jira:ENG-1": {WorkItemID: "jira:ENG-1", Provider: "jira", Type: "story", OrgID: "org"},
			}
			for _, donorSource := range []string{"native_team", "issue_project", "project_ownership"} {
				rows := map[string]GithubWorkItemDerivationDonorRow{"jira:ENG-1": {TeamID: "team-issue", Source: donorSource}}
				edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "pr-p", TargetWorkItemID: "jira:ENG-1", RelationshipType: "relates_to"}}
				derived.LinkedIssue, derived.LinkedIssueEvidence, _, _ = derived.BuildLinkedIssueIndexFromRows(pr.provider, subjects, rows, edges, nil)
				team, _, candidates := derived.Resolve(subjects["pr-p"])
				if got := GithubWorkItemDerivationStringValue(team); got != "team-issue" {
					t.Fatalf("donor %s: primary team = %q, want team-issue from the linked issue", donorSource, got)
				}
				if row := ownershipOnlyRow(t, candidates, "linked_issue"); row.IsPrimary != AttributionPrimary {
					t.Errorf("donor %s: linked_issue row is_primary = %d, want 1", donorSource, row.IsPrimary)
				}
			}
		})
	}
}
