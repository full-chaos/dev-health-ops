package teamattribution

import "testing"

// An issue's repo_ownership row (the owner of its linked pull request's repository) is not a linked_issue donor
// row: a pull or merge request linked to the issue does not take that team (it would be a second hop, pull request
// -> issue -> another pull request's repository). The issue's project owner stays a donor.
func TestAnIssuesLinkedPullRequestRepoOwnerIsNotADonor(t *testing.T) {
	for _, kind := range []struct{ provider, prType, prID string }{{"github", "pr", "ghpr:a/b#7"}, {"gitlab", "merge_request", "a/b!7"}} {
		for _, issueKind := range []struct{ provider, itemType string }{{"github", "issue"}, {"gitlab", "issue"}, {"jira", "story"}, {"linear", "issue"}} {
			name := kind.provider + "/" + issueKind.provider
			issueID := issueKind.provider + ":ISS-9"
			prRepo := linkedRepoN
			resolvePR := func(t *testing.T, facts GithubWorkItemDerivationFacts, issue GithubWorkItemDerivationSubject) []string {
				t.Helper()
				derived := NewGitHubWorkItemDerivationContext(facts)
				derived.LinkedPullRequests = map[string][]GithubWorkItemDerivationLinkedPullRequest{issueID: {linkedPR(linkedRepoA, 1, "native")}}
				subjects := map[string]GithubWorkItemDerivationSubject{
					kind.prID: {WorkItemID: kind.prID, Provider: kind.provider, Type: kind.prType, RepoID: &prRepo, OrgID: "org"},
					issueID:   issue,
				}
				all := map[string]struct{}{kind.prID: {}, issueID: {}}
				edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: kind.prID, TargetWorkItemID: issueID, RelationshipType: "relates_to"}}
				teams, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows(kind.provider, subjects, derived.OwnDonorRows(subjects, all), edges, nil)
				derived.LinkedIssue, derived.LinkedIssueEvidence = teams, evidence
				rows, _ := linkedRows(t, derived, subjects[kind.prID])
				return rows
			}
			issue := GithubWorkItemDerivationSubject{WorkItemID: issueID, Provider: issueKind.provider, Type: issueKind.itemType, OrgID: "org"}
			repoOwner := linkedRepoFact("github", linkedRepoA, "team-a", 1, 100)
			t.Run(name+"/the issue's linked repository owner is not a donor", func(t *testing.T) {
				rows := resolvePR(t, GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{repoOwner}}, issue)
				wantLinkedRows(t, rows, "unassigned||1")
			})
			t.Run(name+"/the issue's project owner stays a donor", func(t *testing.T) {
				projectID := "PRJ"
				owned := issue
				owned.ProjectID = &projectID
				rows := resolvePR(t, GithubWorkItemDerivationFacts{
					Repos:    []GithubWorkItemDerivationRepoFact{repoOwner},
					Projects: []GithubWorkItemDerivationProjectFact{{Provider: issueKind.provider, TeamID: "team-p", TeamName: "P", ProjectID: projectID, IsPrimary: 1, Specificity: 100}},
				}, owned)
				wantLinkedRows(t, rows, "linked_issue|team-p|1")
			})
		}
	}
}
