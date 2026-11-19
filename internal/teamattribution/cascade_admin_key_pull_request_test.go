package teamattribution

import (
	"fmt"
	"testing"
)

// CHAOS-14209: on a pull or merge request, an admin team's project key (a team
// with no provider) is a manual mapping: a GitHub pull request's project id is
// its repository's name, so a key that names the repository is a hand-made
// repository mapping. It ranks with the manual mappings, after linked_issue: a
// native owner (project_ownership, repo_ownership) and a linked issue's donor
// team win over it, and it stays an is_primary = 0 row. With none of them it
// is still the primary, above manual_fallback. An issue keeps the key at its
// rank (the admin key is the project owner of an issue).
func TestAnAdminKeyIsAFallbackOnAPullRequest(t *testing.T) {
	for _, provider := range []string{"github", "gitlab"} {
		key := map[string]string{"github": "acme/payments-api", "gitlab": "acme/payments"}[provider]
		repoID := "repo-payments"
		admin := GithubWorkItemDerivationTeamFact{Provider: "", TeamID: "custom:triage", TeamName: "Triage", ProjectKeys: []string{key}}
		pr := GithubWorkItemDerivationSubject{
			WorkItemID: provider + ":pr-1", Provider: provider, Type: pullRequestType(provider),
			RepoID: &repoID, ProjectID: &key, OrgID: "org",
		}
		cases := []struct {
			name       string
			facts      GithubWorkItemDerivationFacts
			donor      string
			wantTeam   string
			wantSource string
		}{
			{
				name: "repo owner by id",
				facts: GithubWorkItemDerivationFacts{
					Teams: catalogOrder([]GithubWorkItemDerivationTeamFact{admin, {Provider: provider, TeamID: "owner-team"}}),
					Repos: []GithubWorkItemDerivationRepoFact{{Provider: provider, TeamID: "owner-team", RepoID: &repoID, IsPrimary: 1, Specificity: 70}},
				},
				wantTeam: "owner-team", wantSource: "repo_ownership",
			},
			{
				name: "repo owner by name",
				facts: GithubWorkItemDerivationFacts{
					Teams: catalogOrder([]GithubWorkItemDerivationTeamFact{admin, {Provider: provider, TeamID: "owner-team"}}),
					Repos: []GithubWorkItemDerivationRepoFact{{Provider: provider, TeamID: "owner-team", RepoFullName: key, IsPrimary: 1, Specificity: 70}},
				},
				wantTeam: "owner-team", wantSource: "repo_ownership",
			},
			{
				name: "project owner",
				facts: GithubWorkItemDerivationFacts{
					Teams:    catalogOrder([]GithubWorkItemDerivationTeamFact{admin, {Provider: provider, TeamID: "project-team"}}),
					Projects: []GithubWorkItemDerivationProjectFact{{Provider: provider, TeamID: "project-team", ProjectID: key, IsPrimary: 1, Specificity: 110}},
				},
				wantTeam: "project-team", wantSource: "project_ownership",
			},
			{
				name: "no repo owner, a linked issue of another team",
				facts: GithubWorkItemDerivationFacts{
					Teams: catalogOrder([]GithubWorkItemDerivationTeamFact{admin, {Provider: "jira", TeamID: "jira:eng"}}),
				},
				donor:    "jira:eng",
				wantTeam: "jira:eng", wantSource: "linked_issue",
			},
			{
				name: "no native owner and no linked issue: the key is the fallback",
				facts: GithubWorkItemDerivationFacts{
					Teams:           catalogOrder([]GithubWorkItemDerivationTeamFact{admin, {Provider: provider, TeamID: "rule-team"}}),
					ManualFallbacks: []GithubWorkItemDerivationManualFallback{{Provider: provider, ScopeType: "repo", ScopeID: key, TeamID: "rule-team", Priority: 1}},
				},
				wantTeam: "custom:triage", wantSource: "issue_project",
			},
		}
		for _, tc := range cases {
			t.Run(provider+"/"+tc.name, func(t *testing.T) {
				derived := NewGitHubWorkItemDerivationContext(tc.facts)
				if tc.donor != "" {
					subjects := map[string]GithubWorkItemDerivationSubject{
						pr.WorkItemID: pr,
						"jira:ENG-1":  {WorkItemID: "jira:ENG-1", Provider: "jira", Type: "story", OrgID: "org"},
					}
					rows := map[string]GithubWorkItemDerivationDonorRow{"jira:ENG-1": {TeamID: tc.donor, Source: "project_ownership"}}
					edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: pr.WorkItemID, TargetWorkItemID: "jira:ENG-1", RelationshipType: "relates_to"}}
					derived.LinkedIssue, derived.LinkedIssueEvidence, _, _ = derived.BuildLinkedIssueIndexFromRows(provider, subjects, rows, edges, nil)
				}
				teamID, _, candidates := derived.Resolve(pr)
				team, source, count := primaryRow(candidates)
				if GithubWorkItemDerivationStringValue(teamID) != tc.wantTeam || team != tc.wantTeam || source != tc.wantSource || count != 1 {
					t.Fatalf("primary = %q/%s (%d rows), want %s/%s", team, source, count, tc.wantTeam, tc.wantSource)
				}
				adminRows := rowsByTeam(candidates)["custom:triage"]
				if len(adminRows) != 1 || adminRows[0].source != "issue_project" || adminRows[0].evidence != "issue_project_key="+key {
					t.Fatalf("admin team rows = %+v, want one issue_project row (issue_project_key=%s)", adminRows, key)
				}
				wantMark := AttributionNotPrimary
				if tc.wantTeam == "custom:triage" {
					wantMark = AttributionPrimary
				}
				if adminRows[0].isPrimary != wantMark {
					t.Errorf("admin team is_primary = %d, want %d", adminRows[0].isPrimary, wantMark)
				}
				assertEveryRowHasProvenance(t, candidates)
			})
		}
		t.Run(provider+"/the admin key ranks above author_membership", func(t *testing.T) {
			// The key is placed before author_membership, so a reporter's other team adds no second row once the key
			// is the primary (CHAOS-5649 rule 1), as when the key ranked first.
			author := pr
			author.Reporter = strPtr("alice")
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams:   []GithubWorkItemDerivationTeamFact{admin},
				Members: []GithubWorkItemDerivationMemberFact{{Provider: provider, TeamID: "author-team", MemberID: "alice", IsPrimary: 1, Specificity: 60}},
			})
			_, _, candidates := derived.Resolve(author)
			if team, source, _ := primaryRow(candidates); team != "custom:triage" || source != "issue_project" {
				t.Fatalf("primary = %q/%s, want custom:triage/issue_project", team, source)
			}
			if rows := rowsByTeam(candidates)["author-team"]; len(rows) != 0 {
				t.Errorf("author-team rows = %+v, want none: author_membership adds no second team", rows)
			}
		})
		t.Run(provider+"/an issue keeps the admin key above its linked pull requests' repository owner", func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: catalogOrder([]GithubWorkItemDerivationTeamFact{admin, {Provider: provider, TeamID: "owner-team"}}),
				Repos: []GithubWorkItemDerivationRepoFact{{Provider: provider, TeamID: "owner-team", RepoID: &repoID, IsPrimary: 1, Specificity: 70}},
			})
			issue := GithubWorkItemDerivationSubject{WorkItemID: provider + ":issue-1", Provider: provider, Type: "issue", ProjectID: &key, OrgID: "org"}
			derived.LinkedPullRequests = map[string][]GithubWorkItemDerivationLinkedPullRequest{
				issue.WorkItemID: {{RepoID: repoID, PRNumber: 1, Tier: "native"}},
			}
			_, _, candidates := derived.Resolve(issue)
			if team, source, _ := primaryRow(candidates); team != "custom:triage" || source != "issue_project" {
				t.Fatalf("primary = %q/%s, want custom:triage/issue_project", team, source)
			}
			if got := fmt.Sprint(marks(rowsByTeam(candidates)["owner-team"])); got != "map[]" {
				t.Errorf("owner-team marks = %s, want a provenance row only", got)
			}
		})
	}
}
