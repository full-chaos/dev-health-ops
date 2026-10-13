package teamattribution

import (
	"fmt"
	"testing"
)

// A linked item copies its donor's own row, never a resolution of the donor
// in the linked item's context (CHAOS-9460): the donor row's team wins over
// what this context would give the donor, a donor with no row gives nothing,
// and only a row the donor holds on its own (native_team, issue_project,
// project_ownership, repo_ownership) gives its team. Every provider.
func TestALinkedItemCopiesItsDonorsRowOnly(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			repoD := "repo-d"
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Repos: []GithubWorkItemDerivationRepoFact{{Provider: provider, TeamID: "team-context", RepoID: &repoD, IsPrimary: 1, Specificity: 100}},
			})
			subjects := map[string]GithubWorkItemDerivationSubject{
				"pr":    {WorkItemID: "pr", Provider: provider, OrgID: "org"},
				"donor": {WorkItemID: "donor", Provider: provider, RepoID: &repoD, OrgID: "org"},
			}
			edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "pr", TargetWorkItemID: "donor", RelationshipType: "relates_to"}}
			for _, tc := range []struct {
				name string
				rows map[string]GithubWorkItemDerivationDonorRow
				want string
			}{
				{"the donor's row wins over its resolution here", map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "team-row", Source: "repo_ownership"}}, "team-row"},
				{"no donor row gives nothing", map[string]GithubWorkItemDerivationDonorRow{}, ""},
				{"an inherited row gives nothing", map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "team-row", Source: "linked_issue"}}, ""},
				{"a membership row gives nothing", map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "team-row", Source: "assignee_membership"}}, ""},
				{"a fallback row gives nothing", map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "team-row", Source: "manual_fallback"}}, ""},
				{"a native team row gives its team", map[string]GithubWorkItemDerivationDonorRow{"donor": {TeamID: "team-row", Source: "native_team"}}, "team-row"},
				{"a row with no team gives nothing", map[string]GithubWorkItemDerivationDonorRow{"donor": {Source: "repo_ownership"}}, ""},
			} {
				index, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows(provider, subjects, tc.rows, edges, nil)
				inherited, ok := index["pr"]
				if ok != (tc.want != "") || inherited[0] != tc.want {
					t.Errorf("%s: the pull request inherits %q (present %v), want %q (index %v)", tc.name, inherited[0], ok, tc.want, index)
				}
				if ok {
					want := "linked_issue=donor;dependency_type=relates_to;donor_provider=" + provider + ";donor_source=" + tc.rows["donor"].Source
					if evidence["pr"] != want {
						t.Errorf("%s: evidence %q, want %q", tc.name, evidence["pr"], want)
					}
					derived.LinkedIssue, derived.LinkedIssueEvidence = index, evidence
					for _, candidate := range func() []GithubWorkItemDerivationCandidate { _, _, c := derived.Resolve(subjects["pr"]); return c }() {
						if candidate.Source == "linked_issue" && candidate.Evidence != want {
							t.Errorf("%s: the linked_issue row's evidence %q, want %q", tc.name, candidate.Evidence, want)
						}
					}
					derived.LinkedIssue, derived.LinkedIssueEvidence = nil, nil
				}
			}
			index, _, _ := derived.BuildLinkedIssueIndex(provider, subjects, edges, nil)
			if got := fmt.Sprint(index["pr"][0]); got != "team-context" {
				t.Errorf("the reference shape resolves the donor in this context: %q, want team-context", got)
			}
		})
	}
}

// Two edges from one item that reach the same donor (an issue-key edge and a
// direct edge to the same Jira issue) give one linked_issue row; its evidence
// names the relationship that sorts first, never the one the edges happen to
// be read in.
func TestTwoEdgesToOneDonorNameOneRelationship(t *testing.T) {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
	subjects := map[string]GithubWorkItemDerivationSubject{
		"pr":         {WorkItemID: "pr", Provider: "github", OrgID: "org"},
		"jira:ENG-1": {WorkItemID: "jira:ENG-1", Provider: "jira", OrgID: "org"},
	}
	rows := map[string]GithubWorkItemDerivationDonorRow{"jira:ENG-1": {TeamID: "team-row", Source: "project_ownership"}}
	edges := []GithubWorkItemDerivationDependencyEdge{
		{SourceWorkItemID: "pr", TargetWorkItemID: "jira:ENG-1", RelationshipType: "relates_to"},
		{SourceWorkItemID: "pr", TargetWorkItemID: "extkey:ENG-1", RelationshipType: "external_issue_key"},
	}
	for run := 0; run < 20; run++ {
		_, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows("github", subjects, rows, edges, nil)
		if want := "linked_issue=jira:ENG-1;dependency_type=external_issue_key;donor_provider=jira;donor_source=project_ownership"; evidence["pr"] != want {
			t.Fatalf("run %d: evidence %q, want %q", run, evidence["pr"], want)
		}
	}
}

// A stored donor row does not record its team's provider. The team is the one
// the id names for an item of the donor's provider, so an inactive team of
// another provider with the same id never drops it, and the team of the
// donor's provider, when inactive, does.
func TestAStoredDonorRowBindsTheTeamOfTheDonorsProvider(t *testing.T) {
	for _, tc := range []struct {
		name         string
		jiraInactive bool
		want         string
	}{{"another provider's inactive team with the id", false, "shared-id"}, {"the donor provider's team is inactive", true, ""}} {
		t.Run(tc.name, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: []GithubWorkItemDerivationTeamFact{
					{Provider: "github", TeamID: "shared-id", TeamName: "GitHub Shared", Inactive: true},
					{Provider: "jira", TeamID: "shared-id", TeamName: "Jira Shared", Inactive: tc.jiraInactive},
				},
			})
			subjects := map[string]GithubWorkItemDerivationSubject{
				"ghpr:acme/api#7": {WorkItemID: "ghpr:acme/api#7", Provider: "github", Type: "pr", OrgID: "org"},
				"jira:ENG-1":      {WorkItemID: "jira:ENG-1", Provider: "jira", OrgID: "org"},
			}
			edges := []GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "ghpr:acme/api#7", TargetWorkItemID: "jira:ENG-1", RelationshipType: "relates_to"}}
			rows := map[string]GithubWorkItemDerivationDonorRow{"jira:ENG-1": {TeamID: "shared-id", Source: "project_ownership"}}
			derived.LinkedIssue, derived.LinkedIssueEvidence, _, _ = derived.BuildLinkedIssueIndexFromRows("github", subjects, rows, edges, nil)
			teamID, _, candidates := derived.Resolve(subjects["ghpr:acme/api#7"])
			if got := GithubWorkItemDerivationStringValue(teamID); got != tc.want {
				t.Fatalf("team %q, want %q (candidates %+v)", got, tc.want, candidates)
			}
			if tc.want != "" {
				for _, candidate := range candidates {
					if candidate.Source == "linked_issue" && (candidate.TeamProvider != "jira" || GithubWorkItemDerivationStringValue(candidate.TeamName) != "Jira Shared") {
						t.Errorf("linked_issue candidate bound to %q %q, want the jira team", candidate.TeamProvider, GithubWorkItemDerivationStringValue(candidate.TeamName))
					}
				}
			}
		})
	}
}
