package teamattribution

import "testing"

func gitlabOwnershipContext(ownerProjectID, ownerPath string) GithubWorkItemDerivationContext {
	return NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Teams: []GithubWorkItemDerivationTeamFact{{Provider: "gitlab", TeamID: "owner-team"}},
		Projects: []GithubWorkItemDerivationProjectFact{{
			Provider: "gitlab", TeamID: "owner-team", TeamName: "Owner", ProjectID: ownerProjectID, ProjectKey: &ownerPath,
			IsPrimary: 1, Specificity: 110, Priority: 10,
		}},
	})
}

func gitlabSubject(projectID, path string) GithubWorkItemDerivationSubject {
	return GithubWorkItemDerivationSubject{
		WorkItemID: "gitlab:" + path + "#42", Provider: "gitlab", ProjectID: &projectID, ProjectKey: &path, OrgID: "org",
	}
}

func primaryTeam(derived GithubWorkItemDerivationContext, subject GithubWorkItemDerivationSubject) string {
	teamID, _, _ := derived.Resolve(subject)
	return GithubWorkItemDerivationStringValue(teamID)
}

func TestAGitLabItemJoinsItsProjectOwnershipByTheCatalogId(t *testing.T) {
	derived := gitlabOwnershipContext("org:gitlab:123", "platform/api-v2")
	if got := primaryTeam(derived, gitlabSubject("org:gitlab:123", "platform/api-v2")); got != "owner-team" {
		t.Fatalf("an item of project 123 joined %q, want owner-team", got)
	}
	// After a rename the item's key is the old path; the grant still follows the project.
	if got := primaryTeam(derived, gitlabSubject("org:gitlab:123", "acme/api")); got != "owner-team" {
		t.Fatalf("an item of project 123 under its old path joined %q, want owner-team", got)
	}
}

// LEAK: project 123 gave up acme/api and project 456 took it. A stale item of
// 123 at acme/api carries 123's catalog id and the path; the ownership of 456
// names the same path and must not claim it.
func TestAGitLabItemAtAReusedPathJoinsNothingOfTheNewOwner(t *testing.T) {
	derived := gitlabOwnershipContext("org:gitlab:456", "acme/api")
	if got := primaryTeam(derived, gitlabSubject("org:gitlab:123", "acme/api")); got != "" {
		t.Fatalf("a stale item of project 123 joined %q through the path of project 456", got)
	}
	if got := primaryTeam(derived, gitlabSubject("org:gitlab:456", "acme/api")); got != "owner-team" {
		t.Fatalf("an item of project 456 joined %q, want owner-team", got)
	}
}

func TestAGitLabItemAnswersNameJoinsByEveryPathOfItsRepo(t *testing.T) {
	subject := gitlabSubject("org:gitlab:123", "acme/api")
	subject.GitLabPaths = []string{"platform/api-v2", "acme/api"}
	got := GitLabProjectNames(subject)
	if len(got) != 2 || got[0] != "platform/api-v2" || got[1] != "acme/api" {
		t.Fatalf("names = %v, want the current and the minted path once each", got)
	}
	legacy := gitlabSubject("acme/api", "")
	legacy.ProjectKey = nil
	if got := GitLabProjectNames(legacy); len(got) != 1 || got[0] != "acme/api" {
		t.Fatalf("names of an item that holds the path in project_id = %v, want the path", got)
	}
	other := subject
	other.Provider = "github"
	if got := GitLabProjectNames(other); len(got) != 1 || got[0] != "org:gitlab:123" {
		t.Fatalf("names of a non-GitLab item = %v, want its project_id only", got)
	}
}

func manualRepoRuleTeam(provider, scope string, subject GithubWorkItemDerivationSubject) string {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Teams:           []GithubWorkItemDerivationTeamFact{{Provider: provider, TeamID: "rule-team"}},
		ManualFallbacks: []GithubWorkItemDerivationManualFallback{{Provider: provider, ScopeType: "repo", ScopeID: scope, TeamID: "rule-team", Priority: 1}},
	})
	for _, candidate := range derived.ManualCandidates(subject) {
		return GithubWorkItemDerivationStringValue(candidate.TeamID)
	}
	return ""
}

// A manual repo rule written with the new path and one written with the old
// path both match a renamed project; a GitHub repo of the same slug matches
// neither.
func TestAManualRepoRuleMatchesARenamedGitLabProjectByEitherPath(t *testing.T) {
	renamed := gitlabSubject("org:gitlab:123", "acme/api")
	renamed.GitLabPaths = []string{"platform/api-v2", "acme/api"}
	for _, scope := range []string{"platform/api-v2", "acme/api"} {
		if got := manualRepoRuleTeam("gitlab", scope, renamed); got != "rule-team" {
			t.Errorf("rule on %q: team = %q, want rule-team", scope, got)
		}
	}
	if got := manualRepoRuleTeam("gitlab", "acme/other", renamed); got != "" {
		t.Errorf("rule on another path matched %q", got)
	}
	github := GithubWorkItemDerivationSubject{WorkItemID: "github:acme/api#1", Provider: "github", ProjectID: strPtrForTest("acme/api"), OrgID: "org"}
	if got := manualRepoRuleTeam("gitlab", "acme/api", github); got != "" {
		t.Errorf("a gitlab rule matched a GitHub repo of the same slug: %q", got)
	}
}

func strPtrForTest(value string) *string { return &value }

func TestAGitLabItemScopeIsItsPathNotItsCatalogId(t *testing.T) {
	if got := WorkItemDerivationScope(gitlabSubject("org:gitlab:123", "acme/api")); got != "acme/api" {
		t.Fatalf("scope = %q, want the path", got)
	}
}
